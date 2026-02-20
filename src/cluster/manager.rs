use crate::collector::lag_calculator::{LagCalculator, PartitionOffsetMetric, TimestampData};
use crate::collector::offset_collector::{GroupSnapshot, MemberSnapshot};
use crate::collector::timestamp_sampler::TimestampSampler;
use crate::config::{ClusterConfig, CompiledFilters, ExporterConfig, Granularity};
use crate::error::Result;
use crate::kafka::TimestampConsumer;
use crate::kafka::client::{GroupDescription, KafkaClient, TopicPartition};
use crate::leadership::LeadershipStatus;
use crate::metrics::registry::{
    MetricsRegistry, build_cluster_summary_points, build_group_metric_points,
    build_partition_offset_points,
};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, error, info, instrument, trace, warn};

/// Default timeout for a single collection cycle (should be less than `poll_interval`)
const DEFAULT_COLLECTION_TIMEOUT: Duration = Duration::from_secs(60);

pub struct ClusterManager {
    cluster_name: String,
    cluster_labels: HashMap<String, String>,
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
    timestamp_sampler: Option<TimestampSampler>,
    registry: Arc<MetricsRegistry>,
    poll_interval: Duration,
    max_backoff: Duration,
    granularity: Granularity,
    max_concurrent_fetches: usize,
    max_concurrent_groups: usize,
    cache_cleanup_interval: Duration,
    collection_timeout: Duration,
    compacted_topics_cache: Mutex<Option<(HashSet<String>, Instant)>>,
    compacted_topics_cache_ttl: Duration,
}

impl ClusterManager {
    pub fn new(
        config: &ClusterConfig,
        registry: Arc<MetricsRegistry>,
        exporter_config: &ExporterConfig,
    ) -> Result<Self> {
        let cluster_name = config.name.clone();
        let cluster_labels = config.labels.clone();
        let filters = config.compile_filters()?;
        let performance = exporter_config.performance.clone();

        let client = Arc::new(KafkaClient::with_performance(config, performance.clone())?);

        let timestamp_sampler = if exporter_config.timestamp_sampling.enabled {
            let ts_consumer = TimestampConsumer::with_pool_size(
                config,
                exporter_config.timestamp_sampling.max_concurrent_fetches,
            );
            let effective_cache_ttl = std::cmp::max(
                exporter_config.timestamp_sampling.cache_ttl,
                exporter_config.poll_interval * 2,
            );
            Some(TimestampSampler::new(ts_consumer?, effective_cache_ttl))
        } else {
            None
        };

        info!(
            cluster = cluster_name,
            timestamp_sampling = exporter_config.timestamp_sampling.enabled,
            poll_interval = ?exporter_config.poll_interval,
            granularity = ?exporter_config.granularity,
            max_concurrent_fetches = exporter_config.timestamp_sampling.max_concurrent_fetches,
            custom_labels = ?cluster_labels,
            "Created cluster manager"
        );

        // Collection timeout should be less than poll_interval to avoid overlap,
        // but must exceed kafka_timeout so individual API calls can finish.
        let kafka_timeout = performance.kafka_timeout;
        let collection_timeout = if exporter_config.poll_interval > Duration::from_secs(10) {
            let poll_based = exporter_config
                .poll_interval
                .checked_sub(Duration::from_secs(5))
                .expect("should not be negative");
            let min_viable = kafka_timeout + Duration::from_secs(5);
            poll_based.max(min_viable)
        } else {
            DEFAULT_COLLECTION_TIMEOUT
                .min(exporter_config.poll_interval)
                .max(kafka_timeout + Duration::from_secs(5))
        };

        if collection_timeout > exporter_config.poll_interval {
            warn!(
                cluster = cluster_name,
                poll_interval = ?exporter_config.poll_interval,
                kafka_timeout = ?kafka_timeout,
                collection_timeout = ?collection_timeout,
                "collection_timeout exceeds poll_interval; consider increasing poll_interval"
            );
        }

        Ok(Self {
            cluster_name,
            cluster_labels,
            client,
            filters,
            timestamp_sampler,
            registry,
            poll_interval: exporter_config.poll_interval,
            max_backoff: Duration::from_secs(300),
            granularity: exporter_config.granularity,
            max_concurrent_fetches: exporter_config.timestamp_sampling.max_concurrent_fetches,
            max_concurrent_groups: performance.max_concurrent_groups,
            cache_cleanup_interval: exporter_config.timestamp_sampling.cache_ttl * 2,
            collection_timeout,
            compacted_topics_cache: Mutex::new(None),
            compacted_topics_cache_ttl: performance.compacted_topics_cache_ttl,
        })
    }

    #[instrument(skip(self, shutdown, leadership), fields(cluster = %self.cluster_name))]
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>, leadership: LeadershipStatus) {
        info!("Starting collection loop");

        let mut interval = tokio::time::interval(self.poll_interval);
        let mut cache_cleanup_interval = tokio::time::interval(self.cache_cleanup_interval);
        let mut consecutive_errors = 0u32;
        let mut current_backoff = Duration::from_secs(1);
        let mut was_leader = leadership.is_leader();

        if !was_leader {
            info!("Starting in standby mode - waiting for leadership");
        }

        self.registry.set_healthy(true);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Check leadership status before collecting
                    let is_leader = leadership.is_leader();

                    // Log leadership transitions
                    if is_leader != was_leader {
                        if is_leader {
                            info!("Acquired leadership - starting collection");
                        } else {
                            info!("Lost leadership - pausing collection");
                            // Clear metrics when losing leadership to avoid stale data
                            self.registry.remove_cluster(&self.cluster_name);
                        }
                        was_leader = is_leader;
                    }

                    // Skip collection if not leader
                    if !is_leader {
                        debug!("Standby mode - skipping collection");
                        continue;
                    }

                    // Wrap collect_once with a timeout to prevent hangs
                    let collection_result = tokio::time::timeout(
                        self.collection_timeout,
                        self.collect_once()
                    ).await;

                    match collection_result {
                        Ok(Ok(())) => {
                            consecutive_errors = 0;
                            current_backoff = Duration::from_secs(1);
                            self.registry.set_healthy(true);
                        }
                        Ok(Err(e)) => {
                            consecutive_errors += 1;
                            error!(
                                error = %e,
                                consecutive_errors = consecutive_errors,
                                "Collection failed"
                            );

                            if consecutive_errors >= 3 {
                                self.registry.set_healthy(false);

                                let backoff = current_backoff.min(self.max_backoff);
                                warn!(
                                    backoff_secs = backoff.as_secs(),
                                    "Applying backoff due to consecutive errors"
                                );

                                tokio::time::sleep(backoff).await;
                                current_backoff = (current_backoff * 2).min(self.max_backoff);
                            }
                        }
                        Err(_timeout) => {
                            consecutive_errors += 1;
                            error!(
                                timeout_secs = self.collection_timeout.as_secs(),
                                consecutive_errors = consecutive_errors,
                                "Collection timed out"
                            );

                            if consecutive_errors >= 3 {
                                self.registry.set_healthy(false);
                            }
                        }
                    }
                }
                _ = cache_cleanup_interval.tick() => {
                    // Periodic cache cleanup (only if leader, to save resources)
                    if leadership.is_leader()
                        && let Some(ref sampler) = self.timestamp_sampler
                    {
                        let before = sampler.cache_size();
                        sampler.clear_stale_entries();
                        let after = sampler.cache_size();
                        if before != after {
                            debug!(
                                before = before,
                                after = after,
                                "Cleaned up stale cache entries"
                            );
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Received shutdown signal");
                    break;
                }
            }
        }

        // Cleanup
        self.registry.remove_cluster(&self.cluster_name);
        info!("Collection loop stopped");
    }

    #[instrument(skip(self))]
    async fn collect_once(&self) -> Result<()> {
        let start = Instant::now();

        // 1. List and filter consumer groups
        let step = Instant::now();
        let all_groups = self.client.list_consumer_groups()?;
        let list_ms = step.elapsed().as_millis();
        debug!(total_groups = all_groups.len(), "Listed consumer groups");

        let group_ids: Vec<&str> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .map(|g| g.group_id.as_str())
            .collect();
        debug!(
            filtered_groups = group_ids.len(),
            "Filtered consumer groups"
        );

        // 2. Describe consumer groups
        let step = Instant::now();
        let descriptions = self.client.describe_consumer_groups(&group_ids).await?;
        let describe_ms = step.elapsed().as_millis();
        debug!(
            descriptions = descriptions.len(),
            "Described consumer groups"
        );

        // 3. Pre-fetch offsets for all groups to discover ALL partitions with committed offsets
        let step = Instant::now();
        let (all_group_offsets, all_committed_partitions) =
            self.pre_fetch_group_offsets(&descriptions).await;
        let offsets_ms = step.elapsed().as_millis();

        // 4. Fetch watermarks for ALL partitions with committed offsets (targeted, no metadata call)
        let step = Instant::now();
        let watermarks = self
            .client
            .fetch_watermarks_for_partitions(&all_committed_partitions)
            .await?;
        let watermarks_ms = step.elapsed().as_millis();
        debug!(partitions = watermarks.len(), "Fetched targeted watermarks");

        // 5. Fetch compacted topics (cached with TTL)
        let step = Instant::now();
        let compacted_topics = self.fetch_compacted_topics_cached().await;
        let compacted_ms = step.elapsed().as_millis();

        // 6. Begin streaming cycle
        self.registry.begin_cycle(&self.cluster_name);

        // 7. Emit partition offset points from watermarks
        self.push_partition_offset_points(&watermarks);

        // 8. Stream groups and push per-group metrics
        let step = Instant::now();
        let (total_compaction_detected, total_data_loss_partitions, total_skipped_partitions) =
            self.stream_group_metrics(
                descriptions,
                all_group_offsets,
                watermarks,
                compacted_topics,
            )
            .await;
        let stream_ms = step.elapsed().as_millis();

        info!(
            list_groups_ms = %list_ms,
            describe_groups_ms = %describe_ms,
            prefetch_offsets_ms = %offsets_ms,
            fetch_watermarks_ms = %watermarks_ms,
            compacted_topics_ms = %compacted_ms,
            stream_metrics_ms = %stream_ms,
            groups = group_ids.len(),
            partitions = all_committed_partitions.len(),
            "Collection cycle step timings"
        );

        // 9. Emit cluster summary points
        #[allow(clippy::cast_possible_truncation)]
        let poll_time_ms = start.elapsed().as_millis() as u64;
        self.registry.push_points(
            &self.cluster_name,
            build_cluster_summary_points(
                &self.cluster_name,
                poll_time_ms,
                total_compaction_detected,
                total_data_loss_partitions,
                total_skipped_partitions,
                &self.cluster_labels,
            ),
        );

        // 10. Finish cycle
        self.registry.finish_cycle(&self.cluster_name);

        #[allow(clippy::cast_possible_truncation)]
        let scrape_duration_ms = start.elapsed().as_millis() as u64;
        self.registry.set_scrape_duration_ms(scrape_duration_ms);

        debug!(
            elapsed_ms = scrape_duration_ms,
            timestamp_cache_size = self
                .timestamp_sampler
                .as_ref()
                .map_or(0, TimestampSampler::cache_size),
            "Collection cycle completed (streaming), took {} seconds",
            start.elapsed().as_secs_f32()
        );

        Ok(())
    }

    fn push_partition_offset_points(&self, watermarks: &HashMap<TopicPartition, (i64, i64)>) {
        let partition_offsets: Vec<PartitionOffsetMetric> = watermarks
            .iter()
            .map(|(tp, (low, high))| PartitionOffsetMetric {
                cluster_name: self.cluster_name.clone(),
                topic: tp.topic.clone(),
                partition: tp.partition,
                earliest_offset: *low,
                latest_offset: *high,
            })
            .collect();

        self.registry.push_points(
            &self.cluster_name,
            build_partition_offset_points(partition_offsets.iter(), &self.cluster_labels),
        );
    }

    /// Stream through groups concurrently, calculating lag and pushing metrics.
    /// Returns (`total_compaction_detected`, `total_data_loss_partitions`, `total_skipped_partitions`).
    async fn stream_group_metrics(
        &self,
        descriptions: Vec<GroupDescription>,
        all_group_offsets: HashMap<String, HashMap<TopicPartition, i64>>,
        watermarks: HashMap<TopicPartition, (i64, i64)>,
        compacted_topics: HashSet<String>,
    ) -> (u64, u64, u64) {
        #[allow(clippy::cast_possible_truncation)]
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as i64;

        let cluster_name = self.cluster_name.clone();
        let cluster_labels = self.cluster_labels.clone();
        let granularity = self.granularity;
        let compacted_topics = Arc::new(compacted_topics);
        let watermarks = Arc::new(watermarks);
        let timestamp_sampler = self.timestamp_sampler.clone();
        let max_concurrent_fetches = self.max_concurrent_fetches;
        let all_group_offsets = Arc::new(all_group_offsets);

        let mut total_compaction_detected: u64 = 0;
        let mut total_data_loss_partitions: u64 = 0;
        let mut total_skipped_partitions: u64 = 0;

        let mut stream = futures::stream::iter(descriptions)
            .map(|desc| {
                let cluster_name = cluster_name.clone();
                let cluster_labels = cluster_labels.clone();
                let compacted_topics = Arc::clone(&compacted_topics);
                let watermarks = Arc::clone(&watermarks);
                let timestamp_sampler = timestamp_sampler.clone();
                let all_group_offsets = Arc::clone(&all_group_offsets);

                async move {
                    let Some(offsets) = all_group_offsets.get(&desc.group_id).cloned() else {
                        warn!(
                            group = desc.group_id,
                            "No pre-fetched offsets available, skipping group"
                        );
                        return (Vec::new(), 0, 0, 0);
                    };

                    let members: Vec<MemberSnapshot> = desc
                        .members
                        .into_iter()
                        .map(|m| MemberSnapshot {
                            member_id: m.member_id,
                            client_id: m.client_id,
                            client_host: m.client_host,
                            assignments: m.assignments,
                        })
                        .collect();

                    let group = GroupSnapshot {
                        group_id: desc.group_id,
                        members,
                        offsets,
                    };

                    let timestamps = if let Some(ref sampler) = timestamp_sampler {
                        fetch_group_timestamps(sampler, &group, &watermarks, max_concurrent_fetches)
                            .await
                    } else {
                        HashMap::new()
                    };

                    let (partition_metrics, skipped) = LagCalculator::calculate_group(
                        &cluster_name,
                        &group,
                        &watermarks,
                        &timestamps,
                        now_ms,
                        &compacted_topics,
                    );

                    #[allow(clippy::cast_possible_truncation)]
                    let compaction_count = partition_metrics
                        .iter()
                        .filter(|m| m.compaction_detected)
                        .count() as u64;
                    #[allow(clippy::cast_possible_truncation)]
                    let data_loss_count = partition_metrics
                        .iter()
                        .filter(|m| m.data_loss_detected)
                        .count() as u64;

                    let points = build_group_metric_points(
                        partition_metrics.iter(),
                        granularity,
                        skipped,
                        &cluster_labels,
                    );

                    (points, compaction_count, data_loss_count, skipped)
                }
            })
            .buffer_unordered(self.max_concurrent_groups);

        while let Some((points, compaction_count, data_loss_count, skipped)) = stream.next().await {
            self.registry.push_points(&self.cluster_name, points);
            total_compaction_detected += compaction_count;
            total_data_loss_partitions += data_loss_count;
            total_skipped_partitions += skipped;
        }

        (
            total_compaction_detected,
            total_data_loss_partitions,
            total_skipped_partitions,
        )
    }

    /// Pre-fetch committed offsets for all groups, returning the offsets per group
    /// and the union of all committed partitions (for watermark fetching).
    async fn pre_fetch_group_offsets(
        &self,
        descriptions: &[GroupDescription],
    ) -> (
        HashMap<String, HashMap<TopicPartition, i64>>,
        HashSet<TopicPartition>,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_groups));

        let handles: Vec<_> = descriptions
            .iter()
            .map(|desc| {
                let client = Arc::clone(&self.client);
                let sem = Arc::clone(&semaphore);
                let group_id = desc.group_id.clone();
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let result = client.list_consumer_group_offsets(&group_id).await;
                    (group_id, result)
                })
            })
            .collect();

        let mut all_group_offsets: HashMap<String, HashMap<TopicPartition, i64>> = HashMap::new();
        let mut all_committed_partitions: HashSet<TopicPartition> = HashSet::new();

        for handle in handles {
            let Ok((group_id, result)) = handle.await else {
                warn!("Offset fetch task was cancelled");
                continue;
            };
            match result {
                Ok(offsets) => {
                    let filtered: HashMap<TopicPartition, i64> = offsets
                        .into_iter()
                        .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
                        .collect();
                    for tp in filtered.keys() {
                        all_committed_partitions.insert(tp.clone());
                    }
                    all_group_offsets.insert(group_id, filtered);
                }
                Err(e) => {
                    warn!(group = group_id, error = %e, "Failed to fetch group offsets, skipping group");
                }
            }
        }
        debug!(
            committed_partitions = all_committed_partitions.len(),
            groups_with_offsets = all_group_offsets.len(),
            "Pre-fetched offsets for all groups"
        );

        (all_group_offsets, all_committed_partitions)
    }

    /// Fetch compacted topics, using a TTL-based cache to avoid repeated metadata lookups.
    async fn fetch_compacted_topics_cached(&self) -> HashSet<String> {
        let mut cache = self.compacted_topics_cache.lock().await;
        if let Some((ref topics, fetched_at)) = *cache
            && fetched_at.elapsed() < self.compacted_topics_cache_ttl
        {
            debug!("Using cached compacted topics list");
            return topics.clone();
        }
        let topics = self.fetch_compacted_topics_uncached().await;
        *cache = Some((topics.clone(), Instant::now()));
        topics
    }

    async fn fetch_compacted_topics_uncached(&self) -> HashSet<String> {
        match self.client.fetch_compacted_topics().await {
            Ok(topics) => {
                if !topics.is_empty() {
                    trace!(compacted_topics = ?topics, "Identified compacted topics");
                }
                topics
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch topic configs, assuming no compacted topics");
                HashSet::new()
            }
        }
    }
}

/// Fetch timestamps for lagging partitions within a single group.
/// Uses `buffer_unordered` to limit concurrency of blocking timestamp fetches.
async fn fetch_group_timestamps(
    sampler: &TimestampSampler,
    group: &GroupSnapshot,
    watermarks: &HashMap<TopicPartition, (i64, i64)>,
    max_concurrent: usize,
) -> HashMap<(String, TopicPartition), TimestampData> {
    let requests: Vec<(TopicPartition, i64)> = group
        .offsets
        .iter()
        .filter_map(|(tp, committed_offset)| {
            let (low, high) = watermarks
                .get(tp)
                .copied()
                .unwrap_or((*committed_offset, *committed_offset));
            if *committed_offset < low {
                // Data loss: message at committed offset no longer exists — evict stale cache
                sampler.remove_entry(&group.group_id, tp);
                return None;
            }
            (high - committed_offset > 0).then(|| (tp.clone(), *committed_offset))
        })
        .collect();

    if requests.is_empty() {
        return HashMap::new();
    }

    let group_id = group.group_id.clone();

    let mut timestamps = HashMap::new();
    let mut stream = futures::stream::iter(requests)
        .map(|(tp, offset)| {
            let sampler = sampler.clone();
            let group_id = group_id.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    let ts = sampler.get_timestamp(&group_id, &tp, offset);
                    (group_id, tp, ts)
                })
                .await
            }
        })
        .buffer_unordered(max_concurrent);

    while let Some(result) = stream.next().await {
        match result {
            Ok((gid, tp, Ok(Some(ts_result)))) => {
                timestamps.insert(
                    (gid, tp),
                    TimestampData {
                        timestamp_ms: ts_result.timestamp_ms,
                    },
                );
            }
            Ok((gid, tp, Ok(None))) => {
                trace!(
                    group = gid,
                    topic = tp.topic,
                    partition = tp.partition,
                    "No timestamp available"
                );
            }
            Ok((gid, tp, Err(e))) => {
                warn!(
                    group = gid,
                    topic = tp.topic,
                    partition = tp.partition,
                    error = %e,
                    "Failed to fetch timestamp"
                );
            }
            Err(e) => {
                warn!(error = %e, "Timestamp fetch task panicked");
            }
        }
    }

    timestamps
}

impl std::fmt::Debug for ClusterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterManager")
            .field("cluster_name", &self.cluster_name)
            .field("poll_interval", &self.poll_interval)
            .field("granularity", &self.granularity)
            .finish_non_exhaustive()
    }
}
