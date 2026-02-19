use crate::collector::lag_calculator::{LagCalculator, PartitionOffsetMetric, TimestampData};
use crate::collector::offset_collector::{GroupSnapshot, MemberSnapshot};
use crate::collector::timestamp_sampler::TimestampSampler;
use crate::config::{ClusterConfig, CompiledFilters, ExporterConfig, Granularity};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use crate::kafka::TimestampConsumer;
use crate::leadership::LeadershipStatus;
use crate::metrics::registry::{
    build_cluster_summary_points, build_group_metric_points, build_partition_offset_points,
    MetricsRegistry,
};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex};
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
            Some(TimestampSampler::new(
                ts_consumer?,
                exporter_config.timestamp_sampling.cache_ttl,
            ))
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

        // Collection timeout should be less than poll_interval to avoid overlap
        let collection_timeout = if exporter_config.poll_interval > Duration::from_secs(10) {
            exporter_config
                .poll_interval
                .checked_sub(Duration::from_secs(5))
                .expect("should not be negative")
        } else {
            DEFAULT_COLLECTION_TIMEOUT.min(exporter_config.poll_interval)
        };

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
                    if leadership.is_leader() {
                        if let Some(ref sampler) = self.timestamp_sampler {
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

    #[allow(clippy::too_many_lines)]
    #[instrument(skip(self))]
    async fn collect_once(&self) -> Result<()> {
        let start = Instant::now();

        // 1. List and filter consumer groups
        let all_groups = self.client.list_consumer_groups()?;
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
        let descriptions = self.client.describe_consumer_groups(&group_ids).await?;
        debug!(
            descriptions = descriptions.len(),
            "Described consumer groups"
        );

        // 3. Collect consumed partitions from member assignments
        let consumed_partitions: HashSet<TopicPartition> = descriptions
            .iter()
            .flat_map(|desc| desc.members.iter())
            .flat_map(|m| m.assignments.iter())
            .filter(|tp| self.filters.matches_topic(&tp.topic))
            .cloned()
            .collect();
        debug!(
            consumed_partitions = consumed_partitions.len(),
            "Collected consumed partitions"
        );

        // 4. Fetch watermarks only for consumed partitions (targeted, no metadata call)
        let watermarks = self
            .client
            .fetch_watermarks_for_partitions(&consumed_partitions)
            .await?;
        debug!(partitions = watermarks.len(), "Fetched targeted watermarks");

        // 4. Fetch compacted topics (cached with TTL)
        let compacted_topics = {
            let mut cache = self.compacted_topics_cache.lock().await;
            if let Some((ref topics, fetched_at)) = *cache {
                if fetched_at.elapsed() < self.compacted_topics_cache_ttl {
                    debug!("Using cached compacted topics list");
                    topics.clone()
                } else {
                    let topics = self.fetch_compacted_topics_uncached().await;
                    *cache = Some((topics.clone(), Instant::now()));
                    topics
                }
            } else {
                let topics = self.fetch_compacted_topics_uncached().await;
                *cache = Some((topics.clone(), Instant::now()));
                topics
            }
        };

        // 5. Begin streaming cycle
        self.registry.begin_cycle(&self.cluster_name);

        // 6. Emit partition offset points from watermarks (already filtered to consumed partitions)
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
        drop(partition_offsets);

        // 7. Stream groups: fetch offsets → lag calc → timestamps → push points → free
        #[allow(clippy::cast_possible_truncation)]
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as i64;

        let client = Arc::clone(&self.client);
        let filters = self.filters.clone();
        let cluster_name = self.cluster_name.clone();
        let cluster_labels = self.cluster_labels.clone();
        let granularity = self.granularity;
        let compacted_topics = Arc::new(compacted_topics);
        let watermarks = Arc::new(watermarks);
        let timestamp_sampler = self.timestamp_sampler.clone();
        let max_concurrent_fetches = self.max_concurrent_fetches;

        // Accumulate counters across groups
        let mut total_compaction_detected: u64 = 0;
        let mut total_data_loss_partitions: u64 = 0;

        let mut stream = futures::stream::iter(descriptions)
            .map(|desc| {
                let client = Arc::clone(&client);
                let filters = filters.clone();
                let cluster_name = cluster_name.clone();
                let cluster_labels = cluster_labels.clone();
                let compacted_topics = Arc::clone(&compacted_topics);
                let watermarks = Arc::clone(&watermarks);
                let timestamp_sampler = timestamp_sampler.clone();

                async move {
                    // a. Fetch offsets for this group
                    let offsets = match client
                        .list_consumer_group_offsets(&desc.group_id)
                        .await
                    {
                        Ok(offsets) => offsets
                            .into_iter()
                            .filter(|(tp, _)| filters.matches_topic(&tp.topic))
                            .collect(),
                        Err(e) => {
                            warn!(group = desc.group_id, error = %e, "Failed to fetch group offsets");
                            HashMap::new()
                        }
                    };

                    // b. Build GroupSnapshot
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

                    // c. Fetch timestamps inline for lagging partitions
                    let timestamps = if let Some(ref sampler) = timestamp_sampler {
                        fetch_group_timestamps(
                            sampler,
                            &group,
                            &watermarks,
                            max_concurrent_fetches,
                        )
                        .await
                    } else {
                        HashMap::new()
                    };

                    // d. Calculate lag for this group
                    let (partition_metrics, group_metric, topic_metrics) =
                        LagCalculator::calculate_group(
                            &cluster_name,
                            &group,
                            &watermarks,
                            &timestamps,
                            now_ms,
                            &compacted_topics,
                        );

                    // Count compaction/data loss
                    let compaction_count = partition_metrics
                        .iter()
                        .filter(|m| m.compaction_detected)
                        .count() as u64;
                    let data_loss_count = partition_metrics
                        .iter()
                        .filter(|m| m.data_loss_detected)
                        .count() as u64;

                    // e. Build metric points for this group
                    let points = build_group_metric_points(
                        partition_metrics.iter(),
                        std::iter::once(&group_metric),
                        topic_metrics.iter(),
                        granularity,
                        &cluster_labels,
                    );

                    // Group data (partition_metrics, group, timestamps) freed when this block ends
                    (points, compaction_count, data_loss_count)
                }
            })
            .buffer_unordered(self.max_concurrent_groups);

        while let Some((points, compaction_count, data_loss_count)) = stream.next().await {
            // f. Push points to registry immediately
            self.registry.push_points(&self.cluster_name, points);
            total_compaction_detected += compaction_count;
            total_data_loss_partitions += data_loss_count;
        }

        // 8. Emit cluster summary points
        #[allow(clippy::cast_possible_truncation)]
        let poll_time_ms = start.elapsed().as_millis() as u64;
        self.registry.push_points(
            &self.cluster_name,
            build_cluster_summary_points(
                &self.cluster_name,
                poll_time_ms,
                total_compaction_detected,
                total_data_loss_partitions,
                &self.cluster_labels,
            ),
        );

        // 9. Finish cycle
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
            "Collection cycle completed (streaming)"
        );

        Ok(())
    }

    async fn fetch_compacted_topics_uncached(&self) -> HashSet<String> {
        match self.client.fetch_compacted_topics().await {
            Ok(topics) => {
                if !topics.is_empty() {
                    debug!(compacted_topics = ?topics, "Identified compacted topics");
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
            let high_watermark = watermarks
                .get(tp)
                .map_or(*committed_offset, |(_, high)| *high);
            (high_watermark - committed_offset > 0).then(|| (tp.clone(), *committed_offset))
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
