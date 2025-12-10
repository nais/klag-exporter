use crate::collector::lag_calculator::LagCalculator;
use crate::collector::offset_collector::OffsetCollector;
use crate::collector::timestamp_sampler::TimestampSampler;
use crate::config::{ClusterConfig, ExporterConfig, Granularity};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use crate::kafka::TimestampConsumer;
use crate::metrics::registry::MetricsRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Semaphore};
use tracing::{debug, error, info, instrument, warn};

pub struct ClusterManager {
    cluster_name: String,
    cluster_labels: HashMap<String, String>,
    offset_collector: OffsetCollector,
    timestamp_sampler: Option<TimestampSampler>,
    registry: Arc<MetricsRegistry>,
    poll_interval: Duration,
    max_backoff: Duration,
    granularity: Granularity,
    max_concurrent_fetches: usize,
    cache_cleanup_interval: Duration,
}

impl ClusterManager {
    pub fn new(
        config: ClusterConfig,
        registry: Arc<MetricsRegistry>,
        exporter_config: &ExporterConfig,
    ) -> Result<Self> {
        let cluster_name = config.name.clone();
        let cluster_labels = config.labels.clone();
        let filters = config.compile_filters()?;

        let client = Arc::new(KafkaClient::new(&config)?);
        let offset_collector = OffsetCollector::new(Arc::clone(&client), filters);

        let timestamp_sampler = if exporter_config.timestamp_sampling.enabled {
            let ts_consumer = TimestampConsumer::new(&config)?;
            Some(TimestampSampler::new(
                ts_consumer,
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

        Ok(Self {
            cluster_name,
            cluster_labels,
            offset_collector,
            timestamp_sampler,
            registry,
            poll_interval: exporter_config.poll_interval,
            max_backoff: Duration::from_secs(300),
            granularity: exporter_config.granularity,
            max_concurrent_fetches: exporter_config.timestamp_sampling.max_concurrent_fetches,
            cache_cleanup_interval: exporter_config.timestamp_sampling.cache_ttl * 2,
        })
    }

    #[instrument(skip(self, shutdown), fields(cluster = %self.cluster_name))]
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        info!(cluster = %self.cluster_name, "Starting collection loop");

        let mut interval = tokio::time::interval(self.poll_interval);
        let mut cache_cleanup_interval = tokio::time::interval(self.cache_cleanup_interval);
        let mut consecutive_errors = 0u32;
        let mut current_backoff = Duration::from_secs(1);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.collect_once().await {
                        Ok(()) => {
                            consecutive_errors = 0;
                            current_backoff = Duration::from_secs(1);
                            self.registry.set_healthy(true);
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            error!(
                                cluster = %self.cluster_name,
                                error = %e,
                                consecutive_errors = consecutive_errors,
                                "Collection failed"
                            );

                            if consecutive_errors >= 3 {
                                self.registry.set_healthy(false);

                                let backoff = current_backoff.min(self.max_backoff);
                                warn!(
                                    cluster = %self.cluster_name,
                                    backoff_secs = backoff.as_secs(),
                                    "Applying backoff due to consecutive errors"
                                );

                                tokio::time::sleep(backoff).await;
                                current_backoff = (current_backoff * 2).min(self.max_backoff);
                            }
                        }
                    }
                }
                _ = cache_cleanup_interval.tick() => {
                    // Periodic cache cleanup
                    if let Some(ref sampler) = self.timestamp_sampler {
                        let before = sampler.cache_size();
                        sampler.clear_stale_entries();
                        let after = sampler.cache_size();
                        if before != after {
                            debug!(
                                cluster = %self.cluster_name,
                                before = before,
                                after = after,
                                "Cleaned up stale cache entries"
                            );
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!(cluster = %self.cluster_name, "Received shutdown signal");
                    break;
                }
            }
        }

        // Cleanup
        self.registry.remove_cluster(&self.cluster_name);
        info!(cluster = %self.cluster_name, "Collection loop stopped");
    }

    #[instrument(skip(self), fields(cluster = %self.cluster_name))]
    async fn collect_once(&self) -> Result<()> {
        let start = Instant::now();

        // Collect offsets
        let snapshot = tokio::task::block_in_place(|| self.offset_collector.collect())?;

        debug!(
            cluster = %self.cluster_name,
            groups = snapshot.groups.len(),
            partitions = snapshot.watermarks.len(),
            "Collected offsets"
        );

        // Collect timestamps if enabled (with concurrency limit)
        let timestamps = if let Some(ref sampler) = self.timestamp_sampler {
            self.collect_timestamps_concurrent(sampler, &snapshot).await
        } else {
            HashMap::new()
        };

        // Calculate lag metrics
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let poll_time_ms = start.elapsed().as_millis() as u64;
        let lag_metrics = LagCalculator::calculate(&snapshot, &timestamps, now_ms, poll_time_ms);

        // Update registry with granularity and custom labels
        self.registry.update_with_options(
            &self.cluster_name,
            lag_metrics,
            self.granularity,
            &self.cluster_labels,
        );

        // Record scrape duration
        let scrape_duration_ms = start.elapsed().as_millis() as u64;
        self.registry.set_scrape_duration_ms(scrape_duration_ms);

        debug!(
            cluster = %self.cluster_name,
            elapsed_ms = scrape_duration_ms,
            timestamp_cache_size = self.timestamp_sampler.as_ref().map(|s| s.cache_size()).unwrap_or(0),
            "Collection cycle completed"
        );

        Ok(())
    }

    async fn collect_timestamps_concurrent(
        &self,
        sampler: &TimestampSampler,
        snapshot: &crate::collector::offset_collector::OffsetsSnapshot,
    ) -> HashMap<(String, TopicPartition), i64> {
        // Build list of requests
        let mut requests: Vec<(String, TopicPartition, i64)> = Vec::new();

        for group in &snapshot.groups {
            for (tp, committed_offset) in &group.offsets {
                let high_watermark = snapshot.get_high_watermark(tp).unwrap_or(*committed_offset);
                let lag = high_watermark - committed_offset;

                if lag > 0 {
                    requests.push((group.group_id.clone(), tp.clone(), *committed_offset));
                }
            }
        }

        if requests.is_empty() {
            return HashMap::new();
        }

        debug!(
            cluster = %self.cluster_name,
            request_count = requests.len(),
            max_concurrent = self.max_concurrent_fetches,
            "Fetching timestamps concurrently"
        );

        // Use semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent_fetches));
        let mut handles = Vec::with_capacity(requests.len());

        for (group_id, tp, offset) in requests {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let sampler_ref = sampler;
            let group_id_clone = group_id.clone();
            let tp_clone = tp.clone();

            // Use block_in_place for synchronous Kafka operations
            let result = tokio::task::block_in_place(|| {
                let res = sampler_ref.get_timestamp(&group_id_clone, &tp_clone, offset);
                drop(permit);
                res
            });

            handles.push(((group_id, tp), result));
        }

        // Collect results
        let mut timestamps = HashMap::new();
        for ((group_id, tp), result) in handles {
            match result {
                Ok(Some(ts)) => {
                    timestamps.insert((group_id, tp), ts);
                }
                Ok(None) => {
                    debug!(
                        group = group_id,
                        topic = tp.topic,
                        partition = tp.partition,
                        "No timestamp available"
                    );
                }
                Err(e) => {
                    warn!(
                        group = group_id,
                        topic = tp.topic,
                        partition = tp.partition,
                        error = %e,
                        "Failed to fetch timestamp"
                    );
                }
            }
        }

        timestamps
    }
}

impl std::fmt::Debug for ClusterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterManager")
            .field("cluster_name", &self.cluster_name)
            .field("poll_interval", &self.poll_interval)
            .field("granularity", &self.granularity)
            .finish()
    }
}
