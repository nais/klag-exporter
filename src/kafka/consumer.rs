use crate::config::ClusterConfig;
use crate::error::{KlagError, Result};
use crate::kafka::client::TopicPartition;
use rdkafka::Offset;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use tracing::{debug, instrument, trace, warn};

/// Result of fetching a timestamp from Kafka
#[derive(Debug, Clone)]
pub struct TimestampFetchResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

/// Pool-based timestamp consumer. Maintains a pool of reusable `BaseConsumers`
/// to avoid connection churn (TCP/TLS/SASL handshake per fetch).
pub struct TimestampConsumer {
    config: ClusterConfig,
    cluster_name: String,
    fetch_timeout: Duration,
    consumer_counter: AtomicU64,
    pool: Mutex<Vec<BaseConsumer>>,
    pool_size: usize,
}

impl TimestampConsumer {
    pub fn with_pool_size(config: &ClusterConfig, pool_size: usize) -> Result<Self> {
        let mut consumer = Self {
            config: config.clone(),
            cluster_name: config.name.clone(),
            fetch_timeout: Duration::from_secs(5),
            consumer_counter: AtomicU64::new(0),
            pool: Mutex::new(Vec::with_capacity(pool_size)),
            pool_size,
        };

        // Pre-populate the pool
        for _ in 0..pool_size {
            let c = consumer.create_consumer()?;
            consumer
                .pool
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .push(c);
        }

        debug!(
            cluster = %consumer.cluster_name,
            pool_size = pool_size,
            "Created timestamp consumer pool"
        );

        Ok(consumer)
    }

    /// Create a consumer for the pool.
    fn create_consumer(&self) -> Result<BaseConsumer> {
        let counter = self.consumer_counter.fetch_add(1, Ordering::Relaxed);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set(
                "client.id",
                format!("klag-exporter-ts-{}-{}", self.config.name, counter),
            )
            .set(
                "group.id",
                format!("klag-exporter-ts-internal-{}", self.config.name),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Small fetch size for timestamp sampling
            .set("fetch.max.bytes", "1048576")
            .set("max.partition.fetch.bytes", "262144");

        for (key, value) in &self.config.consumer_properties {
            client_config.set(key, value);
        }

        client_config.create().map_err(KlagError::Kafka)
    }

    /// Take a consumer from the pool, or create a new one if the pool is empty.
    /// If `self.pool` is exhausted (more concurrent fetches than `pool_size`); create a temporary one
    fn acquire(&self) -> Result<BaseConsumer> {
        self.pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .map_or_else(|| self.create_consumer(), Ok)
    }

    /// Return a consumer to the pool. If the pool is full, the consumer is dropped.
    fn release(&self, consumer: BaseConsumer) {
        // Unassign before returning to pool to clear any partition state
        let empty = rdkafka::TopicPartitionList::new();
        if let Err(e) = consumer.assign(&empty) {
            warn!(error = %e, "Failed to unassign consumer before returning to pool");
            // Don't return a broken consumer to the pool
            return;
        }

        let mut pool = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        if pool.len() < self.pool_size {
            pool.push(consumer);
        }
        // else: pool is full, consumer is dropped
    }

    #[instrument(skip(self), fields(cluster = %self.cluster_name, topic = %tp.topic, partition = tp.partition, offset = offset))]
    pub fn fetch_timestamp(
        &self,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<Option<TimestampFetchResult>> {
        use rdkafka::TopicPartitionList;

        let consumer = self.acquire()?;

        // RAII guard ensures consumer is returned to pool even on early return/panic.
        // We borrow the consumer before wrapping it — the guard only owns it for Drop.
        struct PoolGuard<'a> {
            consumer: Option<BaseConsumer>,
            pool: &'a TimestampConsumer,
        }
        impl Drop for PoolGuard<'_> {
            fn drop(&mut self) {
                if let Some(consumer) = self.consumer.take() {
                    self.pool.release(consumer);
                }
            }
        }

        let mut guard = PoolGuard {
            consumer: Some(consumer),
            pool: self,
        };
        // We just set consumer to Some, so as_mut() always returns Some here.
        // The guard retains ownership for Drop (pool return).
        let Some(consumer) = guard.consumer.as_mut() else {
            // Structurally unreachable — we just set it to Some above
            return Ok(None);
        };

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(offset))
            .map_err(KlagError::Kafka)?;

        consumer.assign(&tpl).map_err(KlagError::Kafka)?;

        consumer.poll(self.fetch_timeout).map_or_else(
            || {
                debug!(
                    topic = tp.topic,
                    partition = tp.partition,
                    offset = offset,
                    "No message available at offset (may be beyond high watermark)"
                );
                Ok(None)
            },
            |result| match result {
                Ok(msg) => {
                    let timestamp = msg.timestamp().to_millis();

                    trace!(
                        topic = tp.topic,
                        partition = tp.partition,
                        requested_offset = offset,
                        actual_offset = msg.offset(),
                        timestamp = ?timestamp,
                        "Fetched message timestamp"
                    );

                    Ok(timestamp.map(|ts| TimestampFetchResult { timestamp_ms: ts }))
                }
                Err(e) => {
                    warn!(
                        topic = tp.topic,
                        partition = tp.partition,
                        error = %e,
                        "Failed to fetch message"
                    );
                    Err(KlagError::Kafka(e))
                }
            },
        )
    }
}

impl std::fmt::Debug for TimestampConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampConsumer")
            .field("cluster", &self.cluster_name)
            .field("pool_size", &self.pool_size)
            .finish_non_exhaustive()
    }
}
