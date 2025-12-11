use crate::config::ClusterConfig;
use crate::error::{KlagError, Result};
use crate::kafka::client::TopicPartition;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::Offset;
use std::time::Duration;
use tracing::{debug, instrument, warn};

/// Result of fetching a timestamp from Kafka
#[derive(Debug, Clone)]
pub struct TimestampFetchResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

pub struct TimestampConsumer {
    consumer: BaseConsumer,
    cluster_name: String,
    fetch_timeout: Duration,
}

impl TimestampConsumer {
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set(
                "client.id",
                format!("klag-exporter-ts-{}", config.name),
            )
            .set(
                "group.id",
                format!("klag-exporter-ts-internal-{}", config.name),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Small fetch size for timestamp sampling
            .set("fetch.max.bytes", "1048576")
            .set("max.partition.fetch.bytes", "262144");

        for (key, value) in &config.consumer_properties {
            client_config.set(key, value);
        }

        let consumer: BaseConsumer = client_config.create()?;

        Ok(Self {
            consumer,
            cluster_name: config.name.clone(),
            fetch_timeout: Duration::from_secs(5),
        })
    }

    #[instrument(skip(self), fields(cluster = %self.cluster_name, topic = %tp.topic, partition = tp.partition, offset = offset))]
    pub fn fetch_timestamp(&self, tp: &TopicPartition, offset: i64) -> Result<Option<TimestampFetchResult>> {
        use rdkafka::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(offset))
            .map_err(KlagError::Kafka)?;

        self.consumer
            .assign(&tpl)
            .map_err(KlagError::Kafka)?;

        // Seek to the exact offset to ensure we get that specific message
        self.consumer
            .seek(&tp.topic, tp.partition, Offset::Offset(offset), Duration::from_secs(5))
            .map_err(KlagError::Kafka)?;

        // Poll for message
        match self.consumer.poll(self.fetch_timeout) {
            Some(result) => match result {
                Ok(msg) => {
                    let timestamp = msg.timestamp().to_millis();

                    debug!(
                        topic = tp.topic,
                        partition = tp.partition,
                        requested_offset = offset,
                        actual_offset = msg.offset(),
                        timestamp = ?timestamp,
                        "Fetched message timestamp"
                    );

                    Ok(timestamp.map(|ts| TimestampFetchResult {
                        timestamp_ms: ts,
                    }))
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
            None => {
                debug!(
                    topic = tp.topic,
                    partition = tp.partition,
                    offset = offset,
                    "No message available at offset (may be beyond high watermark)"
                );
                Ok(None)
            }
        }
    }

    #[allow(dead_code)]
    pub fn fetch_timestamps_batch(
        &self,
        requests: &[(TopicPartition, i64)],
    ) -> Vec<(TopicPartition, Result<Option<TimestampFetchResult>>)> {
        requests
            .iter()
            .map(|(tp, offset)| {
                let result = self.fetch_timestamp(tp, *offset);
                (tp.clone(), result)
            })
            .collect()
    }
}

impl std::fmt::Debug for TimestampConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampConsumer")
            .field("cluster", &self.cluster_name)
            .finish()
    }
}
