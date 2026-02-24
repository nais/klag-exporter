use crate::config::{ClusterConfig, PerformanceConfig};
use crate::error::{KlagError, Result};
use rdkafka::TopicPartitionList;
use rdkafka::admin::{AdminClient, AdminOptions, ListConsumerGroupOffsets, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::groups::GroupList;
use rdkafka::metadata::Metadata;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, instrument, trace, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
}

#[derive(Debug, Clone)]
pub struct GroupMemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<TopicPartition>,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub members: Vec<GroupMemberInfo>,
}

pub struct KafkaClient {
    admin: AdminClient<DefaultClientContext>,
    consumer: BaseConsumer,
    config: ClusterConfig,
    timeout: Duration,
    performance: PerformanceConfig,
}

impl KafkaClient {
    pub fn with_performance(
        config: &ClusterConfig,
        performance: PerformanceConfig,
    ) -> Result<Self> {
        let timeout = performance.kafka_timeout;

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.bootstrap_servers);
        client_config.set("client.id", format!("klag-exporter-{}", config.name));

        for (key, value) in &config.consumer_properties {
            client_config.set(key, value);
        }

        let admin: AdminClient<DefaultClientContext> =
            client_config.create().map_err(KlagError::Kafka)?;

        let consumer: BaseConsumer = client_config
            .clone()
            .set(
                "group.id",
                format!("klag-exporter-internal-{}", config.name),
            )
            .set("enable.auto.commit", "false")
            .create()
            .map_err(KlagError::Kafka)?;

        Ok(Self {
            admin,
            consumer,
            config: config.clone(),
            timeout,
            performance,
        })
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
        let group_list: GroupList = tokio::task::block_in_place(|| {
            self.consumer
                .fetch_group_list(None, self.timeout)
                .map_err(KlagError::Kafka)
        })?;

        let groups = group_list
            .groups()
            .iter()
            .map(|g| ConsumerGroupInfo {
                group_id: g.name().to_string(),
            })
            .collect();

        Ok(groups)
    }

    #[instrument(skip(self, group_ids), fields(cluster = %self.config.name, count = group_ids.len()))]
    pub async fn describe_consumer_groups(
        &self,
        group_ids: &[&str],
    ) -> Result<Vec<GroupDescription>> {
        let batch_size = self.performance.describe_groups_batch_size;
        const MAX_CONCURRENT_BATCHES: usize = 5;

        let mut descriptions = Vec::with_capacity(group_ids.len());

        // Limit concurrent admin ops to avoid overwhelming librdkafka's
        // internal broker threads (which can SIGSEGV under heavy concurrent load).
        let mut remaining = group_ids
            .chunks(batch_size)
            .map(|chunk| {
                let opts = self.admin_options();
                let ids: Vec<String> = chunk.iter().map(|s| (*s).to_string()).collect();
                async move {
                    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
                    self.admin
                        .describe_consumer_groups(refs.iter(), &opts)
                        .await
                        .map_err(KlagError::Kafka)
                }
            });
        loop {
            let round: Vec<_> = remaining.by_ref().take(MAX_CONCURRENT_BATCHES).collect();
            if round.is_empty() {
                break;
            }
            let batch_results = futures::future::try_join_all(round).await?;

            for results in batch_results {
                for result in results {
                    match result {
                        Ok(desc) => {
                            let members = desc
                                .members
                                .into_iter()
                                .map(|m| {
                                    let assignments = m
                                        .assignment
                                        .map(|a| {
                                            a.partitions
                                                .elements()
                                                .iter()
                                                .map(|e| {
                                                    TopicPartition::new(e.topic(), e.partition())
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();

                                    GroupMemberInfo {
                                        member_id: m.consumer_id,
                                        client_id: m.client_id,
                                        client_host: m.host,
                                        assignments,
                                    }
                                })
                                .collect();

                            descriptions.push(GroupDescription {
                                group_id: desc.group_id,
                                members,
                            });
                        }
                        Err((group_id, err)) => {
                            warn!(group = %group_id, error = %err, "Failed to describe consumer group");
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(descriptions)
    }

    /// Fetch committed offsets for a consumer group using the Admin API.
    /// Uses the existing `AdminClient` connection — no additional consumers/FDs needed.
    #[instrument(skip(self), fields(cluster = %self.config.name, group = %group_id))]
    pub async fn list_consumer_group_offsets(
        &self,
        group_id: &str,
    ) -> Result<HashMap<TopicPartition, i64>> {
        let opts = self.admin_options();
        let request = ListConsumerGroupOffsets::from_group(group_id);

        let results = self
            .admin
            .list_consumer_group_offsets(std::iter::once(&request), &opts)
            .await
            .map_err(KlagError::Kafka)?;

        let mut offsets = HashMap::new();

        for result in results {
            match result {
                Ok(consumer_group) => {
                    for elem in consumer_group.topic_partitions.elements() {
                        let offset_val = elem.offset();
                        if let rdkafka::Offset::Offset(offset) = offset_val
                            && offset >= 0
                        {
                            offsets.insert(
                                TopicPartition::new(elem.topic(), elem.partition()),
                                offset,
                            );
                        } else {
                            trace!(
                                group = group_id,
                                topic = elem.topic(),
                                partition = elem.partition(),
                                offset = ?offset_val,
                                "Skipping partition with non-numeric or negative committed offset"
                            );
                        }
                    }
                }
                Err((gid, err)) => {
                    warn!(group = %gid, error = %err, "Group result error");
                }
            }
        }

        trace!(
            group = group_id,
            partitions = offsets.len(),
            "Fetched committed offsets via Admin API"
        );
        Ok(offsets)
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn fetch_metadata(&self) -> Result<Metadata> {
        tokio::task::block_in_place(|| {
            self.consumer
                .fetch_metadata(None, self.timeout)
                .map_err(KlagError::Kafka)
        })
    }

    /// Fetch watermarks for only the specified partitions.
    ///
    /// Primary path: bulk `Admin::list_offsets` (efficient for large partition sets).
    /// Fallback: per-partition `Consumer::fetch_watermarks` for any partitions where
    /// `list_offsets` returned an unresolved offset (sentinel value like `Offset::End`).
    #[instrument(skip(self, partitions), fields(cluster = %self.config.name, count = partitions.len()))]
    pub async fn fetch_watermarks_for_partitions(
        &self,
        partitions: &HashSet<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        let mut tpl_earliest = TopicPartitionList::new();
        let mut tpl_latest = TopicPartitionList::new();

        for tp in partitions {
            tpl_earliest
                .add_partition_offset(&tp.topic, tp.partition, rdkafka::Offset::Beginning)
                .map_err(KlagError::Kafka)?;
            tpl_latest
                .add_partition_offset(&tp.topic, tp.partition, rdkafka::Offset::End)
                .map_err(KlagError::Kafka)?;
        }

        let total = tpl_earliest.count();
        debug!(
            partitions = total,
            "Fetching targeted watermarks via Admin API"
        );

        let opts = self.admin_options();
        let (earliest_results, latest_results) = futures::future::try_join(
            self.admin.list_offsets(&tpl_earliest, &opts),
            self.admin.list_offsets(&tpl_latest, &opts),
        )
        .await
        .map_err(KlagError::Kafka)?;

        let earliest_map = Self::collect_resolved_offsets(earliest_results, "earliest");
        let mut watermarks = Self::build_watermarks(latest_results, &earliest_map);

        self.fallback_missing_watermarks(partitions, &mut watermarks, total);

        debug!(
            fetched = watermarks.len(),
            requested = total,
            "Targeted watermark fetch completed"
        );

        Ok(watermarks)
    }

    /// Extract resolved numeric offsets from `list_offsets` results, logging unresolved variants.
    fn collect_resolved_offsets(
        results: Vec<rdkafka::admin::ListOffsetsResult>,
        label: &str,
    ) -> HashMap<(String, i32), i64> {
        let mut map = HashMap::new();
        for result in results {
            match result {
                Ok(info) => {
                    if let rdkafka::Offset::Offset(offset) = info.offset {
                        map.insert((info.topic, info.partition), offset);
                    } else {
                        warn!(
                            topic = %info.topic,
                            partition = info.partition,
                            offset = ?info.offset,
                            "list_offsets returned unresolved {label} offset variant"
                        );
                    }
                }
                Err((topic, partition, err)) => {
                    warn!(topic = %topic, partition, error = %err, "Failed to fetch {label} offset");
                }
            }
        }
        map
    }

    /// Build (low, high) watermark map from latest-offset results paired with earliest offsets.
    fn build_watermarks(
        latest_results: Vec<rdkafka::admin::ListOffsetsResult>,
        earliest_map: &HashMap<(String, i32), i64>,
    ) -> HashMap<TopicPartition, (i64, i64)> {
        let mut watermarks = HashMap::new();
        for result in latest_results {
            match result {
                Ok(info) => {
                    if let rdkafka::Offset::Offset(high) = info.offset {
                        let low = earliest_map
                            .get(&(info.topic.clone(), info.partition))
                            .copied()
                            .unwrap_or(0);
                        watermarks.insert(
                            TopicPartition::new(&info.topic, info.partition),
                            (low, high),
                        );
                    } else {
                        warn!(
                            topic = %info.topic,
                            partition = info.partition,
                            offset = ?info.offset,
                            "list_offsets returned unresolved latest offset variant"
                        );
                    }
                }
                Err((topic, partition, err)) => {
                    warn!(topic = %topic, partition, error = %err, "Failed to fetch latest offset");
                }
            }
        }
        watermarks
    }

    /// For any partitions not resolved by `list_offsets`, fall back to
    /// the proven `Consumer::fetch_watermarks` (`rd_kafka_query_watermark_offsets`).
    fn fallback_missing_watermarks(
        &self,
        partitions: &HashSet<TopicPartition>,
        watermarks: &mut HashMap<TopicPartition, (i64, i64)>,
        total: usize,
    ) {
        let missing: Vec<_> = partitions
            .iter()
            .filter(|tp| !watermarks.contains_key(*tp))
            .collect();

        if missing.is_empty() {
            return;
        }

        warn!(
            missing = missing.len(),
            resolved = watermarks.len(),
            total,
            "list_offsets did not resolve all partitions, falling back to Consumer::fetch_watermarks"
        );

        tokio::task::block_in_place(|| {
            for tp in &missing {
                match self
                    .consumer
                    .fetch_watermarks(&tp.topic, tp.partition, self.timeout)
                {
                    Ok((low, high)) => {
                        watermarks.insert((*tp).clone(), (low, high));
                    }
                    Err(err) => {
                        warn!(
                            topic = %tp.topic,
                            partition = tp.partition,
                            error = %err,
                            "Fallback watermark fetch also failed"
                        );
                    }
                }
            }
        });
    }

    pub fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeout))
    }

    /// Fetch topics that have compaction enabled (cleanup.policy contains "compact").
    /// Batches `describe_configs` in chunks to avoid broker timeouts on large clusters.
    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn fetch_compacted_topics(&self) -> Result<HashSet<String>> {
        const BATCH_SIZE: usize = 500;

        let metadata = self.fetch_metadata().await?;
        let topic_names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect();

        if topic_names.is_empty() {
            return Ok(HashSet::new());
        }

        let mut compacted_topics = HashSet::new();

        for chunk in topic_names.chunks(BATCH_SIZE) {
            let resources: Vec<ResourceSpecifier> = chunk
                .iter()
                .map(|name| ResourceSpecifier::Topic(name.as_str()))
                .collect();

            let opts = self.admin_options();
            let results = self
                .admin
                .describe_configs(resources.iter(), &opts)
                .await
                .map_err(KlagError::Kafka)?;

            for result in results {
                match result {
                    Ok(resource) => {
                        // Extract topic name from OwnedResourceSpecifier
                        let topic_name = match &resource.specifier {
                            rdkafka::admin::OwnedResourceSpecifier::Topic(name) => name.clone(),
                            _ => continue, // Skip non-topic resources
                        };
                        for entry in resource.entries {
                            if entry.name == "cleanup.policy"
                                && let Some(value) = entry.value
                                && value.contains("compact")
                            {
                                trace!(topic = %topic_name, cleanup_policy = %value, "Topic has compaction enabled");
                                compacted_topics.insert(topic_name.clone());
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            "Failed to describe config for resource"
                        );
                    }
                }
            }
        }

        debug!(
            count = compacted_topics.len(),
            topics = ?compacted_topics,
            "Identified compacted topics"
        );

        Ok(compacted_topics)
    }
}

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("cluster", &self.config.name)
            .finish_non_exhaustive()
    }
}
