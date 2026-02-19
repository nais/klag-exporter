use crate::config::{ClusterConfig, PerformanceConfig};
use crate::error::{KlagError, Result};
use rdkafka::admin::{AdminClient, AdminOptions, ListConsumerGroupOffsets, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::groups::GroupList;
use rdkafka::metadata::Metadata;
use rdkafka::TopicPartitionList;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, instrument, warn};

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
    #[allow(dead_code)]
    pub protocol_type: String,
    #[allow(dead_code)]
    pub state: String,
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
    pub state: String,
    #[allow(dead_code)]
    pub protocol_type: String,
    #[allow(dead_code)]
    pub protocol: String,
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
    /// Create a new `KafkaClient` with default performance config.
    /// Prefer `with_performance` for large clusters.
    #[allow(dead_code)]
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        Self::with_performance(config, PerformanceConfig::default())
    }

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

    #[allow(dead_code)]
    pub const fn performance(&self) -> &PerformanceConfig {
        &self.performance
    }

    pub fn cluster_name(&self) -> &str {
        &self.config.name
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
        let group_list: GroupList = self
            .consumer
            .fetch_group_list(None, self.timeout)
            .map_err(KlagError::Kafka)?;

        let groups = group_list
            .groups()
            .iter()
            .map(|g| ConsumerGroupInfo {
                group_id: g.name().to_string(),
                protocol_type: g.protocol_type().to_string(),
                state: g.state().to_string(),
            })
            .collect();

        debug!(count = group_list.groups().len(), "Listed consumer groups");
        Ok(groups)
    }

    #[instrument(skip(self, group_ids), fields(cluster = %self.config.name, count = group_ids.len()))]
    pub async fn describe_consumer_groups(
        &self,
        group_ids: &[&str],
    ) -> Result<Vec<GroupDescription>> {
        let opts = self.admin_options();
        let results = self
            .admin
            .describe_consumer_groups(group_ids.iter(), &opts)
            .await
            .map_err(KlagError::Kafka)?;

        let mut descriptions = Vec::with_capacity(group_ids.len());

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
                                        .map(|e| TopicPartition::new(e.topic(), e.partition()))
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
                        state: format!("{:?}", desc.state),
                        protocol_type: desc.partition_assignor.clone(),
                        protocol: desc.partition_assignor,
                        members,
                    });
                }
                Err((group_id, err)) => {
                    warn!(group = %group_id, error = %err, "Failed to describe consumer group");
                }
            }
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
                        if let rdkafka::Offset::Offset(offset) = elem.offset() {
                            if offset >= 0 {
                                offsets.insert(
                                    TopicPartition::new(elem.topic(), elem.partition()),
                                    offset,
                                );
                            }
                        }
                    }
                }
                Err((gid, err)) => {
                    warn!(group = %gid, error = %err, "Group result error");
                }
            }
        }

        debug!(
            group = group_id,
            partitions = offsets.len(),
            "Fetched committed offsets via Admin API"
        );
        Ok(offsets)
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn fetch_metadata(&self) -> Result<Metadata> {
        self.consumer
            .fetch_metadata(None, self.timeout)
            .map_err(KlagError::Kafka)
    }

    /// Fetch watermarks for all partitions in parallel using the Admin API.
    /// Uses two batched `list_offsets` calls (earliest + latest) instead of per-partition `fetch_watermarks`.
    #[instrument(skip(self))]
    pub async fn fetch_all_watermarks_parallel(
        &self,
    ) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        let metadata = self.fetch_metadata()?;

        // Build TopicPartitionList with all partitions
        let mut tpl_earliest = TopicPartitionList::new();
        let mut tpl_latest = TopicPartitionList::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                tpl_earliest
                    .add_partition_offset(topic.name(), partition.id(), rdkafka::Offset::Beginning)
                    .map_err(KlagError::Kafka)?;
                tpl_latest
                    .add_partition_offset(topic.name(), partition.id(), rdkafka::Offset::End)
                    .map_err(KlagError::Kafka)?;
            }
        }

        let total_partitions = tpl_earliest.count();
        debug!(
            partitions = total_partitions,
            "Fetching watermarks via Admin API list_offsets"
        );

        let opts = self.admin_options();

        // Issue both calls concurrently
        let (earliest_results, latest_results) = futures::future::try_join(
            self.admin.list_offsets(&tpl_earliest, &opts),
            self.admin.list_offsets(&tpl_latest, &opts),
        )
        .await
        .map_err(KlagError::Kafka)?;

        // Build earliest offsets map
        let mut earliest_map: HashMap<(String, i32), i64> = HashMap::new();
        for result in earliest_results {
            match result {
                Ok(info) => {
                    if let rdkafka::Offset::Offset(offset) = info.offset {
                        earliest_map.insert((info.topic, info.partition), offset);
                    }
                }
                Err((topic, partition, err)) => {
                    warn!(topic = %topic, partition = partition, error = %err, "Failed to fetch earliest offset");
                }
            }
        }

        // Build combined watermarks map from latest results
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
                    }
                }
                Err((topic, partition, err)) => {
                    warn!(topic = %topic, partition = partition, error = %err, "Failed to fetch latest offset");
                }
            }
        }

        debug!(
            fetched = watermarks.len(),
            total = total_partitions,
            "Admin API watermark fetch completed"
        );

        Ok(watermarks)
    }

    #[allow(dead_code)]
    pub const fn inner_admin(&self) -> &AdminClient<DefaultClientContext> {
        &self.admin
    }

    pub fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeout))
    }

    /// Fetch topics that have compaction enabled (cleanup.policy contains "compact")
    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn fetch_compacted_topics(&self) -> Result<HashSet<String>> {
        let metadata = self.fetch_metadata()?;
        let topic_names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect();

        if topic_names.is_empty() {
            return Ok(HashSet::new());
        }

        let resources: Vec<ResourceSpecifier> = topic_names
            .iter()
            .map(|name| ResourceSpecifier::Topic(name.as_str()))
            .collect();

        let opts = self.admin_options();
        let results = self
            .admin
            .describe_configs(resources.iter(), &opts)
            .await
            .map_err(KlagError::Kafka)?;

        let mut compacted_topics = HashSet::new();

        for result in results {
            match result {
                Ok(resource) => {
                    // Extract topic name from OwnedResourceSpecifier
                    let topic_name = match &resource.specifier {
                        rdkafka::admin::OwnedResourceSpecifier::Topic(name) => name.clone(),
                        _ => continue, // Skip non-topic resources
                    };
                    for entry in resource.entries {
                        if entry.name == "cleanup.policy" {
                            if let Some(value) = entry.value {
                                if value.contains("compact") {
                                    debug!(topic = %topic_name, cleanup_policy = %value, "Topic has compaction enabled");
                                    compacted_topics.insert(topic_name.clone());
                                }
                            }
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
            .finish()
    }
}
