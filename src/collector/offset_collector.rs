use crate::config::{CompiledFilters, PerformanceConfig};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, instrument, warn};

pub struct OffsetCollector {
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
    performance: PerformanceConfig,
}

#[derive(Debug, Clone)]
pub struct OffsetsSnapshot {
    pub cluster_name: String,
    pub groups: Vec<GroupSnapshot>,
    pub watermarks: HashMap<TopicPartition, (i64, i64)>,
    #[allow(dead_code)]
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    #[allow(dead_code)]
    pub state: String,
    pub members: Vec<MemberSnapshot>,
    pub offsets: HashMap<TopicPartition, i64>,
}

#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<TopicPartition>,
}

impl OffsetCollector {
    /// Create a new `OffsetCollector` with default performance config.
    /// Prefer `with_performance` for large clusters.
    #[allow(dead_code)]
    pub fn new(client: Arc<KafkaClient>, filters: CompiledFilters) -> Self {
        let performance = client.performance().clone();
        Self {
            client,
            filters,
            performance,
        }
    }

    pub const fn with_performance(
        client: Arc<KafkaClient>,
        filters: CompiledFilters,
        performance: PerformanceConfig,
    ) -> Self {
        Self {
            client,
            filters,
            performance,
        }
    }

    /// Collect offsets sequentially (legacy method).
    /// For large clusters, use `collect_parallel` instead.
    #[allow(dead_code)]
    #[instrument(skip(self), fields(cluster = %self.client.cluster_name()))]
    pub async fn collect(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // List all consumer groups
        let all_groups = self.client.list_consumer_groups()?;
        debug!(
            total_groups = all_groups.len(),
            "Listed all consumer groups"
        );

        // Filter groups
        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        // Get group descriptions
        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();
        let descriptions = self.client.describe_consumer_groups(&group_ids).await?;

        // Fetch watermarks for all topics
        let watermarks = self.client.fetch_all_watermarks_parallel().await?;
        debug!(partitions = watermarks.len(), "Fetched watermarks");

        // Build group snapshots
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = self
                .client
                .list_consumer_group_offsets(&desc.group_id)
                .await?;

            // Filter offsets by topic whitelist/blacklist
            let filtered_offsets: HashMap<TopicPartition, i64> = offsets
                .into_iter()
                .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
                .collect();

            let members = desc
                .members
                .into_iter()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id,
                    client_id: m.client_id,
                    client_host: m.client_host,
                    assignments: m.assignments,
                })
                .collect();

            groups.push(GroupSnapshot {
                group_id: desc.group_id,
                state: desc.state,
                members,
                offsets: filtered_offsets,
            });
        }

        // Filter watermarks by topic
        let filtered_watermarks: HashMap<TopicPartition, (i64, i64)> = watermarks
            .into_iter()
            .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
            .collect();

        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "Collection completed");

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks: filtered_watermarks,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Collect offsets with parallel watermark and group offset fetching.
    /// Builds `GroupSnapshot`s directly as each group's offsets arrive — no intermediate `HashMap`.
    #[instrument(skip(self))]
    pub async fn collect_parallel(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // List all consumer groups (single call, cannot parallelize)
        let all_groups = self.client.list_consumer_groups()?;
        debug!(
            total_groups = all_groups.len(),
            "Listed all consumer groups"
        );

        // Filter groups
        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        // Get group descriptions via Admin API
        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();
        let descriptions = self.client.describe_consumer_groups(&group_ids).await?;
        debug!(
            descriptions = descriptions.len(),
            "Fetched consumer group's descriptions"
        );

        // Fetch watermarks in parallel via Admin API
        let watermarks = self.client.fetch_all_watermarks_parallel().await?;
        debug!(
            partitions = watermarks.len(),
            "Fetched watermarks (parallel)"
        );

        // Fetch group offsets and build GroupSnapshots directly, in parallel
        let groups = self.fetch_group_snapshots_parallel(&descriptions).await;

        // Filter watermarks by topic
        let filtered_watermarks: HashMap<TopicPartition, (i64, i64)> = watermarks
            .into_iter()
            .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
            .collect();

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "Parallel collection completed"
        );

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks: filtered_watermarks,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Fetch offsets for all groups in parallel and build `GroupSnapshot`s directly.
    /// Each task produces a complete `GroupSnapshot` — no intermediate `HashMap` needed.
    async fn fetch_group_snapshots_parallel(
        &self,
        descriptions: &[crate::kafka::client::GroupDescription],
    ) -> Vec<GroupSnapshot> {
        let max_concurrent = self.performance.max_concurrent_groups;

        debug!(
            groups = descriptions.len(),
            max_concurrent = max_concurrent,
            "Fetching group offsets in parallel via Admin API"
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let client = Arc::clone(&self.client);
        let filters = self.filters.clone();

        let mut handles = Vec::with_capacity(descriptions.len());

        for desc in descriptions {
            let group_id = desc.group_id.clone();
            let state = desc.state.clone();
            let members: Vec<MemberSnapshot> = desc
                .members
                .iter()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    assignments: m.assignments.clone(),
                })
                .collect();
            let permit = semaphore.clone();
            let client_clone = Arc::clone(&client);
            let filters_clone = filters.clone();

            let handle = tokio::spawn(async move {
                let _permit_guard: OwnedSemaphorePermit =
                    permit.acquire_owned().await.expect("semaphore closed");

                let offsets = client_clone.list_consumer_group_offsets(&group_id).await;

                match offsets {
                    Ok(offsets) => {
                        let filtered_offsets: HashMap<TopicPartition, i64> = offsets
                            .into_iter()
                            .filter(|(tp, _)| filters_clone.matches_topic(&tp.topic))
                            .collect();

                        Some(GroupSnapshot {
                            group_id,
                            state,
                            members,
                            offsets: filtered_offsets,
                        })
                    }
                    Err(e) => {
                        warn!(group = group_id, error = %e, "Failed to fetch group offsets");
                        Some(GroupSnapshot {
                            group_id,
                            state,
                            members,
                            offsets: HashMap::new(),
                        })
                    }
                }
            });

            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await;

        results
            .into_iter()
            .filter_map(|r| match r {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    warn!(error = %e, "Group snapshot task panicked");
                    None
                }
            })
            .collect()
    }
}

impl OffsetsSnapshot {
    #[allow(dead_code)]
    pub fn filtered_groups(&self) -> Vec<&str> {
        self.groups.iter().map(|g| g.group_id.as_str()).collect()
    }

    #[allow(dead_code)]
    pub fn get_high_watermark(&self, tp: &TopicPartition) -> Option<i64> {
        self.watermarks.get(tp).map(|(_, high)| *high)
    }
}

fn chrono_timestamp_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis(),
    )
    .expect("timestamp overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offsets_snapshot_filtered_groups() {
        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![
                GroupSnapshot {
                    group_id: "group1".to_string(),
                    state: "Stable".to_string(),
                    members: vec![],
                    offsets: HashMap::new(),
                },
                GroupSnapshot {
                    group_id: "group2".to_string(),
                    state: "Stable".to_string(),
                    members: vec![],
                    offsets: HashMap::new(),
                },
            ],
            watermarks: HashMap::new(),
            timestamp_ms: 0,
        };

        let groups = snapshot.filtered_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group1"));
        assert!(groups.contains(&"group2"));
    }
}
