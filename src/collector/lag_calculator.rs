use crate::collector::offset_collector::{GroupSnapshot, OffsetsSnapshot};
use crate::kafka::client::TopicPartition;
use std::collections::{HashMap, HashSet};
use tracing::warn;

#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used by test infrastructure via update_with_options
pub struct LagMetrics {
    pub partition_metrics: Vec<PartitionLagMetric>,
    pub partition_offsets: Vec<PartitionOffsetMetric>,
    pub poll_time_ms: u64,
    /// Number of partitions where log compaction was detected
    pub compaction_detected_count: u64,
    /// Number of partitions where data loss occurred (committed offset < low watermark)
    pub data_loss_partition_count: u64,
    /// Number of partitions skipped due to missing watermarks
    pub skipped_partition_count: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionLagMetric {
    pub cluster_name: String,
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub member_host: String,
    pub consumer_id: String,
    pub client_id: String,
    pub committed_offset: i64,
    pub lag: i64,
    pub lag_seconds: Option<f64>,
    /// Whether compaction was detected for this partition's timestamp fetch
    pub compaction_detected: bool,
    /// Whether data loss occurred (`committed_offset` < `low_watermark`)
    pub data_loss_detected: bool,
    /// Number of messages lost to retention (`low_watermark` - `committed_offset` when positive)
    pub messages_lost: i64,
    /// Offset distance to deletion boundary (`committed_offset` - `low_watermark`)
    pub retention_margin: i64,
    /// Percentage of retention window occupied by lag (0=caught up, 100=at boundary, >100=data loss)
    pub lag_retention_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct PartitionOffsetMetric {
    pub cluster_name: String,
    pub topic: String,
    pub partition: i32,
    pub earliest_offset: i64,
    pub latest_offset: i64,
}

pub struct LagCalculator;

/// Timestamp data for a partition
#[derive(Debug, Clone)]
pub struct TimestampData {
    pub timestamp_ms: i64,
}

impl LagCalculator {
    /// Calculate lag metrics for a single group against the given watermarks.
    #[allow(clippy::cast_precision_loss)]
    pub fn calculate_group(
        cluster_name: &str,
        group: &GroupSnapshot,
        watermarks: &HashMap<TopicPartition, (i64, i64)>,
        timestamps: &HashMap<(String, TopicPartition), TimestampData>,
        now_ms: i64,
        compacted_topics: &HashSet<String>,
    ) -> (Vec<PartitionLagMetric>, u64) {
        let mut partition_metrics = Vec::new();
        let mut skipped_partitions: u64 = 0;

        let member_map = build_member_map(group);

        for (tp, committed_offset) in &group.offsets {
            let Some(&(low_watermark, high_watermark)) = watermarks.get(tp) else {
                // No watermark available — skip rather than silently reporting lag=0
                warn!(
                    group = %group.group_id,
                    topic = %tp.topic,
                    partition = tp.partition,
                    "Missing watermark for committed partition, skipping"
                );
                skipped_partitions += 1;
                continue;
            };

            let lag = (high_watermark - committed_offset).max(0);

            let data_loss_detected = *committed_offset < low_watermark;
            let messages_lost = (low_watermark - *committed_offset).max(0);
            let retention_margin = *committed_offset - low_watermark;

            let retention_window = high_watermark - low_watermark;
            let lag_retention_ratio = if retention_window > 0 {
                let current_lag = high_watermark - *committed_offset;
                (current_lag as f64 / retention_window as f64) * 100.0
            } else {
                0.0
            };

            let (member_host, consumer_id, client_id) =
                member_map
                    .get(tp)
                    .map_or((String::new(), String::new(), String::new()), |m| {
                        (
                            m.client_host.to_string(),
                            m.member_id.to_string(),
                            m.client_id.to_string(),
                        )
                    });

            let ts_data = timestamps.get(&(group.group_id.clone(), tp.clone()));
            let lag_seconds = if lag > 0 {
                ts_data
                    .map(|td| ((now_ms - td.timestamp_ms) as f64) / 1000.0)
                    .map(|s| s.max(0.0))
            } else {
                Some(0.0)
            };
            let compaction_detected = compacted_topics.contains(&tp.topic);

            partition_metrics.push(PartitionLagMetric {
                cluster_name: cluster_name.to_string(),
                group_id: group.group_id.clone(),
                topic: tp.topic.clone(),
                partition: tp.partition,
                member_host,
                consumer_id,
                client_id,
                committed_offset: *committed_offset,
                lag,
                lag_seconds,
                compaction_detected,
                data_loss_detected,
                messages_lost,
                retention_margin,
                lag_retention_ratio,
            });
        }

        (partition_metrics, skipped_partitions)
    }

    /// Calculate lag metrics for the full snapshot. Retained for test compatibility.
    #[allow(dead_code)]
    pub fn calculate(
        snapshot: &OffsetsSnapshot,
        timestamps: &HashMap<(String, TopicPartition), TimestampData>,
        now_ms: i64,
        poll_time_ms: u64,
        compacted_topics: &HashSet<String>,
    ) -> LagMetrics {
        let mut partition_metrics = Vec::new();
        let mut total_skipped: u64 = 0;

        // Partition offset metrics (independent of groups)
        let partition_offsets: Vec<PartitionOffsetMetric> = snapshot
            .watermarks
            .iter()
            .map(|(tp, (low, high))| PartitionOffsetMetric {
                cluster_name: snapshot.cluster_name.clone(),
                topic: tp.topic.clone(),
                partition: tp.partition,
                earliest_offset: *low,
                latest_offset: *high,
            })
            .collect();

        // Process each consumer group using the per-group method
        for group in &snapshot.groups {
            let (p_metrics, skipped) = Self::calculate_group(
                &snapshot.cluster_name,
                group,
                &snapshot.watermarks,
                timestamps,
                now_ms,
                compacted_topics,
            );
            partition_metrics.extend(p_metrics);
            total_skipped += skipped;
        }

        // Count compaction and data loss detections from partition metrics
        let compaction_detected_count = partition_metrics
            .iter()
            .filter(|m| m.compaction_detected)
            .count() as u64;
        let data_loss_partition_count = partition_metrics
            .iter()
            .filter(|m| m.data_loss_detected)
            .count() as u64;
        let skipped_partition_count = total_skipped;

        LagMetrics {
            partition_metrics,
            partition_offsets,
            poll_time_ms,
            compaction_detected_count,
            data_loss_partition_count,
            skipped_partition_count,
        }
    }
}

struct MemberRef<'a> {
    member_id: &'a str,
    client_id: &'a str,
    client_host: &'a str,
}

fn build_member_map(group: &GroupSnapshot) -> HashMap<TopicPartition, MemberRef<'_>> {
    let mut map = HashMap::new();

    for member in &group.members {
        for assignment in &member.assignments {
            map.insert(
                assignment.clone(),
                MemberRef {
                    member_id: &member.member_id,
                    client_id: &member.client_id,
                    client_host: &member.client_host,
                },
            );
        }
    }

    map
}

#[allow(dead_code)] // Used by test infrastructure via update_with_options
impl LagMetrics {
    pub fn iter_partition_metrics(&self) -> impl Iterator<Item = &PartitionLagMetric> {
        self.partition_metrics.iter()
    }

    pub fn iter_partition_offsets(&self) -> impl Iterator<Item = &PartitionOffsetMetric> {
        self.partition_offsets.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::offset_collector::MemberSnapshot;

    fn make_snapshot() -> OffsetsSnapshot {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));
        watermarks.insert(TopicPartition::new("topic1", 1), (0, 200));
        watermarks.insert(TopicPartition::new("topic2", 0), (0, 50));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150);
        offsets.insert(TopicPartition::new("topic2", 0), 50);

        OffsetsSnapshot {
            cluster_name: "test-cluster".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "test-group".to_string(),
                members: vec![MemberSnapshot {
                    member_id: "member-1".to_string(),
                    client_id: "client-1".to_string(),
                    client_host: "host-1".to_string(),
                    assignments: vec![
                        TopicPartition::new("topic1", 0),
                        TopicPartition::new("topic1", 1),
                    ],
                }],
                offsets,
            }],
            watermarks,
        }
    }

    #[test]
    fn test_lag_calculator_offset_lag() {
        let snapshot = make_snapshot();
        let timestamps = HashMap::new();
        let now_ms = 1000000;

        let metrics =
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new());

        // topic1 partition 0: 100 - 90 = 10
        // topic1 partition 1: 200 - 150 = 50
        // topic2 partition 0: 50 - 50 = 0
        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();
        assert_eq!(p0.lag, 10);

        let p1 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 1)
            .unwrap();
        assert_eq!(p1.lag, 50);

        let p2 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic2" && m.partition == 0)
            .unwrap();
        assert_eq!(p2.lag, 0);
    }

    #[test]
    fn test_lag_calculator_time_lag() {
        let snapshot = make_snapshot();
        let mut timestamps = HashMap::new();
        // Message at offset 90 was produced at time 900000 (100 seconds ago)
        timestamps.insert(
            ("test-group".to_string(), TopicPartition::new("topic1", 0)),
            TimestampData {
                timestamp_ms: 900000,
            },
        );

        let now_ms = 1000000;
        let metrics =
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new());

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();

        assert_eq!(p0.lag_seconds, Some(100.0));
        assert!(!p0.compaction_detected);
    }

    #[test]
    fn test_lag_calculator_handles_negative_lag() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));

        let mut offsets = HashMap::new();
        // Committed offset > high watermark (race condition)
        offsets.insert(TopicPartition::new("topic1", 0), 110);

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "test-group".to_string(),
                members: vec![],
                offsets,
            }],
            watermarks,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.partition == 0)
            .unwrap();

        // Lag should be clamped to 0
        assert_eq!(p0.lag, 0);
    }

    #[test]
    fn test_partition_offset_metrics() {
        let snapshot = make_snapshot();
        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        assert_eq!(metrics.partition_offsets.len(), 3);

        let topic1_p0 = metrics
            .partition_offsets
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();

        assert_eq!(topic1_p0.earliest_offset, 0);
        assert_eq!(topic1_p0.latest_offset, 100);
    }

    /// Regression: Bug 1 — missing watermark should skip partition and count it,
    /// not silently report lag=0.
    #[test]
    fn test_missing_watermark_skips_partition() {
        let mut watermarks = HashMap::new();
        // Only provide watermark for topic1/0, NOT topic1/1
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150); // no watermark for this

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "g1".to_string(),
                members: vec![],
                offsets,
            }],
            watermarks,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        // Only 1 partition metric (the one with watermark)
        assert_eq!(metrics.partition_metrics.len(), 1);
        assert_eq!(metrics.partition_metrics[0].topic, "topic1");
        assert_eq!(metrics.partition_metrics[0].partition, 0);
        // Skipped partition count should be 1
        assert_eq!(metrics.skipped_partition_count, 1);
    }

    /// Regression: Bug 2 — lag_seconds should be None when lag>0 but no timestamp,
    /// not Some(0.0).
    #[test]
    fn test_lag_seconds_none_when_no_timestamp() {
        let snapshot = make_snapshot();
        // No timestamps provided at all
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 1_000_000, 100, &HashSet::new());

        // topic1/0 has lag=10, but no timestamp data
        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();
        assert_eq!(p0.lag, 10);
        assert_eq!(p0.lag_seconds, None);
    }

    /// Regression: Bug 2 — lag_seconds should be Some(0.0) when lag is zero
    /// (caught up, regardless of timestamp availability).
    #[test]
    fn test_lag_seconds_zero_when_caught_up() {
        let snapshot = make_snapshot();
        // topic2/0 has committed=50, high=50, so lag=0
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 1_000_000, 100, &HashSet::new());

        let p2 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic2" && m.partition == 0)
            .unwrap();
        assert_eq!(p2.lag, 0);
        assert_eq!(p2.lag_seconds, Some(0.0));
    }

    /// Regression: data loss detection when committed_offset < low_watermark.
    #[test]
    fn test_data_loss_detection() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (50, 200));

        let mut offsets = HashMap::new();
        // committed_offset=30 < low_watermark=50 → data loss
        offsets.insert(TopicPartition::new("topic1", 0), 30);

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "g1".to_string(),
                members: vec![],
                offsets,
            }],
            watermarks,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());
        let m = &metrics.partition_metrics[0];

        assert!(m.data_loss_detected);
        assert_eq!(m.messages_lost, 20); // 50 - 30
        assert_eq!(m.retention_margin, -20); // 30 - 50
        assert_eq!(m.lag, 170); // 200 - 30
        assert_eq!(metrics.data_loss_partition_count, 1);
    }

    /// Verify lag_retention_ratio calculation.
    #[test]
    fn test_lag_retention_ratio() {
        let mut watermarks = HashMap::new();
        // retention_window = 200 - 0 = 200
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 200));

        let mut offsets = HashMap::new();
        // lag = 200 - 100 = 100, ratio = 100/200 * 100 = 50%
        offsets.insert(TopicPartition::new("topic1", 0), 100);

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "g1".to_string(),
                members: vec![],
                offsets,
            }],
            watermarks,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());
        let m = &metrics.partition_metrics[0];

        assert!((m.lag_retention_ratio - 50.0).abs() < f64::EPSILON);
    }

    /// Verify compaction_detected flag is set for topics in the compacted set.
    #[test]
    fn test_compaction_detected_flag() {
        let snapshot = make_snapshot();
        let mut compacted = HashSet::new();
        compacted.insert("topic1".to_string());

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &compacted);

        for m in &metrics.partition_metrics {
            if m.topic == "topic1" {
                assert!(m.compaction_detected, "topic1 should be marked compacted");
            } else {
                assert!(
                    !m.compaction_detected,
                    "topic2 should not be marked compacted"
                );
            }
        }
        assert_eq!(metrics.compaction_detected_count, 2); // 2 topic1 partitions
    }

    /// Regression: unassigned partitions (offsets exist, no member) should still
    /// produce lag metrics with empty member fields.
    #[test]
    fn test_unassigned_partition_still_reports_lag() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));
        watermarks.insert(TopicPartition::new("topic1", 1), (0, 200));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150);

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "g1".to_string(),
                members: vec![
                    // Only partition 0 is assigned
                    MemberSnapshot {
                        member_id: "m1".to_string(),
                        client_id: "c1".to_string(),
                        client_host: "host1".to_string(),
                        assignments: vec![TopicPartition::new("topic1", 0)],
                    },
                ],
                offsets,
            }],
            watermarks,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        // Both partitions should produce metrics
        assert_eq!(metrics.partition_metrics.len(), 2);

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.partition == 0)
            .unwrap();
        assert_eq!(p0.lag, 10);
        assert_eq!(p0.member_host, "host1");

        // Partition 1 is unassigned — should still report lag, with empty member info
        let p1 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.partition == 1)
            .unwrap();
        assert_eq!(p1.lag, 50);
        assert!(p1.member_host.is_empty());
        assert!(p1.consumer_id.is_empty());
    }
}
