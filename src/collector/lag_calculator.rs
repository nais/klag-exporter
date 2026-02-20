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
    use crate::test_strategies::strategies::*;
    use proptest::prelude::*;

    fn make_group(
        group_id: &str,
        watermarks: HashMap<TopicPartition, (i64, i64)>,
        offsets: HashMap<TopicPartition, i64>,
    ) -> (GroupSnapshot, HashMap<TopicPartition, (i64, i64)>) {
        let group = GroupSnapshot {
            group_id: group_id.to_string(),
            members: vec![],
            offsets,
        };
        (group, watermarks)
    }

    proptest! {
        /// For any (low, high, committed), lag == max(0, high - committed)
        #[test]
        fn prop_lag_is_non_negative(
            (low, high, committed) in arb_watermarks_and_committed(),
            cluster in arb_cluster_name(),
            group_id in arb_group_id(),
        ) {
            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (low, high));
            let mut offsets = HashMap::new();
            offsets.insert(tp, committed);

            let (group, wm) = make_group(&group_id, watermarks, offsets);
            let (metrics, _skipped) = LagCalculator::calculate_group(
                &cluster, &group, &wm, &HashMap::new(), 0, &HashSet::new(),
            );

            prop_assert_eq!(metrics.len(), 1);
            prop_assert!(metrics[0].lag >= 0, "lag must be non-negative, got {}", metrics[0].lag);
            prop_assert_eq!(metrics[0].lag, (high - committed).max(0));
        }

        /// `data_loss_detected` iff `committed < low_watermark`
        #[test]
        fn prop_data_loss_iff_committed_below_low(
            (low, high, committed) in arb_watermarks_and_committed(),
        ) {
            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (low, high));
            let mut offsets = HashMap::new();
            offsets.insert(tp, committed);

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &HashMap::new(), 0, &HashSet::new(),
            );

            let m = &metrics[0];
            prop_assert_eq!(m.data_loss_detected, committed < low);
            prop_assert_eq!(m.messages_lost, (low - committed).max(0));
            prop_assert_eq!(m.retention_margin, committed - low);
        }

        /// `lag_retention_ratio` == `(high - committed) / (high - low) * 100`
        #[test]
        fn prop_lag_retention_ratio(
            (low, high, committed) in arb_watermarks_and_committed(),
        ) {
            prop_assume!(high > low);

            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (low, high));
            let mut offsets = HashMap::new();
            offsets.insert(tp, committed);

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &HashMap::new(), 0, &HashSet::new(),
            );

            let expected = ((high - committed) as f64 / (high - low) as f64) * 100.0;
            prop_assert!((metrics[0].lag_retention_ratio - expected).abs() < 1e-10);
        }

        /// When `committed == high`, `lag_seconds` is `Some(0.0)` regardless of timestamps
        #[test]
        fn prop_lag_seconds_zero_when_caught_up(
            low in 0..i64::MAX / 2,
            high in 0..i64::MAX / 2,
            now_ms in 0..i64::MAX / 2,
        ) {
            prop_assume!(high >= low);
            let committed = high;

            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (low, high));
            let mut offsets = HashMap::new();
            offsets.insert(tp, committed);

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &HashMap::new(), now_ms, &HashSet::new(),
            );

            prop_assert_eq!(metrics[0].lag, 0);
            prop_assert_eq!(metrics[0].lag_seconds, Some(0.0));
        }

        /// When `committed < high` and no timestamp, `lag_seconds` is `None`
        #[test]
        fn prop_lag_seconds_none_without_timestamp(
            (low, high, committed) in arb_watermarks_and_committed(),
        ) {
            prop_assume!(committed < high);

            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (low, high));
            let mut offsets = HashMap::new();
            offsets.insert(tp, committed);

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &HashMap::new(), 1_000_000, &HashSet::new(),
            );

            prop_assert!(metrics[0].lag > 0);
            prop_assert_eq!(metrics[0].lag_seconds, None);
        }

        /// `compaction_detected` matches set membership
        #[test]
        fn prop_compaction_flag_matches_set(
            topic in "[a-z][a-z0-9]{0,20}",
            in_set in proptest::bool::ANY,
        ) {
            let tp = TopicPartition::new(&topic, 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (0, 100));
            let mut offsets = HashMap::new();
            offsets.insert(tp, 50);

            let mut compacted = HashSet::new();
            if in_set {
                compacted.insert(topic.clone());
            }

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &HashMap::new(), 0, &compacted,
            );

            prop_assert_eq!(metrics[0].compaction_detected, in_set);
        }

        /// `lag_seconds` == `(now_ms - timestamp_ms) / 1000` when lag > 0 and timestamp present
        #[test]
        fn prop_lag_seconds_from_timestamp(
            now_ms in 1_000i64..i64::MAX / 2,
            ts_offset in 0i64..1_000_000,
        ) {
            let timestamp_ms = now_ms - ts_offset;
            let tp = TopicPartition::new("t", 0);
            let mut watermarks = HashMap::new();
            watermarks.insert(tp.clone(), (0, 100));
            let mut offsets = HashMap::new();
            offsets.insert(tp.clone(), 50);

            let mut timestamps = HashMap::new();
            timestamps.insert(
                ("g".to_string(), tp),
                TimestampData { timestamp_ms },
            );

            let (group, wm) = make_group("g", watermarks, offsets);
            let (metrics, _) = LagCalculator::calculate_group(
                "c", &group, &wm, &timestamps, now_ms, &HashSet::new(),
            );

            let expected = (now_ms - timestamp_ms) as f64 / 1000.0;
            prop_assert_eq!(metrics[0].lag_seconds, Some(expected));
        }
    }

    // ---- Regression tests ----

    /// Regression: missing watermark should skip partition, not report lag=0.
    #[test]
    fn test_missing_watermark_skips_partition() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150);

        let (group, wm) = make_group("g1", watermarks, offsets);
        let (metrics, skipped) = LagCalculator::calculate_group(
            "test",
            &group,
            &wm,
            &HashMap::new(),
            0,
            &HashSet::new(),
        );

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].topic, "topic1");
        assert_eq!(metrics[0].partition, 0);
        assert_eq!(skipped, 1);
    }

    /// Regression: unassigned partitions should still produce lag metrics with empty member fields.
    #[test]
    fn test_unassigned_partition_still_reports_lag() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));
        watermarks.insert(TopicPartition::new("topic1", 1), (0, 200));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150);

        let group = GroupSnapshot {
            group_id: "g1".to_string(),
            members: vec![MemberSnapshot {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "host1".to_string(),
                assignments: vec![TopicPartition::new("topic1", 0)],
            }],
            offsets,
        };

        let (metrics, _) = LagCalculator::calculate_group(
            "test",
            &group,
            &watermarks,
            &HashMap::new(),
            0,
            &HashSet::new(),
        );

        assert_eq!(metrics.len(), 2);

        let p0 = metrics.iter().find(|m| m.partition == 0).unwrap();
        assert_eq!(p0.lag, 10);
        assert_eq!(p0.member_host, "host1");

        let p1 = metrics.iter().find(|m| m.partition == 1).unwrap();
        assert_eq!(p1.lag, 50);
        assert!(p1.member_host.is_empty());
        assert!(p1.consumer_id.is_empty());
    }
}
