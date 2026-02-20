use crate::collector::lag_calculator::{LagMetrics, PartitionLagMetric, PartitionOffsetMetric};
use crate::config::Granularity;
use crate::metrics::definitions::{
    HELP_COMPACTION_DETECTED, HELP_DATA_LOSS_PARTITIONS, HELP_GROUP_LAG, HELP_GROUP_LAG_SECONDS,
    HELP_GROUP_OFFSET, HELP_LAG_RETENTION_RATIO, HELP_LAST_UPDATE_TIMESTAMP, HELP_MESSAGES_LOST,
    HELP_PARTITION_EARLIEST_OFFSET, HELP_PARTITION_LATEST_OFFSET, HELP_POLL_TIME_MS,
    HELP_RETENTION_MARGIN, HELP_SCRAPE_DURATION_SECONDS, HELP_SKIPPED_PARTITIONS, HELP_UP,
    LABEL_CLIENT_ID, LABEL_CLUSTER_NAME, LABEL_COMPACTION_DETECTED, LABEL_CONSUMER_ID,
    LABEL_DATA_LOSS_DETECTED, LABEL_GROUP, LABEL_MEMBER_HOST, LABEL_PARTITION, LABEL_TOPIC,
    METRIC_COMPACTION_DETECTED, METRIC_DATA_LOSS_PARTITIONS, METRIC_GROUP_LAG,
    METRIC_GROUP_LAG_SECONDS, METRIC_GROUP_OFFSET, METRIC_LAG_RETENTION_RATIO,
    METRIC_LAST_UPDATE_TIMESTAMP, METRIC_MESSAGES_LOST, METRIC_PARTITION_EARLIEST_OFFSET,
    METRIC_PARTITION_LATEST_OFFSET, METRIC_POLL_TIME_MS, METRIC_RETENTION_MARGIN,
    METRIC_SCRAPE_DURATION_SECONDS, METRIC_SKIPPED_PARTITIONS, METRIC_UP,
};
use crate::metrics::types::{Labels, MetricPoint, OtelDataPoint, OtelMetric};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default staleness threshold: 3x the typical poll interval
const DEFAULT_STALENESS_THRESHOLD: Duration = Duration::from_secs(90);

pub struct MetricsRegistry {
    metrics: DashMap<String, Vec<MetricPoint>>,
    last_update: DashMap<String, Instant>,
    last_update_timestamp: DashMap<String, u64>, // Unix timestamp in seconds
    healthy: AtomicBool,
    last_scrape_duration_ms: AtomicU64,
    staleness_threshold: Duration,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::with_staleness_threshold(DEFAULT_STALENESS_THRESHOLD)
    }

    pub fn with_staleness_threshold(staleness_threshold: Duration) -> Self {
        Self {
            metrics: DashMap::new(),
            last_update: DashMap::new(),
            last_update_timestamp: DashMap::new(),
            healthy: AtomicBool::new(true),
            last_scrape_duration_ms: AtomicU64::new(0),
            staleness_threshold,
        }
    }

    /// Update metrics with granularity control and custom labels
    #[allow(clippy::cast_precision_loss, dead_code)] // Used by tests
    pub fn update_with_options(
        &self,
        cluster: &str,
        lag_metrics: &LagMetrics,
        granularity: Granularity,
        custom_labels: &HashMap<String, String>,
    ) {
        let mut points = Vec::new();

        points.extend(build_partition_offset_points(
            lag_metrics.iter_partition_offsets(),
            custom_labels,
        ));

        points.extend(build_group_metric_points(
            lag_metrics.iter_partition_metrics(),
            granularity,
            lag_metrics.skipped_partition_count,
            custom_labels,
        ));

        points.extend(build_cluster_summary_points(
            cluster,
            lag_metrics.poll_time_ms,
            lag_metrics.compaction_detected_count,
            lag_metrics.data_loss_partition_count,
            lag_metrics.skipped_partition_count,
            custom_labels,
        ));

        // Store metrics and update timestamps
        let now = Instant::now();
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();

        self.metrics.insert(cluster.to_string(), points);
        self.last_update.insert(cluster.to_string(), now);
        self.last_update_timestamp
            .insert(cluster.to_string(), unix_timestamp);
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::SeqCst);
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn set_scrape_duration_ms(&self, duration_ms: u64) {
        self.last_scrape_duration_ms
            .store(duration_ms, Ordering::SeqCst);
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn get_scrape_duration_seconds(&self) -> f64 {
        self.last_scrape_duration_ms.load(Ordering::SeqCst) as f64 / 1000.0
    }

    pub fn render_prometheus(&self) -> String {
        self.render_prometheus_with_staleness_check(true)
    }

    /// Render Prometheus metrics, optionally filtering out stale clusters
    pub fn render_prometheus_with_staleness_check(&self, filter_stale: bool) -> String {
        let mut output = String::new();
        let mut seen_metrics: HashSet<String> = HashSet::new();
        let now = Instant::now();

        // Collect DashMap guards to keep them alive while we borrow their contents
        let guards: Vec<_> = self
            .metrics
            .iter()
            .filter(|entry| {
                if !filter_stale {
                    return true;
                }
                self.last_update
                    .get(entry.key())
                    .is_some_and(|last_update| {
                        now.duration_since(*last_update) <= self.staleness_threshold
                    })
            })
            .collect();

        // Group points by metric name, borrowing from the guards (no cloning)
        let mut by_name: HashMap<&str, Vec<&MetricPoint>> = HashMap::new();
        for guard in &guards {
            for point in guard.value() {
                by_name.entry(point.name.as_str()).or_default().push(point);
            }
        }

        // Sort metric names for consistent output
        let mut names: Vec<_> = by_name.keys().copied().collect();
        names.sort_unstable();

        for name in names {
            let points = &by_name[name];
            if points.is_empty() {
                continue;
            }

            // Output HELP and TYPE once per metric
            if seen_metrics.insert(name.to_string()) {
                let first = points[0];
                output.push_str(format!("# HELP {name} {}\n", first.help).as_str());
                output.push_str(format!("# TYPE {name} {}\n", first.metric_type.as_str()).as_str());
            }

            // Output all data points
            for point in points {
                let labels_str = render_labels(&point.labels);
                output.push_str(
                    format!("{}{labels_str} {}\n", point.name, point.value.as_f64()).as_str(),
                );
            }
        }

        // Add scrape duration metric
        let scrape_duration = self.get_scrape_duration_seconds();
        output.push_str(
            format!("# HELP {METRIC_SCRAPE_DURATION_SECONDS} {HELP_SCRAPE_DURATION_SECONDS}\n")
                .as_str(),
        );
        output.push_str(format!("# TYPE {METRIC_SCRAPE_DURATION_SECONDS} gauge\n").as_str());
        output
            .push_str(format!("{METRIC_SCRAPE_DURATION_SECONDS} {scrape_duration:.6}\n").as_str());

        // Add exporter health metric
        output.push_str(format!("# HELP {METRIC_UP} {HELP_UP}\n").as_str());
        output.push_str(format!("# TYPE {METRIC_UP} gauge\n").as_str());
        output.push_str(format!("{} {}\n", METRIC_UP, i32::from(self.is_healthy())).as_str());

        // Add last update timestamp metric per cluster
        if !self.last_update_timestamp.is_empty() {
            output.push_str(
                format!("# HELP {METRIC_LAST_UPDATE_TIMESTAMP} {HELP_LAST_UPDATE_TIMESTAMP}\n",)
                    .as_str(),
            );
            output.push_str(format!("# TYPE {METRIC_LAST_UPDATE_TIMESTAMP} gauge\n").as_str());
            for entry in &self.last_update_timestamp {
                let cluster = entry.key();
                let timestamp = entry.value();
                // Only include non-stale clusters if filtering is enabled
                if filter_stale
                    && let Some(last_update) = self.last_update.get(cluster)
                    && now.duration_since(*last_update) > self.staleness_threshold
                {
                    continue;
                }
                output.push_str(format!(
                    "{METRIC_LAST_UPDATE_TIMESTAMP}{{{LABEL_CLUSTER_NAME}=\"{cluster}\"}} {timestamp}\n",
                ).as_str());
            }
        }

        output
    }

    pub fn get_otel_metrics(&self) -> Vec<OtelMetric> {
        let mut otel_metrics: HashMap<String, OtelMetric> = HashMap::new();
        let now = Instant::now();

        for entry in &self.metrics {
            // Skip stale clusters (same logic as render_prometheus)
            let is_fresh = self
                .last_update
                .get(entry.key())
                .is_some_and(|last_update| {
                    now.duration_since(*last_update) <= self.staleness_threshold
                });
            if !is_fresh {
                continue;
            }

            for point in entry.value() {
                let metric = otel_metrics
                    .entry(point.name.clone())
                    .or_insert_with(|| OtelMetric {
                        name: point.name.clone(),
                        data_points: Vec::new(),
                    });

                metric.data_points.push(OtelDataPoint {
                    attributes: point.labels.clone(),
                    value: point.value.as_f64(),
                });
            }
        }

        // Add scrape duration metric
        let scrape_duration = self.get_scrape_duration_seconds();
        otel_metrics.insert(
            METRIC_SCRAPE_DURATION_SECONDS.to_string(),
            OtelMetric {
                name: METRIC_SCRAPE_DURATION_SECONDS.to_string(),
                data_points: vec![OtelDataPoint {
                    attributes: HashMap::new(),
                    value: scrape_duration,
                }],
            },
        );

        // Add up metric
        otel_metrics.insert(
            METRIC_UP.to_string(),
            OtelMetric {
                name: METRIC_UP.to_string(),
                data_points: vec![OtelDataPoint {
                    attributes: HashMap::new(),
                    value: if self.is_healthy() { 1.0 } else { 0.0 },
                }],
            },
        );

        otel_metrics.into_values().collect()
    }

    /// Begin a new collection cycle for a cluster.
    /// Clears existing metric points so that `push_points` can append incrementally.
    pub fn begin_cycle(&self, cluster: &str) {
        self.metrics.insert(cluster.to_string(), Vec::new());
    }

    /// Append metric points for a cluster during a collection cycle.
    /// Call between `begin_cycle` and `finish_cycle`.
    pub fn push_points(&self, cluster: &str, points: Vec<MetricPoint>) {
        self.metrics
            .entry(cluster.to_string())
            .or_default()
            .extend(points);
    }

    /// Finish a collection cycle for a cluster.
    /// Records the update timestamps so staleness checks work correctly.
    pub fn finish_cycle(&self, cluster: &str) {
        let now = Instant::now();
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        self.last_update.insert(cluster.to_string(), now);
        self.last_update_timestamp
            .insert(cluster.to_string(), unix_timestamp);
    }

    pub fn remove_cluster(&self, cluster: &str) {
        self.metrics.remove(cluster);
        self.last_update.remove(cluster);
        self.last_update_timestamp.remove(cluster);
    }

    pub fn cluster_count(&self) -> usize {
        self.metrics.len()
    }
}

fn render_labels(labels: &Labels) -> String {
    if labels.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);

    let label_str = pairs
        .into_iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");

    format!("{{{label_str}}}")
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn add_custom_labels(labels: &mut Labels, custom_labels: &HashMap<String, String>) {
    for (k, v) in custom_labels {
        labels.insert(k.clone(), v.clone());
    }
}

/// Build metric points for partition earliest/latest offsets (watermarks).
#[allow(clippy::cast_precision_loss)]
pub fn build_partition_offset_points<'a>(
    partition_offsets: impl Iterator<Item = &'a PartitionOffsetMetric>,
    custom_labels: &HashMap<String, String>,
) -> Vec<MetricPoint> {
    let mut points = Vec::new();
    for m in partition_offsets {
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
        labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
        labels.insert(LABEL_PARTITION.to_string(), m.partition.to_string());
        add_custom_labels(&mut labels, custom_labels);

        points.push(MetricPoint::gauge(
            METRIC_PARTITION_LATEST_OFFSET,
            labels.clone(),
            m.latest_offset as f64,
            HELP_PARTITION_LATEST_OFFSET,
        ));

        points.push(MetricPoint::gauge(
            METRIC_PARTITION_EARLIEST_OFFSET,
            labels,
            m.earliest_offset as f64,
            HELP_PARTITION_EARLIEST_OFFSET,
        ));
    }
    points
}

/// Build metric points for a single consumer group's lag data.
/// Granularity controls which labels are included:
/// - `Partition`: full labels (`partition`, `member_host`, `consumer_id`, `client_id`)
/// - `Topic`: only `cluster_name`, group, topic (partition metrics are pre-aggregated per topic)
#[allow(clippy::cast_precision_loss)]
pub fn build_group_metric_points<'a>(
    partition_metrics: impl Iterator<Item = &'a PartitionLagMetric>,
    granularity: Granularity,
    skipped_partitions: u64,
    custom_labels: &HashMap<String, String>,
) -> Vec<MetricPoint> {
    let mut points = Vec::new();

    build_partition_level_points(partition_metrics, granularity, custom_labels, &mut points);

    // Emit skipped_partitions per-group (only when > 0)
    if skipped_partitions > 0
        && let Some(first) = points.first()
    {
        let mut labels = Labels::new();
        if let Some(cluster) = first.labels.get(LABEL_CLUSTER_NAME) {
            labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.clone());
        }
        if let Some(group) = first.labels.get(LABEL_GROUP) {
            labels.insert(LABEL_GROUP.to_string(), group.clone());
        }
        add_custom_labels(&mut labels, custom_labels);
        points.push(MetricPoint::gauge(
            METRIC_SKIPPED_PARTITIONS,
            labels,
            skipped_partitions as f64,
            HELP_SKIPPED_PARTITIONS,
        ));
    }

    points
}

#[allow(clippy::cast_precision_loss)]
fn build_partition_level_points<'a>(
    partition_metrics: impl Iterator<Item = &'a PartitionLagMetric>,
    granularity: Granularity,
    custom_labels: &HashMap<String, String>,
    points: &mut Vec<MetricPoint>,
) {
    if granularity == Granularity::Topic {
        build_topic_aggregated_points(partition_metrics, custom_labels, points);
    } else {
        build_per_partition_points(partition_metrics, custom_labels, points);
    }
}

/// Emit one set of metric points per partition (full label set).
#[allow(clippy::cast_precision_loss)]
fn build_per_partition_points<'a>(
    partition_metrics: impl Iterator<Item = &'a PartitionLagMetric>,
    custom_labels: &HashMap<String, String>,
    points: &mut Vec<MetricPoint>,
) {
    for m in partition_metrics {
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
        labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
        labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
        labels.insert(LABEL_PARTITION.to_string(), m.partition.to_string());
        labels.insert(LABEL_MEMBER_HOST.to_string(), m.member_host.clone());
        labels.insert(LABEL_CONSUMER_ID.to_string(), m.consumer_id.clone());
        labels.insert(LABEL_CLIENT_ID.to_string(), m.client_id.clone());
        add_custom_labels(&mut labels, custom_labels);

        emit_partition_metrics(m, &labels, custom_labels, points);
    }
}

/// Pre-aggregate partitions per `(cluster, group, topic)`, then emit one point per topic.
#[allow(clippy::cast_precision_loss)]
fn build_topic_aggregated_points<'a>(
    partition_metrics: impl Iterator<Item = &'a PartitionLagMetric>,
    custom_labels: &HashMap<String, String>,
    points: &mut Vec<MetricPoint>,
) {
    // Aggregate key: (cluster_name, group_id, topic)
    let mut aggregated: HashMap<(String, String, String), TopicAggregation> = HashMap::new();

    for m in partition_metrics {
        let key = (m.cluster_name.clone(), m.group_id.clone(), m.topic.clone());
        let agg = aggregated.entry(key).or_default();
        agg.committed_offset += m.committed_offset;
        agg.lag += m.lag;
        agg.lag_seconds = match (agg.lag_seconds, m.lag_seconds) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        agg.compaction_detected |= m.compaction_detected;
        agg.data_loss_detected |= m.data_loss_detected;
        agg.messages_lost += m.messages_lost;
        agg.retention_margin = agg.retention_margin.min(m.retention_margin);
        agg.lag_retention_ratio = agg.lag_retention_ratio.max(m.lag_retention_ratio);
    }

    for ((cluster, group, topic), agg) in &aggregated {
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.clone());
        labels.insert(LABEL_GROUP.to_string(), group.clone());
        labels.insert(LABEL_TOPIC.to_string(), topic.clone());
        add_custom_labels(&mut labels, custom_labels);

        points.push(MetricPoint::gauge(
            METRIC_GROUP_OFFSET,
            labels.clone(),
            agg.committed_offset as f64,
            HELP_GROUP_OFFSET,
        ));

        points.push(MetricPoint::gauge(
            METRIC_GROUP_LAG,
            labels.clone(),
            agg.lag as f64,
            HELP_GROUP_LAG,
        ));

        if let Some(lag_seconds) = agg.lag_seconds {
            labels.insert(
                LABEL_COMPACTION_DETECTED.to_string(),
                agg.compaction_detected.to_string(),
            );
            labels.insert(
                LABEL_DATA_LOSS_DETECTED.to_string(),
                agg.data_loss_detected.to_string(),
            );
            points.push(MetricPoint::gauge(
                METRIC_GROUP_LAG_SECONDS,
                labels.clone(),
                lag_seconds,
                HELP_GROUP_LAG_SECONDS,
            ));
        }

        let mut data_loss_labels = Labels::new();
        data_loss_labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.clone());
        data_loss_labels.insert(LABEL_GROUP.to_string(), group.clone());
        data_loss_labels.insert(LABEL_TOPIC.to_string(), topic.clone());
        add_custom_labels(&mut data_loss_labels, custom_labels);

        points.push(MetricPoint::gauge(
            METRIC_MESSAGES_LOST,
            data_loss_labels.clone(),
            agg.messages_lost as f64,
            HELP_MESSAGES_LOST,
        ));

        points.push(MetricPoint::gauge(
            METRIC_RETENTION_MARGIN,
            data_loss_labels.clone(),
            agg.retention_margin as f64,
            HELP_RETENTION_MARGIN,
        ));

        points.push(MetricPoint::gauge(
            METRIC_LAG_RETENTION_RATIO,
            data_loss_labels,
            agg.lag_retention_ratio,
            HELP_LAG_RETENTION_RATIO,
        ));
    }
}

/// Local aggregation state for Topic granularity — not exported.
struct TopicAggregation {
    committed_offset: i64,
    lag: i64,
    lag_seconds: Option<f64>,
    compaction_detected: bool,
    data_loss_detected: bool,
    messages_lost: i64,
    retention_margin: i64,
    lag_retention_ratio: f64,
}

impl Default for TopicAggregation {
    fn default() -> Self {
        Self {
            committed_offset: 0,
            lag: 0,
            lag_seconds: None,
            compaction_detected: false,
            data_loss_detected: false,
            messages_lost: 0,
            retention_margin: i64::MAX,
            lag_retention_ratio: 0.0,
        }
    }
}

/// Shared helper: emit metric points for a single partition's data.
#[allow(clippy::cast_precision_loss)]
fn emit_partition_metrics(
    m: &PartitionLagMetric,
    labels: &Labels,
    custom_labels: &HashMap<String, String>,
    points: &mut Vec<MetricPoint>,
) {
    points.push(MetricPoint::gauge(
        METRIC_GROUP_OFFSET,
        labels.clone(),
        m.committed_offset as f64,
        HELP_GROUP_OFFSET,
    ));

    points.push(MetricPoint::gauge(
        METRIC_GROUP_LAG,
        labels.clone(),
        m.lag as f64,
        HELP_GROUP_LAG,
    ));

    if let Some(lag_seconds) = m.lag_seconds {
        let mut lag_labels = labels.clone();
        lag_labels.insert(
            LABEL_COMPACTION_DETECTED.to_string(),
            m.compaction_detected.to_string(),
        );
        lag_labels.insert(
            LABEL_DATA_LOSS_DETECTED.to_string(),
            m.data_loss_detected.to_string(),
        );
        points.push(MetricPoint::gauge(
            METRIC_GROUP_LAG_SECONDS,
            lag_labels,
            lag_seconds,
            HELP_GROUP_LAG_SECONDS,
        ));
    }

    let mut data_loss_labels = Labels::new();
    data_loss_labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
    data_loss_labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
    data_loss_labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
    data_loss_labels.insert(LABEL_PARTITION.to_string(), m.partition.to_string());
    add_custom_labels(&mut data_loss_labels, custom_labels);

    points.push(MetricPoint::gauge(
        METRIC_MESSAGES_LOST,
        data_loss_labels.clone(),
        m.messages_lost as f64,
        HELP_MESSAGES_LOST,
    ));

    points.push(MetricPoint::gauge(
        METRIC_RETENTION_MARGIN,
        data_loss_labels.clone(),
        m.retention_margin as f64,
        HELP_RETENTION_MARGIN,
    ));

    points.push(MetricPoint::gauge(
        METRIC_LAG_RETENTION_RATIO,
        data_loss_labels,
        m.lag_retention_ratio,
        HELP_LAG_RETENTION_RATIO,
    ));
}

/// Build cluster-level summary metric points (poll time, compaction count, data loss count).
#[allow(clippy::cast_precision_loss)]
pub fn build_cluster_summary_points(
    cluster: &str,
    poll_time_ms: u64,
    compaction_detected_count: u64,
    data_loss_partition_count: u64,
    skipped_partition_count: u64,
    custom_labels: &HashMap<String, String>,
) -> Vec<MetricPoint> {
    let mut poll_labels = Labels::new();
    poll_labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.to_string());
    add_custom_labels(&mut poll_labels, custom_labels);

    let mut points = vec![
        MetricPoint::gauge(
            METRIC_POLL_TIME_MS,
            poll_labels.clone(),
            poll_time_ms as f64,
            HELP_POLL_TIME_MS,
        ),
        MetricPoint::gauge(
            METRIC_COMPACTION_DETECTED,
            poll_labels.clone(),
            compaction_detected_count as f64,
            HELP_COMPACTION_DETECTED,
        ),
        MetricPoint::gauge(
            METRIC_DATA_LOSS_PARTITIONS,
            poll_labels.clone(),
            data_loss_partition_count as f64,
            HELP_DATA_LOSS_PARTITIONS,
        ),
    ];

    if skipped_partition_count > 0 {
        points.push(MetricPoint::gauge(
            METRIC_SKIPPED_PARTITIONS,
            poll_labels,
            skipped_partition_count as f64,
            HELP_SKIPPED_PARTITIONS,
        ));
    }

    points
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lag_metrics() -> LagMetrics {
        LagMetrics {
            partition_metrics: vec![PartitionLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                member_host: "host1".to_string(),
                consumer_id: "consumer-1".to_string(),
                client_id: "client-1".to_string(),
                committed_offset: 100,
                lag: 10,
                lag_seconds: Some(5.5),
                compaction_detected: false,
                data_loss_detected: false,
                messages_lost: 0,
                retention_margin: 100,
                lag_retention_ratio: 9.09,
            }],
            partition_offsets: vec![PartitionOffsetMetric {
                cluster_name: "test-cluster".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                earliest_offset: 0,
                latest_offset: 110,
            }],
            poll_time_ms: 50,
            compaction_detected_count: 0,
            data_loss_partition_count: 0,
            skipped_partition_count: 0,
        }
    }

    #[test]
    fn test_metrics_registry_update_replaces() {
        let registry = MetricsRegistry::new();

        let metrics1 = make_lag_metrics();
        registry.update_with_options(
            "test-cluster",
            &metrics1,
            Granularity::Partition,
            &HashMap::new(),
        );

        let output1 = registry.render_prometheus();
        assert!(output1.contains("kafka_consumergroup_group_lag"));

        // Update with new metrics
        let mut metrics2 = make_lag_metrics();
        metrics2.partition_metrics[0].lag = 20;
        registry.update_with_options(
            "test-cluster",
            &metrics2,
            Granularity::Partition,
            &HashMap::new(),
        );

        let output2 = registry.render_prometheus();
        assert!(output2.contains("20")); // New lag value
    }

    #[test]
    fn test_prometheus_format_gauge() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();
        registry.update_with_options(
            "test-cluster",
            &metrics,
            Granularity::Partition,
            &HashMap::new(),
        );

        let output = registry.render_prometheus();

        assert!(output.contains("# TYPE kafka_consumergroup_group_lag gauge"));
        assert!(output.contains("kafka_consumergroup_group_lag{"));
    }

    #[test]
    fn test_prometheus_format_labels() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();
        registry.update_with_options(
            "test-cluster",
            &metrics,
            Granularity::Partition,
            &HashMap::new(),
        );

        let output = registry.render_prometheus();

        assert!(output.contains("cluster_name=\"test-cluster\""));
        assert!(output.contains("group=\"test-group\""));
        assert!(output.contains("topic=\"test-topic\""));
    }

    #[test]
    fn test_metrics_registry_evicts_disappeared_groups() {
        let registry = MetricsRegistry::new();

        let metrics = make_lag_metrics();
        registry.update_with_options(
            "cluster1",
            &metrics,
            Granularity::Partition,
            &HashMap::new(),
        );

        assert_eq!(registry.cluster_count(), 1);

        registry.remove_cluster("cluster1");
        assert_eq!(registry.cluster_count(), 0);
    }

    #[test]
    fn test_up_metric() {
        let registry = MetricsRegistry::new();
        registry.set_healthy(true);

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_up 1"));

        registry.set_healthy(false);
        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_up 0"));
    }

    #[test]
    fn test_granularity_topic_omits_partition_labels() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();

        registry.update_with_options(
            "test-cluster",
            &metrics,
            Granularity::Topic,
            &HashMap::new(),
        );

        let output = registry.render_prometheus();

        // Should have lag metrics (same metric names regardless of granularity)
        assert!(output.contains("kafka_consumergroup_group_lag"));
        assert!(output.contains("kafka_consumergroup_group_offset"));

        // Consumer group metrics should NOT have partition-level labels
        for line in output.lines() {
            if line.starts_with("kafka_consumergroup_") {
                assert!(
                    !line.contains("partition="),
                    "unexpected partition= in: {line}"
                );
                assert!(
                    !line.contains("member_host="),
                    "unexpected member_host= in: {line}"
                );
                assert!(
                    !line.contains("consumer_id="),
                    "unexpected consumer_id= in: {line}"
                );
                assert!(
                    !line.contains("client_id="),
                    "unexpected client_id= in: {line}"
                );
            }
        }
    }

    #[test]
    fn test_incremental_begin_push_finish_cycle() {
        let registry = MetricsRegistry::new();

        registry.begin_cycle("test-cluster");

        // Push partition offset points
        let mut labels1 = Labels::new();
        labels1.insert(LABEL_CLUSTER_NAME.to_string(), "test-cluster".to_string());
        labels1.insert(LABEL_TOPIC.to_string(), "topic1".to_string());
        labels1.insert(LABEL_PARTITION.to_string(), "0".to_string());
        registry.push_points(
            "test-cluster",
            vec![MetricPoint::gauge(
                METRIC_PARTITION_LATEST_OFFSET,
                labels1,
                110.0,
                HELP_PARTITION_LATEST_OFFSET,
            )],
        );

        // Push group-level points in a second call
        let mut labels2 = Labels::new();
        labels2.insert(LABEL_CLUSTER_NAME.to_string(), "test-cluster".to_string());
        labels2.insert(LABEL_GROUP.to_string(), "group1".to_string());
        labels2.insert(LABEL_TOPIC.to_string(), "topic1".to_string());
        labels2.insert(LABEL_PARTITION.to_string(), "0".to_string());
        registry.push_points(
            "test-cluster",
            vec![MetricPoint::gauge(
                METRIC_GROUP_LAG,
                labels2,
                42.0,
                HELP_GROUP_LAG,
            )],
        );

        registry.finish_cycle("test-cluster");

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_partition_latest_offset"));
        assert!(output.contains("110"));
        assert!(output.contains("kafka_consumergroup_group_lag"));
        assert!(output.contains("42"));
        assert_eq!(registry.cluster_count(), 1);
    }

    #[test]
    fn test_cluster_summary_points_regression() {
        let points = build_cluster_summary_points("c1", 150, 3, 2, 0, &HashMap::new());

        assert_eq!(points.len(), 3);
        let names: Vec<&str> = points.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&METRIC_POLL_TIME_MS));
        assert!(names.contains(&METRIC_COMPACTION_DETECTED));
        assert!(names.contains(&METRIC_DATA_LOSS_PARTITIONS));
    }

    use proptest::prelude::*;

    /// Find the unique metric line matching `metric_prefix` and `label_filter`,
    /// assert exactly one match, and return its parsed value.
    fn find_metric_value(output: &str, metric_prefix: &str, label_filter: &str) -> f64 {
        let lines: Vec<&str> = output
            .lines()
            .filter(|l| l.starts_with(metric_prefix) && l.contains(label_filter))
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "Expected 1 line for {metric_prefix} with {label_filter}, got {}: {lines:?}",
            lines.len()
        );
        lines[0]
            .rsplit_once(' ')
            .expect("metric line should have a value")
            .1
            .parse()
            .expect("should parse as f64")
    }

    proptest! {
        /// Escaped output never contains bare `"`, `\`, or `\n`
        #[test]
        fn prop_escape_label_value_safe(input in ".*") {
            let escaped = escape_label_value(&input);
            let chars: Vec<char> = escaped.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '\\' {
                    prop_assert!(i + 1 < chars.len(), "trailing backslash");
                    prop_assert!(
                        matches!(chars[i + 1], '\\' | '"' | 'n'),
                        "invalid escape: \\{}", chars[i + 1]
                    );
                    i += 2;
                } else {
                    prop_assert_ne!(chars[i], '"', "unescaped quote at {}", i);
                    prop_assert_ne!(chars[i], '\n', "unescaped newline at {}", i);
                    i += 1;
                }
            }
        }

        /// `set_scrape_duration_ms(ms)` renders as `ms / 1000.0`
        #[test]
        fn prop_scrape_duration_conversion(ms in 0u64..10_000_000) {
            let registry = MetricsRegistry::new();
            registry.set_scrape_duration_ms(ms);
            let output = registry.render_prometheus();

            let expected = format!("{}", ms as f64 / 1000.0);
            prop_assert!(
                output.contains(&expected),
                "Expected {} in output for ms={}", expected, ms
            );
        }

        /// Custom labels with arbitrary keys/values appear in Prometheus output
        #[test]
        fn prop_custom_labels_present(
            key in "[a-z][a-z0-9_]{0,20}",
            value in "[a-zA-Z0-9_-]{1,30}",
        ) {
            let registry = MetricsRegistry::new();
            let metrics = make_lag_metrics();

            let mut custom_labels = HashMap::new();
            custom_labels.insert(key.clone(), value.clone());

            registry.update_with_options(
                "test-cluster",
                &metrics,
                Granularity::Partition,
                &custom_labels,
            );

            let output = registry.render_prometheus();
            let expected = format!("{key}=\"{value}\"");
            prop_assert!(
                output.contains(&expected),
                "Expected '{}' in output", expected
            );
        }

        /// `skipped_partitions` metric present iff `skipped > 0`
        #[test]
        fn prop_cluster_summary_skipped_iff_nonzero(
            poll_ms in 0u64..100_000,
            compaction in 0u64..1000,
            data_loss in 0u64..1000,
            skipped in 0u64..1000,
        ) {
            let points = build_cluster_summary_points(
                "c", poll_ms, compaction, data_loss, skipped, &HashMap::new(),
            );

            let has_skipped = points.iter().any(|p| p.name == METRIC_SKIPPED_PARTITIONS);
            prop_assert_eq!(has_skipped, skipped > 0);

            // Always has poll_time, compaction, data_loss
            let base_count = 3 + u64::from(skipped > 0);
            prop_assert_eq!(points.len() as u64, base_count);
        }

        /// Topic granularity: aggregated lag == sum of per-partition lags
        #[test]
        fn prop_topic_granularity_sum_invariant(
            lags in proptest::collection::vec(0i64..10_000, 1..10),
        ) {
            let partition_metrics: Vec<PartitionLagMetric> = lags
                .iter()
                .enumerate()
                .map(|(i, &lag)| PartitionLagMetric {
                    cluster_name: "c".to_string(),
                    group_id: "g".to_string(),
                    topic: "t".to_string(),
                    partition: i as i32,
                    member_host: String::new(),
                    consumer_id: String::new(),
                    client_id: String::new(),
                    committed_offset: 1000 - lag,
                    lag,
                    lag_seconds: Some(lag as f64),
                    compaction_detected: false,
                    data_loss_detected: false,
                    messages_lost: 0,
                    retention_margin: 500,
                    lag_retention_ratio: 5.0,
                })
                .collect();

            let expected_lag_sum: i64 = lags.iter().sum();
            let expected_offset_sum: i64 = partition_metrics.iter().map(|m| m.committed_offset).sum();

            let registry = MetricsRegistry::new();
            let metrics = LagMetrics {
                partition_metrics,
                partition_offsets: vec![],
                poll_time_ms: 0,
                compaction_detected_count: 0,
                data_loss_partition_count: 0,
                skipped_partition_count: 0,
            };

            registry.update_with_options("c", &metrics, Granularity::Topic, &HashMap::new());
            let output = registry.render_prometheus();
            let filter = "group=\"g\"";

            let lag = find_metric_value(&output, "kafka_consumergroup_group_lag{", filter);
            prop_assert!(
                (lag - expected_lag_sum as f64).abs() < f64::EPSILON,
                "lag sum: expected {}, got {}", expected_lag_sum, lag
            );

            let offset = find_metric_value(&output, "kafka_consumergroup_group_offset{", filter);
            prop_assert!(
                (offset - expected_offset_sum as f64).abs() < f64::EPSILON,
                "offset sum: expected {}, got {}", expected_offset_sum, offset
            );
        }

        /// Topic granularity: boolean labels use OR across partitions
        #[test]
        fn prop_topic_granularity_boolean_or(
            compaction_flags in proptest::collection::vec(proptest::bool::ANY, 2..5),
            data_loss_flags in proptest::collection::vec(proptest::bool::ANY, 2..5),
        ) {
            let len = compaction_flags.len().min(data_loss_flags.len());
            let partition_metrics: Vec<PartitionLagMetric> = (0..len)
                .map(|i| PartitionLagMetric {
                    cluster_name: "c".to_string(),
                    group_id: "g".to_string(),
                    topic: "t".to_string(),
                    partition: i as i32,
                    member_host: String::new(),
                    consumer_id: String::new(),
                    client_id: String::new(),
                    committed_offset: 50,
                    lag: 10,
                    lag_seconds: Some(1.0),
                    compaction_detected: compaction_flags[i],
                    data_loss_detected: data_loss_flags[i],
                    messages_lost: if data_loss_flags[i] { 5 } else { 0 },
                    retention_margin: 50,
                    lag_retention_ratio: 5.0,
                })
                .collect();

            let expected_compaction = compaction_flags.iter().take(len).any(|&b| b);
            let expected_data_loss = data_loss_flags.iter().take(len).any(|&b| b);

            let registry = MetricsRegistry::new();
            let metrics = LagMetrics {
                partition_metrics,
                partition_offsets: vec![],
                poll_time_ms: 0,
                compaction_detected_count: 0,
                data_loss_partition_count: 0,
                skipped_partition_count: 0,
            };

            registry.update_with_options("c", &metrics, Granularity::Topic, &HashMap::new());
            let output = registry.render_prometheus();

            let lag_sec_line: Vec<&str> = output
                .lines()
                .filter(|l| {
                    l.starts_with("kafka_consumergroup_group_lag_seconds{")
                        && l.contains("group=\"g\"")
                })
                .collect();
            prop_assert_eq!(lag_sec_line.len(), 1);

            let expected_c = format!("compaction_detected=\"{expected_compaction}\"");
            prop_assert!(
                lag_sec_line[0].contains(&expected_c),
                "Expected {}, got: {}", expected_c, lag_sec_line[0]
            );

            let expected_d = format!("data_loss_detected=\"{expected_data_loss}\"");
            prop_assert!(
                lag_sec_line[0].contains(&expected_d),
                "Expected {}, got: {}", expected_d, lag_sec_line[0]
            );
        }
    }

    #[test]
    fn test_otel_excludes_stale_cluster() {
        let registry = MetricsRegistry::with_staleness_threshold(Duration::from_millis(50));
        registry.set_healthy(true);

        // Insert data for two clusters using a metric that has cluster_name label
        registry.begin_cycle("fresh");
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), "fresh".to_string());
        registry.push_points(
            "fresh",
            vec![MetricPoint::gauge(
                METRIC_POLL_TIME_MS,
                labels,
                100.0,
                HELP_POLL_TIME_MS,
            )],
        );
        registry.finish_cycle("fresh");

        registry.begin_cycle("stale");
        let mut labels2 = Labels::new();
        labels2.insert(LABEL_CLUSTER_NAME.to_string(), "stale".to_string());
        registry.push_points(
            "stale",
            vec![MetricPoint::gauge(
                METRIC_POLL_TIME_MS,
                labels2,
                200.0,
                HELP_POLL_TIME_MS,
            )],
        );
        registry.finish_cycle("stale");

        // Wait for staleness threshold to expire
        std::thread::sleep(Duration::from_millis(150));

        // Re-update only "fresh"
        registry.begin_cycle("fresh");
        let mut labels3 = Labels::new();
        labels3.insert(LABEL_CLUSTER_NAME.to_string(), "fresh".to_string());
        registry.push_points(
            "fresh",
            vec![MetricPoint::gauge(
                METRIC_POLL_TIME_MS,
                labels3,
                100.0,
                HELP_POLL_TIME_MS,
            )],
        );
        registry.finish_cycle("fresh");

        let otel_metrics = registry.get_otel_metrics();

        // Collect all cluster_name attribute values from OTel data points
        let cluster_names: Vec<&str> = otel_metrics
            .iter()
            .flat_map(|m| &m.data_points)
            .filter_map(|dp| dp.attributes.get(LABEL_CLUSTER_NAME))
            .map(String::as_str)
            .collect();

        assert!(
            cluster_names.contains(&"fresh"),
            "Expected 'fresh' cluster in OTel metrics, got: {cluster_names:?}"
        );
        assert!(
            !cluster_names.contains(&"stale"),
            "Stale cluster should be filtered from OTel metrics"
        );
    }
}
