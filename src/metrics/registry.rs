use crate::collector::lag_calculator::{
    GroupLagMetric, LagMetrics, PartitionLagMetric, PartitionOffsetMetric, TopicLagMetric,
};
use crate::config::Granularity;
use crate::metrics::definitions::{
    HELP_COMPACTION_DETECTED, HELP_DATA_LOSS_PARTITIONS, HELP_GROUP_LAG, HELP_GROUP_LAG_SECONDS,
    HELP_GROUP_MAX_LAG, HELP_GROUP_MAX_LAG_SECONDS, HELP_GROUP_OFFSET, HELP_GROUP_SUM_LAG,
    HELP_GROUP_TOPIC_SUM_LAG, HELP_LAG_RETENTION_RATIO, HELP_LAST_UPDATE_TIMESTAMP,
    HELP_MESSAGES_LOST, HELP_PARTITION_EARLIEST_OFFSET, HELP_PARTITION_LATEST_OFFSET,
    HELP_POLL_TIME_MS, HELP_RETENTION_MARGIN, HELP_SCRAPE_DURATION_SECONDS, HELP_UP,
    LABEL_CLIENT_ID, LABEL_CLUSTER_NAME, LABEL_COMPACTION_DETECTED, LABEL_CONSUMER_ID,
    LABEL_DATA_LOSS_DETECTED, LABEL_GROUP, LABEL_MEMBER_HOST, LABEL_PARTITION, LABEL_TOPIC,
    METRIC_COMPACTION_DETECTED, METRIC_DATA_LOSS_PARTITIONS, METRIC_GROUP_LAG,
    METRIC_GROUP_LAG_SECONDS, METRIC_GROUP_MAX_LAG, METRIC_GROUP_MAX_LAG_SECONDS,
    METRIC_GROUP_OFFSET, METRIC_GROUP_SUM_LAG, METRIC_GROUP_TOPIC_SUM_LAG,
    METRIC_LAG_RETENTION_RATIO, METRIC_LAST_UPDATE_TIMESTAMP, METRIC_MESSAGES_LOST,
    METRIC_PARTITION_EARLIEST_OFFSET, METRIC_PARTITION_LATEST_OFFSET, METRIC_POLL_TIME_MS,
    METRIC_RETENTION_MARGIN, METRIC_SCRAPE_DURATION_SECONDS, METRIC_UP,
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
            lag_metrics.iter_group_metrics(),
            lag_metrics.iter_topic_metrics(),
            granularity,
            custom_labels,
        ));

        points.extend(build_cluster_summary_points(
            cluster,
            lag_metrics.poll_time_ms,
            lag_metrics.compaction_detected_count,
            lag_metrics.data_loss_partition_count,
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

        // Collect all points, optionally filtering out stale clusters
        let all_points: Vec<MetricPoint> = self
            .metrics
            .iter()
            .filter(|entry| {
                if !filter_stale {
                    return true;
                }
                // Check if this cluster's data is fresh
                self.last_update
                    .get(entry.key()) // No timestamp means stale
                    .is_some_and(|last_update| {
                        now.duration_since(*last_update) <= self.staleness_threshold
                    })
            })
            .flat_map(|entry| entry.value().clone())
            .collect();

        // Group metrics by name for proper HELP/TYPE output
        let mut by_name: HashMap<String, Vec<MetricPoint>> = HashMap::new();
        for point in all_points {
            by_name.entry(point.name.clone()).or_default().push(point);
        }

        // Sort metric names for consistent output
        let mut names: Vec<_> = by_name.keys().cloned().collect();
        names.sort();

        for name in names {
            let points = &by_name[&name];
            if points.is_empty() {
                continue;
            }

            // Output HELP and TYPE once per metric
            if !seen_metrics.contains(&name) {
                let first = &points[0];
                output.push_str(format!("# HELP {name} {}\n", first.help).as_str());
                output.push_str(format!("# TYPE {name} {}\n", first.metric_type.as_str()).as_str());
                seen_metrics.insert(name.clone());
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
                if filter_stale {
                    if let Some(last_update) = self.last_update.get(cluster) {
                        if now.duration_since(*last_update) > self.staleness_threshold {
                            continue;
                        }
                    }
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

        for entry in &self.metrics {
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
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
pub fn build_group_metric_points<'a>(
    partition_metrics: impl Iterator<Item = &'a PartitionLagMetric>,
    group_metrics: impl Iterator<Item = &'a GroupLagMetric>,
    topic_metrics: impl Iterator<Item = &'a TopicLagMetric>,
    granularity: Granularity,
    custom_labels: &HashMap<String, String>,
) -> Vec<MetricPoint> {
    let mut points = Vec::new();

    match granularity {
        Granularity::Partition => {
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
                    labels.insert(
                        LABEL_COMPACTION_DETECTED.to_string(),
                        m.compaction_detected.to_string(),
                    );
                    labels.insert(
                        LABEL_DATA_LOSS_DETECTED.to_string(),
                        m.data_loss_detected.to_string(),
                    );
                    points.push(MetricPoint::gauge(
                        METRIC_GROUP_LAG_SECONDS,
                        labels.clone(),
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
        }
        Granularity::Topic => {
            // Skip partition-level metrics, only output topic aggregates
        }
    }

    for m in group_metrics {
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
        labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
        add_custom_labels(&mut labels, custom_labels);

        points.push(MetricPoint::gauge(
            METRIC_GROUP_MAX_LAG,
            labels.clone(),
            m.max_lag as f64,
            HELP_GROUP_MAX_LAG,
        ));

        points.push(MetricPoint::gauge(
            METRIC_GROUP_SUM_LAG,
            labels.clone(),
            m.sum_lag as f64,
            HELP_GROUP_SUM_LAG,
        ));

        if let Some(max_lag_seconds) = m.max_lag_seconds {
            points.push(MetricPoint::gauge(
                METRIC_GROUP_MAX_LAG_SECONDS,
                labels,
                max_lag_seconds,
                HELP_GROUP_MAX_LAG_SECONDS,
            ));
        }
    }

    for m in topic_metrics {
        let mut labels = Labels::new();
        labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
        labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
        labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
        add_custom_labels(&mut labels, custom_labels);

        points.push(MetricPoint::gauge(
            METRIC_GROUP_TOPIC_SUM_LAG,
            labels,
            m.sum_lag as f64,
            HELP_GROUP_TOPIC_SUM_LAG,
        ));
    }

    points
}

/// Build cluster-level summary metric points (poll time, compaction count, data loss count).
#[allow(clippy::cast_precision_loss)]
pub fn build_cluster_summary_points(
    cluster: &str,
    poll_time_ms: u64,
    compaction_detected_count: u64,
    data_loss_partition_count: u64,
    custom_labels: &HashMap<String, String>,
) -> Vec<MetricPoint> {
    let mut poll_labels = Labels::new();
    poll_labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.to_string());
    add_custom_labels(&mut poll_labels, custom_labels);

    vec![
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
            poll_labels,
            data_loss_partition_count as f64,
            HELP_DATA_LOSS_PARTITIONS,
        ),
    ]
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
            group_metrics: vec![GroupLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                max_lag: 10,
                max_lag_seconds: Some(5.5),
                sum_lag: 10,
            }],
            topic_metrics: vec![TopicLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                sum_lag: 10,
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
    fn test_escape_label_value() {
        assert_eq!(escape_label_value("simple"), "simple");
        assert_eq!(escape_label_value("with\"quote"), "with\\\"quote");
        assert_eq!(escape_label_value("with\\backslash"), "with\\\\backslash");
        assert_eq!(escape_label_value("with\nnewline"), "with\\nnewline");
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
    fn test_scrape_duration_metric() {
        let registry = MetricsRegistry::new();
        registry.set_scrape_duration_ms(1500);

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_scrape_duration_seconds"));
        assert!(output.contains("1.5"));
    }

    #[test]
    fn test_granularity_topic_skips_partition_metrics() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();

        registry.update_with_options(
            "test-cluster",
            &metrics,
            Granularity::Topic,
            &HashMap::new(),
        );

        let output = registry.render_prometheus();

        // Should have topic-level metrics
        assert!(output.contains("kafka_consumergroup_group_max_lag"));
        assert!(output.contains("kafka_consumergroup_group_topic_sum_lag"));

        // Should NOT have partition-level consumer group metrics
        assert!(!output.contains("kafka_consumergroup_group_lag{"));
        assert!(!output.contains("kafka_consumergroup_group_offset{"));
    }

    #[test]
    fn test_custom_labels_added() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();

        let mut custom_labels = HashMap::new();
        custom_labels.insert("environment".to_string(), "production".to_string());
        custom_labels.insert("datacenter".to_string(), "us-west-2".to_string());

        registry.update_with_options(
            "test-cluster",
            &metrics,
            Granularity::Partition,
            &custom_labels,
        );

        let output = registry.render_prometheus();

        assert!(output.contains("environment=\"production\""));
        assert!(output.contains("datacenter=\"us-west-2\""));
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
        registry.push_points(
            "test-cluster",
            vec![MetricPoint::gauge(
                METRIC_GROUP_MAX_LAG,
                labels2,
                42.0,
                HELP_GROUP_MAX_LAG,
            )],
        );

        registry.finish_cycle("test-cluster");

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_partition_latest_offset"));
        assert!(output.contains("110"));
        assert!(output.contains("kafka_consumergroup_group_max_lag"));
        assert!(output.contains("42"));
        assert_eq!(registry.cluster_count(), 1);
    }
}
