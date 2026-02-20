use crate::config::OtelConfig;
use crate::error::Result;
use crate::metrics::registry::MetricsRegistry;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

pub async fn run_otel_exporter(
    registry: Arc<MetricsRegistry>,
    config: OtelConfig,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    info!(endpoint = %config.endpoint, "Starting OpenTelemetry exporter");

    // Initialize OTLP exporter
    let meter_provider = match init_meter_provider(&config) {
        Ok(provider) => provider,
        Err(e) => {
            error!(error = %e, "Failed to initialize OpenTelemetry meter provider");
            return Err(crate::error::KlagError::Otel(e.to_string()));
        }
    };

    global::set_meter_provider(meter_provider.clone());
    let meter = global::meter("klag-exporter");

    // Register all observable gauges using a shared helper
    let _gauges = register_otel_gauges(&meter, &registry);

    info!("OpenTelemetry metrics registered, waiting for shutdown...");

    // Wait for shutdown signal
    let _ = shutdown.recv().await;
    info!("OpenTelemetry exporter shutting down");

    // Shutdown meter provider
    if let Err(e) = meter_provider.shutdown() {
        warn!(error = %e, "Failed to shutdown OpenTelemetry meter provider");
    }

    Ok(())
}

/// Gauge definition: (`metric_name`, description)
const OTEL_GAUGE_DEFS: &[(&str, &str)] = &[
    (
        "kafka_partition_latest_offset",
        "Latest offset for a partition",
    ),
    (
        "kafka_partition_earliest_offset",
        "Earliest offset for a partition",
    ),
    (
        "kafka_consumergroup_group_offset",
        "Current committed offset of a consumer group for a partition",
    ),
    (
        "kafka_consumergroup_group_lag",
        "Current lag of a consumer group for a partition",
    ),
    (
        "kafka_consumergroup_group_lag_seconds",
        "Time lag in seconds for a consumer group partition",
    ),
    (
        "kafka_consumergroup_poll_time_ms",
        "Time taken to collect consumer group metrics in milliseconds",
    ),
    (
        "kafka_lag_exporter_scrape_duration_seconds",
        "Duration of the last metrics collection in seconds",
    ),
    (
        "kafka_lag_exporter_up",
        "Whether the Kafka lag exporter is healthy",
    ),
    (
        "kafka_consumergroup_group_messages_lost",
        "Number of messages deleted by retention before consumer processed them",
    ),
    (
        "kafka_consumergroup_group_retention_margin",
        "Offset distance between consumer position and deletion boundary",
    ),
    (
        "kafka_consumergroup_group_lag_retention_ratio",
        "Percentage of retention window occupied by consumer lag",
    ),
    (
        "kafka_lag_exporter_data_loss_partitions_total",
        "Number of partitions where data loss occurred",
    ),
];

/// Registers all `Otel` observable gauges, returning handles to keep them alive.
fn register_otel_gauges(
    meter: &opentelemetry::metrics::Meter,
    registry: &Arc<MetricsRegistry>,
) -> Vec<opentelemetry::metrics::ObservableGauge<f64>> {
    OTEL_GAUGE_DEFS
        .iter()
        .map(|&(name, description)| {
            let reg = Arc::clone(registry);
            let metric_name = name.to_string();
            meter
                .f64_observable_gauge(name)
                .with_description(description)
                .with_callback(move |observer| {
                    for metric in reg.get_otel_metrics() {
                        if metric.name == metric_name {
                            for dp in &metric.data_points {
                                let attrs: Vec<KeyValue> = dp
                                    .attributes
                                    .iter()
                                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                    .collect();
                                observer.observe(dp.value, &attrs);
                            }
                        }
                    }
                })
                .build()
        })
        .collect()
}

fn init_meter_provider(
    config: &OtelConfig,
) -> std::result::Result<SdkMeterProvider, Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::runtime;

    // Build the exporter
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    // Create periodic reader
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(config.export_interval)
        .build();

    // Build the provider
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "klag-exporter",
        )]))
        .build();

    debug!(endpoint = %config.endpoint, "OpenTelemetry meter provider initialized");
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Integration test for Otel exporter requires a running collector
    // This unit test just verifies the registry can be queried without panics
    #[test]
    fn test_otel_metrics_from_registry() {
        let registry = MetricsRegistry::new();
        registry.set_healthy(true);
        registry.set_scrape_duration_ms(100);

        let metrics = registry.get_otel_metrics();

        // Should have at least the up and scrape duration metrics
        assert!(!metrics.is_empty());

        let metric_names: Vec<_> = metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(metric_names.contains(&"kafka_lag_exporter_up"));
        assert!(metric_names.contains(&"kafka_lag_exporter_scrape_duration_seconds"));
    }
}
