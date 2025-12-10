# Rust Kafka Lag Exporter - Development Skill Guide

## Overview

This skill guide provides comprehensive best practices for building a high-performance Kafka consumer group lag exporter in Rust. It covers tokio async patterns, testable architecture, error handling, and production-ready code organization.

---

## Files to Create/Modify

### New Files
1. `src/main.rs` - Entry point, CLI args, signal handling
2. `src/config.rs` - Configuration loading (TOML/env)
3. `src/error.rs` - Error types using thiserror
4. `src/kafka/mod.rs` - Kafka module root
5. `src/kafka/client.rs` - Kafka client wrapper
6. `src/kafka/admin.rs` - Admin API operations
7. `src/kafka/consumer.rs` - Consumer for timestamp fetching
8. `src/collector/mod.rs` - Collector module root
9. `src/collector/offset_collector.rs` - Offset fetching
10. `src/collector/timestamp_sampler.rs` - Timestamp sampling with cache
11. `src/collector/lag_calculator.rs` - Lag computation
12. `src/metrics/mod.rs` - Metrics module root
13. `src/metrics/registry.rs` - In-memory metrics storage
14. `src/metrics/definitions.rs` - Metric names/labels
15. `src/metrics/types.rs` - MetricValue, Labels types
16. `src/export/mod.rs` - Export module root
17. `src/export/prometheus.rs` - Prometheus text format
18. `src/export/otel.rs` - OpenTelemetry exporter
19. `src/http/mod.rs` - HTTP module root
20. `src/http/server.rs` - HTTP server (/metrics, /health, /ready)
21. `src/cluster/mod.rs` - Cluster module root
22. `src/cluster/manager.rs` - Per-cluster orchestration
23. `Cargo.toml` - Dependencies
24. `config.example.toml` - Configuration template

---

## Cargo.toml - Recommended Dependencies

```toml
[package]
name = "klag-exporter"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"

[dependencies]
# Async runtime
tokio = { version = "1.43", features = ["full", "tracing"] }

# Kafka client
rdkafka = { version = "0.36", features = ["cmake-build", "ssl", "sasl"] }

# HTTP server
axum = { version = "0.8", features = ["tokio"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["trace", "timeout"] }

# Metrics
metrics = "0.24"
metrics-exporter-prometheus = "0.16"

# OpenTelemetry
opentelemetry = "0.27"
opentelemetry_sdk = { version = "0.27", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.27", features = ["tonic", "metrics"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"

# Configuration
config = "0.14"

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }

# Error handling
thiserror = "2"
anyhow = "1"

# Concurrent data structures
dashmap = "6"

# Regex for filtering
regex = "1"

# Time utilities
chrono = { version = "0.4", default-features = false, features = ["clock"] }

[dev-dependencies]
# Testing
tokio-test = "0.4"
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["kafka"] }
wiremock = "0.6"
proptest = "1"
criterion = { version = "0.5", features = ["async_tokio"] }

# HTTP testing
axum-test = "16"

[[bench]]
name = "metrics_benchmark"
harness = false
```

---

## Project Structure Best Practices

```
src/
├── main.rs                    # Entry point only - minimal code
├── lib.rs                     # Re-exports for testing
├── config.rs                  # Config structs + validation
├── error.rs                   # Central error types
├── kafka/
│   ├── mod.rs                 # pub use exports
│   ├── client.rs              # KafkaClient trait + impl
│   ├── admin.rs               # AdminClient wrapper
│   └── consumer.rs            # TimestampConsumer
├── collector/
│   ├── mod.rs
│   ├── offset_collector.rs    # Offset fetching logic
│   ├── timestamp_sampler.rs   # Cached timestamp fetching
│   └── lag_calculator.rs      # Pure computation
├── metrics/
│   ├── mod.rs
│   ├── registry.rs            # DashMap-based storage
│   ├── definitions.rs         # Metric name constants
│   └── types.rs               # Value types
├── export/
│   ├── mod.rs
│   ├── prometheus.rs          # Text format rendering
│   └── otel.rs                # OTLP export
├── http/
│   ├── mod.rs
│   └── server.rs              # Axum routes
└── cluster/
    ├── mod.rs
    └── manager.rs             # Collection orchestration

tests/
├── integration/
│   ├── mod.rs
│   ├── kafka_tests.rs         # Testcontainers Kafka tests
│   └── http_tests.rs          # HTTP endpoint tests
└── common/
    └── mod.rs                 # Shared test utilities
```

---

## Error Handling Pattern

### `src/error.rs`

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP server error: {0}")]
    Http(String),

    #[error("Timeout waiting for {operation}")]
    Timeout { operation: String },

    #[error("Regex compilation failed: {0}")]
    Regex(#[from] regex::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
```

### Function: `Error::is_retryable(&self) -> bool`
Returns true for transient errors (Kafka connection issues, timeouts) that warrant retry with backoff.

### Function: `Error::config(msg: impl Into<String>) -> Error`
Convenience constructor for configuration validation errors.

---

## Async Patterns with Tokio

### Graceful Shutdown Pattern

```rust
use tokio::sync::broadcast;
use tokio::signal;

pub async fn shutdown_signal(shutdown_tx: broadcast::Sender<()>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    let _ = shutdown_tx.send(());
}
```

### Function: `setup_signal_handlers(shutdown_tx: Sender<()>)`
Registers SIGTERM/SIGINT handlers; sends to broadcast channel on signal.

### Bounded Concurrency Pattern

```rust
use tokio::sync::Semaphore;
use std::sync::Arc;

pub struct ConcurrentFetcher {
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
}

impl ConcurrentFetcher {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    pub async fn fetch_all<T, F, Fut>(&self, items: Vec<T>, f: F) -> Vec<Result<T::Output>>
    where
        T: Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T::Output>> + Send,
    {
        let futures = items.into_iter().map(|item| {
            let permit = self.semaphore.clone().acquire_owned();
            async move {
                let _permit = permit.await?;
                f(item).await
            }
        });

        futures::future::join_all(futures).await
    }
}
```

### Function: `ConcurrentFetcher::fetch_all<T, F, Fut>(items, f)`
Executes async function `f` on all items with bounded concurrency via semaphore.

### select! for Timeout with Cancellation

```rust
use tokio::time::{timeout, Duration};

pub async fn with_timeout<T>(
    future: impl Future<Output = T>,
    duration: Duration,
    operation: &str,
) -> Result<T> {
    timeout(duration, future)
        .await
        .map_err(|_| Error::Timeout {
            operation: operation.to_string(),
        })
}
```

### Function: `with_timeout<T>(future, duration, operation)`
Wraps future with timeout; returns Error::Timeout on expiration.

---

## Configuration Pattern

### `src/config.rs`

```rust
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub exporter: ExporterConfig,
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExporterConfig {
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    pub http_port: u16,
    pub http_host: String,
    pub timestamp_sampling: TimestampSamplingConfig,
    pub otel: Option<OtelConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default)]
    pub group_whitelist: Vec<String>,
    #[serde(default)]
    pub group_blacklist: Vec<String>,
    #[serde(default)]
    pub consumer_properties: std::collections::HashMap<String, String>,
}

impl Config {
    pub fn load(path: Option<&str>) -> Result<Self> {
        // Load from file with env var substitution
    }

    pub fn validate(&self) -> Result<()> {
        // Validate bootstrap_servers not empty, regex patterns compile
    }
}
```

### Function: `Config::load(path: Option<&str>) -> Result<Config>`
Loads TOML config from file; substitutes `${ENV_VAR}` patterns; validates.

### Function: `Config::validate(&self) -> Result<()>`
Validates all fields: non-empty bootstrap servers, compilable regex patterns.

### Function: `ClusterConfig::compile_filters(&self) -> Result<CompiledFilters>`
Compiles group/topic whitelist and blacklist regex patterns into `Regex` objects.

---

## Kafka Client Abstraction

### `src/kafka/client.rs`

```rust
use async_trait::async_trait;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;

/// Trait for Kafka operations - enables mocking in tests
#[async_trait]
pub trait KafkaOperations: Send + Sync {
    async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>>;
    async fn describe_consumer_groups(&self, group_ids: &[&str]) -> Result<Vec<GroupDescription>>;
    async fn list_consumer_group_offsets(&self, group_id: &str) -> Result<HashMap<TopicPartition, i64>>;
    async fn list_offsets(&self, partitions: &[TopicPartition], position: OffsetPosition) -> Result<HashMap<TopicPartition, i64>>;
}

pub struct KafkaClient {
    admin: AdminClient<DefaultClientContext>,
    config: ClusterConfig,
}

impl KafkaClient {
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        // Build rdkafka client config
    }
}

#[async_trait]
impl KafkaOperations for KafkaClient {
    // Implementations...
}
```

### Function: `KafkaClient::new(config: &ClusterConfig) -> Result<Self>`
Creates AdminClient with configured bootstrap servers, auth, and timeouts.

### Function: `list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>>`
Lists all consumer groups via Admin API; returns group IDs with protocol type.

### Function: `describe_consumer_groups(&self, group_ids) -> Result<Vec<GroupDescription>>`
Describes groups to get member assignments (client_id, consumer_id, host, partitions).

### Function: `list_consumer_group_offsets(&self, group_id) -> Result<HashMap<TopicPartition, i64>>`
Fetches committed offsets for a consumer group.

### Function: `list_offsets(&self, partitions, position) -> Result<HashMap<TopicPartition, i64>>`
Fetches earliest or latest offsets for given partitions.

---

## Concurrent Metrics Registry with DashMap

### `src/metrics/registry.rs`

```rust
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct MetricsRegistry {
    partition_metrics: Arc<DashMap<PartitionKey, PartitionMetrics>>,
    group_metrics: Arc<DashMap<GroupKey, GroupMetrics>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            partition_metrics: Arc::new(DashMap::new()),
            group_metrics: Arc::new(DashMap::new()),
        }
    }

    pub fn update(&self, cluster: &str, metrics: LagMetrics) {
        // Use entry API for atomic updates
        for metric in metrics.partition_metrics {
            let key = PartitionKey::new(cluster, &metric);
            self.partition_metrics
                .entry(key)
                .and_modify(|m| *m = metric.clone())
                .or_insert(metric);
        }

        // Evict stale metrics
        self.evict_stale(cluster, &metrics);
    }

    pub fn render_prometheus(&self) -> String {
        // Iterate and format
    }
}
```

### Function: `MetricsRegistry::new() -> Self`
Creates thread-safe in-memory metrics registry using DashMap.

### Function: `MetricsRegistry::update(&self, cluster, metrics)`
Replaces all metrics for a cluster; handles metric eviction for disappeared groups.

### Function: `MetricsRegistry::render_prometheus(&self) -> String`
Renders all metrics in Prometheus text exposition format.

### Function: `MetricsRegistry::get_otel_metrics(&self) -> Vec<OtelMetric>`
Returns metrics in OpenTelemetry-compatible format.

---

## HTTP Server with Axum

### `src/http/server.rs`

```rust
use axum::{
    Router,
    routing::get,
    extract::State,
    response::IntoResponse,
    http::StatusCode,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<MetricsRegistry>,
    pub ready: Arc<AtomicBool>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .with_state(state)
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.registry.render_prometheus();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

async fn health_handler() -> impl IntoResponse {
    StatusCode::OK
}

async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    if state.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
```

### Function: `create_router(state: AppState) -> Router`
Creates Axum router with /metrics, /health, /ready endpoints.

### Function: `metrics_handler(State) -> impl IntoResponse`
Returns Prometheus text format metrics from registry.

### Function: `health_handler() -> impl IntoResponse`
Returns 200 OK for liveness probe.

### Function: `ready_handler(State) -> impl IntoResponse`
Returns 200 if ready flag set, 503 otherwise.

---

## Timestamp Sampler with TTL Cache

### `src/collector/timestamp_sampler.rs`

```rust
use dashmap::DashMap;
use std::time::{Duration, Instant};

pub struct TimestampSampler {
    cache: DashMap<CacheKey, CachedTimestamp>,
    ttl: Duration,
    consumer: TimestampConsumer,
}

struct CachedTimestamp {
    timestamp: i64,
    cached_at: Instant,
    offset: i64,
}

impl TimestampSampler {
    pub fn new(consumer: TimestampConsumer, cache_ttl: Duration) -> Self {
        Self {
            cache: DashMap::new(),
            ttl: cache_ttl,
            consumer,
        }
    }

    pub async fn get_timestamp(&self, tp: &TopicPartition, offset: i64) -> Result<Option<i64>> {
        let key = CacheKey::new(tp);

        // Check cache
        if let Some(cached) = self.cache.get(&key) {
            if cached.cached_at.elapsed() < self.ttl && cached.offset == offset {
                return Ok(Some(cached.timestamp));
            }
        }

        // Fetch from Kafka
        let timestamp = self.consumer.fetch_timestamp(tp, offset).await?;

        if let Some(ts) = timestamp {
            self.cache.insert(key, CachedTimestamp {
                timestamp: ts,
                cached_at: Instant::now(),
                offset,
            });
        }

        Ok(timestamp)
    }

    pub fn clear_stale_entries(&self) {
        self.cache.retain(|_, v| v.cached_at.elapsed() < self.ttl);
    }
}
```

### Function: `TimestampSampler::new(consumer, cache_ttl) -> Self`
Creates timestamp sampler with TTL-based cache.

### Function: `get_timestamp(&self, tp, offset) -> Result<Option<i64>>`
Returns cached timestamp or fetches from Kafka; updates cache on fetch.

### Function: `clear_stale_entries(&self)`
Removes cache entries older than TTL. Call periodically.

---

## Lag Calculator (Pure Functions)

### `src/collector/lag_calculator.rs`

```rust
pub struct LagCalculator;

impl LagCalculator {
    pub fn calculate(
        snapshot: &OffsetsSnapshot,
        timestamps: &HashMap<TopicPartition, i64>,
        now_ms: i64,
    ) -> LagMetrics {
        let mut partition_metrics = Vec::new();
        let mut group_aggregates: HashMap<String, GroupAggregate> = HashMap::new();

        for (group_id, group_offsets) in &snapshot.consumer_offsets {
            for (tp, committed_offset) in group_offsets {
                let high_watermark = snapshot.high_watermarks.get(tp).copied().unwrap_or(0);

                // Clamp negative lag to 0 (race condition protection)
                let offset_lag = (high_watermark - committed_offset).max(0);

                let time_lag_seconds = timestamps
                    .get(tp)
                    .map(|ts| ((now_ms - ts) / 1000).max(0))
                    .unwrap_or(0);

                partition_metrics.push(PartitionLagMetric {
                    group: group_id.clone(),
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                    offset_lag,
                    time_lag_seconds,
                    committed_offset: *committed_offset,
                    high_watermark,
                });

                // Update aggregates
                group_aggregates
                    .entry(group_id.clone())
                    .and_modify(|agg| {
                        agg.max_lag = agg.max_lag.max(offset_lag);
                        agg.sum_lag += offset_lag;
                        agg.max_time_lag = agg.max_time_lag.max(time_lag_seconds);
                    })
                    .or_insert(GroupAggregate {
                        max_lag: offset_lag,
                        sum_lag: offset_lag,
                        max_time_lag: time_lag_seconds,
                    });
            }
        }

        LagMetrics {
            partition_metrics,
            group_aggregates,
        }
    }
}
```

### Function: `LagCalculator::calculate(snapshot, timestamps, now_ms) -> LagMetrics`
Computes all lag metrics from snapshot data. Pure function - no side effects.

### Function: `LagMetrics::iter_partition_metrics(&self) -> impl Iterator`
Iterates over partition-level metrics for export.

### Function: `LagMetrics::iter_group_metrics(&self) -> impl Iterator`
Iterates over group-level aggregate metrics.

---

## Cluster Manager (Orchestration)

### `src/cluster/manager.rs`

```rust
use tokio::sync::broadcast;
use std::time::Duration;

pub struct ClusterManager {
    config: ClusterConfig,
    registry: Arc<MetricsRegistry>,
    kafka_client: Arc<dyn KafkaOperations>,
    timestamp_sampler: TimestampSampler,
}

impl ClusterManager {
    pub fn new(
        config: ClusterConfig,
        registry: Arc<MetricsRegistry>,
    ) -> Result<Self> {
        // Initialize Kafka client and timestamp sampler
    }

    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        let mut backoff = ExponentialBackoff::default();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.collect_once().await {
                        Ok(_) => {
                            backoff.reset();
                        }
                        Err(e) => {
                            tracing::error!(
                                cluster = %self.config.name,
                                error = %e,
                                "Collection failed"
                            );
                            if e.is_retryable() {
                                tokio::time::sleep(backoff.next_backoff()).await;
                            }
                        }
                    }
                }
                _ = shutdown.recv() => {
                    tracing::info!(cluster = %self.config.name, "Shutting down");
                    break;
                }
            }
        }
    }

    async fn collect_once(&self) -> Result<()> {
        let start = Instant::now();

        // 1. Fetch offsets
        let snapshot = self.offset_collector.collect().await?;

        // 2. Sample timestamps for lagging partitions
        let timestamps = self.sample_timestamps(&snapshot).await?;

        // 3. Calculate metrics
        let now_ms = chrono::Utc::now().timestamp_millis();
        let metrics = LagCalculator::calculate(&snapshot, &timestamps, now_ms);

        // 4. Update registry
        self.registry.update(&self.config.name, metrics);

        // Record operational metrics
        tracing::debug!(
            cluster = %self.config.name,
            duration_ms = %start.elapsed().as_millis(),
            "Collection complete"
        );

        Ok(())
    }

    pub fn spawn(
        config: ClusterConfig,
        registry: Arc<MetricsRegistry>,
        shutdown: broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let manager = match ClusterManager::new(config.clone(), registry) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(cluster = %config.name, error = %e, "Failed to create manager");
                    return;
                }
            };
            manager.run(shutdown).await;
        })
    }
}
```

### Function: `ClusterManager::new(config, registry) -> Result<Self>`
Creates manager for a single Kafka cluster.

### Function: `ClusterManager::run(&self, shutdown)`
Main collection loop. Polls at configured interval, updates registry.

### Function: `ClusterManager::collect_once(&self) -> Result<()>`
Single collection cycle: fetch offsets, sample timestamps, calculate lag, update registry.

### Function: `ClusterManager::spawn(config, registry, shutdown) -> JoinHandle<()>`
Spawns cluster manager as independent tokio task. Isolates failures.

---

## Tracing and Logging Setup

### `src/main.rs`

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,rdkafka=warn"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .json())
        .init();
}
```

### Function: `init_tracing()`
Initializes structured JSON logging with environment-based filter.

---

## OpenTelemetry Integration

### `src/export/otel.rs`

```rust
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};

pub struct OtelExporter {
    registry: Arc<MetricsRegistry>,
    meter_provider: SdkMeterProvider,
}

impl OtelExporter {
    pub fn new(registry: Arc<MetricsRegistry>, config: &OtelConfig) -> Result<Self> {
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()?;

        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .with_resource(Resource::builder()
                .with_service_name("klag-exporter")
                .build())
            .build();

        global::set_meter_provider(provider.clone());

        Ok(Self {
            registry,
            meter_provider: provider,
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.meter_provider.shutdown()?;
        Ok(())
    }
}
```

### Function: `OtelExporter::new(registry, config) -> Result<Self>`
Creates OpenTelemetry exporter with configured OTLP endpoint.

### Function: `OtelExporter::shutdown(&self) -> Result<()>`
Gracefully shuts down meter provider, flushing pending exports.

---

## Testing Patterns

### Unit Test Pattern (Pure Functions)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lag_calculator_offset_lag() {
        let snapshot = OffsetsSnapshot {
            consumer_offsets: [(
                "test-group".to_string(),
                [(TopicPartition::new("topic", 0), 100)].into(),
            )].into(),
            high_watermarks: [(TopicPartition::new("topic", 0), 150)].into(),
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0);

        assert_eq!(metrics.partition_metrics[0].offset_lag, 50);
    }

    #[test]
    fn test_lag_calculator_handles_negative_lag() {
        // Committed > high_watermark (race condition)
        let snapshot = OffsetsSnapshot {
            consumer_offsets: [(
                "test-group".to_string(),
                [(TopicPartition::new("topic", 0), 200)].into(),
            )].into(),
            high_watermarks: [(TopicPartition::new("topic", 0), 150)].into(),
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0);

        assert_eq!(metrics.partition_metrics[0].offset_lag, 0); // Clamped
    }
}
```

### Async Test Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_cluster_manager_shutdown() {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let registry = Arc::new(MetricsRegistry::new());

        let handle = tokio::spawn(async move {
            // Simulate manager loop
            let mut rx = shutdown_rx;
            tokio::select! {
                _ = rx.recv() => {}
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    panic!("Should have shutdown");
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown_tx.send(()).unwrap();

        handle.await.unwrap();
    }
}
```

### Mock Kafka Client for Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockKafkaClient {
        groups: Vec<ConsumerGroupInfo>,
        offsets: HashMap<String, HashMap<TopicPartition, i64>>,
    }

    #[async_trait]
    impl KafkaOperations for MockKafkaClient {
        async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
            Ok(self.groups.clone())
        }

        async fn list_consumer_group_offsets(&self, group_id: &str) -> Result<HashMap<TopicPartition, i64>> {
            Ok(self.offsets.get(group_id).cloned().unwrap_or_default())
        }

        // ... other methods
    }

    #[tokio::test]
    async fn test_offset_collector_with_mock() {
        let mock_client = Arc::new(MockKafkaClient {
            groups: vec![ConsumerGroupInfo {
                id: "test-group".to_string(),
                protocol_type: "consumer".to_string(),
            }],
            offsets: [(
                "test-group".to_string(),
                [(TopicPartition::new("topic", 0), 100)].into(),
            )].into(),
        });

        let collector = OffsetCollector::new(mock_client, CompiledFilters::default());
        let snapshot = collector.collect().await.unwrap();

        assert_eq!(snapshot.consumer_offsets.len(), 1);
    }
}
```

### Integration Test with Testcontainers

```rust
// tests/integration/kafka_tests.rs
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage,
    ImageExt,
};

#[tokio::test]
async fn test_end_to_end_metrics_collection() -> Result<(), Box<dyn std::error::Error>> {
    // Start Kafka container
    let kafka = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_exposed_port(9092.tcp())
        .with_env_var("KAFKA_BROKER_ID", "1")
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT")
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_wait_for(WaitFor::message_on_stdout("started"))
        .start()
        .await?;

    let host_port = kafka.get_host_port_ipv4(9092).await?;
    let bootstrap_servers = format!("localhost:{}", host_port);

    // Create test configuration
    let config = ClusterConfig {
        name: "test-cluster".to_string(),
        bootstrap_servers,
        group_whitelist: vec![".*".to_string()],
        ..Default::default()
    };

    // Test offset collection
    let client = KafkaClient::new(&config)?;
    let groups = client.list_consumer_groups().await?;

    // Verify internal topics exist
    assert!(groups.iter().any(|g| g.id.starts_with("__")));

    Ok(())
}
```

### HTTP Endpoint Test with axum-test

```rust
// tests/integration/http_tests.rs
use axum_test::TestServer;

#[tokio::test]
async fn test_prometheus_endpoint_returns_metrics() {
    let registry = Arc::new(MetricsRegistry::new());

    // Add test metrics
    registry.update("test-cluster", LagMetrics {
        partition_metrics: vec![PartitionLagMetric {
            group: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset_lag: 100,
            time_lag_seconds: 5,
            committed_offset: 900,
            high_watermark: 1000,
        }],
        group_aggregates: Default::default(),
    });

    let state = AppState {
        registry,
        ready: Arc::new(AtomicBool::new(true)),
    };

    let app = create_router(state);
    let server = TestServer::new(app).unwrap();

    let response = server.get("/metrics").await;
    response.assert_status_ok();

    let body = response.text();
    assert!(body.contains("kafka_consumergroup_group_lag"));
    assert!(body.contains("test-group"));
    assert!(body.contains("100"));
}

#[tokio::test]
async fn test_ready_endpoint_not_ready() {
    let state = AppState {
        registry: Arc::new(MetricsRegistry::new()),
        ready: Arc::new(AtomicBool::new(false)),
    };

    let app = create_router(state);
    let server = TestServer::new(app).unwrap();

    let response = server.get("/ready").await;
    response.assert_status_service_unavailable();
}
```

---

## Test Specifications

### Unit Tests

| Test Name | Behavior to Cover |
|-----------|-------------------|
| `test_config_loads_from_file` | Config correctly parses TOML with all fields |
| `test_config_env_override` | Environment variables override file config values |
| `test_config_validates_bootstrap_servers` | Validation fails if bootstrap_servers is empty |
| `test_regex_filter_whitelist_match` | Group/topic passes when matches whitelist pattern |
| `test_regex_filter_blacklist_reject` | Group rejected when matches blacklist despite whitelist |
| `test_lag_calculator_offset_lag` | Offset lag = high_watermark - committed_offset |
| `test_lag_calculator_time_lag` | Time lag = now - message_timestamp in seconds |
| `test_lag_calculator_handles_negative_lag` | Lag clamped to 0 when committed > high_watermark |
| `test_lag_calculator_max_lag_aggregation` | Max lag is highest across all partitions |
| `test_lag_calculator_sum_lag_aggregation` | Sum lag totals all partition lags correctly |
| `test_metrics_registry_update_replaces` | Update replaces metrics, old metrics removed |
| `test_metrics_registry_evicts_disappeared_groups` | Groups no longer present are evicted |
| `test_prometheus_format_gauge` | Gauge metric renders correct text format |
| `test_prometheus_format_labels_escaped` | Labels with special chars properly escaped |
| `test_timestamp_cache_ttl_expiry` | Cached timestamps expire after TTL |
| `test_timestamp_sampler_returns_cached` | Returns cached value without Kafka call when fresh |

### Integration Tests

| Test Name | Behavior to Cover |
|-----------|-------------------|
| `test_end_to_end_metrics_collection` | Full flow with real Kafka via testcontainers |
| `test_prometheus_endpoint_returns_metrics` | HTTP /metrics returns valid Prometheus format |
| `test_health_endpoint_always_ok` | HTTP /health returns 200 |
| `test_ready_endpoint_reflects_state` | HTTP /ready returns 503 until ready |
| `test_graceful_shutdown` | SIGTERM triggers clean shutdown |
| `test_cluster_reconnect_on_failure` | Exporter recovers after Kafka disconnect |
| `test_multiple_clusters_isolated` | Separate metrics per cluster, failures isolated |

---

## Performance Considerations

### DashMap Shard Configuration

```rust
// For high-concurrency workloads, tune shard count
// Default is num_cpus * 4, which is usually optimal
let map = DashMap::with_shard_amount(32);
```

### Batch Operations

```rust
// Prefer bulk operations over individual updates
registry.update_batch(cluster, metrics_batch);

// Use retain for bulk deletion
map.retain(|k, _| active_keys.contains(k));
```

### Avoid Holding DashMap Guards Across Await Points

```rust
// BAD - holds lock across await
let value = map.get(&key);
some_async_operation().await; // Lock held!
drop(value);

// GOOD - clone and release immediately
let value = map.get(&key).map(|v| v.clone());
some_async_operation().await;
```

---

## Summary

This skill guide covers:

1. **Error handling** with `thiserror` for typed errors and automatic conversions
2. **Async patterns** with tokio: graceful shutdown, bounded concurrency, timeouts
3. **Testable architecture** via traits for dependency injection and mocking
4. **Concurrent data structures** with DashMap for lock-free metrics storage
5. **HTTP server** with Axum following idiomatic patterns
6. **Configuration** with serde + toml + environment variable substitution
7. **Observability** with tracing and OpenTelemetry integration
8. **Testing** at unit, integration, and end-to-end levels with testcontainers

Follow these patterns to build a production-ready, maintainable, and testable Kafka lag exporter.
