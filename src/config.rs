use crate::error::{KlagError, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub exporter: ExporterConfig,
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExporterConfig {
    #[serde(with = "humantime_serde", default = "default_poll_interval")]
    pub poll_interval: Duration,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_http_host")]
    pub http_host: String,
    #[serde(default = "default_granularity")]
    pub granularity: Granularity,
    #[serde(default)]
    pub timestamp_sampling: TimestampSamplingConfig,
    #[serde(default)]
    pub otel: OtelConfig,
    #[serde(default)]
    pub leadership: LeadershipConfig,
    #[serde(default)]
    pub performance: PerformanceConfig,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Topic,
    Partition,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimestampSamplingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(with = "humantime_serde", default = "default_cache_ttl")]
    pub cache_ttl: Duration,
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: usize,
}

/// Performance tuning configuration for large clusters.
/// These settings control parallelism and timeouts for Kafka operations.
#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Timeout for individual Kafka API operations (metadata, watermarks, etc.)
    #[serde(with = "humantime_serde", default = "default_kafka_timeout")]
    pub kafka_timeout: Duration,
    /// Maximum number of consumer groups to fetch offsets for in parallel
    #[serde(default = "default_max_concurrent_groups")]
    pub max_concurrent_groups: usize,
    /// Maximum number of consumer groups to describe in a single Admin API call
    #[serde(default = "default_describe_groups_batch_size")]
    pub describe_groups_batch_size: usize,
    /// TTL for the compacted topics cache (compacted topic list rarely changes)
    #[serde(
        with = "humantime_serde",
        default = "default_compacted_topics_cache_ttl"
    )]
    pub compacted_topics_cache_ttl: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OtelConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_otel_endpoint")]
    pub endpoint: String,
    #[serde(with = "humantime_serde", default = "default_export_interval")]
    pub export_interval: Duration,
}

/// Configuration for leader election in high availability deployments.
#[derive(Debug, Deserialize, Clone)]
pub struct LeadershipConfig {
    /// Enable leader election. When disabled (default), runs in single-instance mode.
    #[serde(default)]
    pub enabled: bool,
    /// Leadership provider type. Currently only `kubernetes` is supported.
    #[serde(default = "default_leadership_provider")]
    pub provider: LeadershipProvider,
    /// Name of the Kubernetes Lease resource.
    #[serde(default = "default_lease_name")]
    pub lease_name: String,
    /// Namespace for the Lease resource. Supports env var substitution.
    #[serde(default = "default_lease_namespace")]
    pub lease_namespace: String,
    /// Identity of this instance. Defaults to `HOSTNAME` or `POD_NAME` env var.
    #[allow(dead_code)] // Used by kubernetes feature
    pub identity: Option<String>,
    /// Duration the lease is valid in seconds.
    #[serde(default = "default_lease_duration")]
    #[allow(dead_code)] // Used by kubernetes feature
    pub lease_duration_secs: u32,
    /// Grace period for lease renewal in seconds. Must be less than `lease_duration`.
    #[serde(default = "default_grace_period")]
    #[allow(dead_code)] // Used by kubernetes feature
    pub grace_period_secs: u32,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LeadershipProvider {
    #[default]
    Kubernetes,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default = "default_whitelist")]
    pub group_whitelist: Vec<String>,
    #[serde(default)]
    pub group_blacklist: Vec<String>,
    #[serde(default = "default_whitelist")]
    pub topic_whitelist: Vec<String>,
    #[serde(default = "default_topic_blacklist")]
    pub topic_blacklist: Vec<String>,
    #[serde(default)]
    pub consumer_properties: HashMap<String, String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

const fn default_poll_interval() -> Duration {
    Duration::from_secs(30)
}

const fn default_http_port() -> u16 {
    8000
}

fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_granularity() -> Granularity {
    Granularity::Topic
}

const fn default_true() -> bool {
    true
}

const fn default_cache_ttl() -> Duration {
    Duration::from_secs(60)
}

const fn default_max_concurrent_fetches() -> usize {
    10
}

const fn default_kafka_timeout() -> Duration {
    Duration::from_secs(30)
}

const fn default_max_concurrent_groups() -> usize {
    10
}

const fn default_describe_groups_batch_size() -> usize {
    100
}

const fn default_compacted_topics_cache_ttl() -> Duration {
    Duration::from_secs(300)
}

fn default_otel_endpoint() -> String {
    "http://localhost:4317".to_string()
}

const fn default_export_interval() -> Duration {
    Duration::from_secs(60)
}

const fn default_leadership_provider() -> LeadershipProvider {
    LeadershipProvider::Kubernetes
}

fn default_lease_name() -> String {
    "klag-exporter".to_string()
}

fn default_lease_namespace() -> String {
    "default".to_string()
}

const fn default_lease_duration() -> u32 {
    15
}

const fn default_grace_period() -> u32 {
    5
}

fn default_whitelist() -> Vec<String> {
    vec![".*".to_string()]
}

fn default_topic_blacklist() -> Vec<String> {
    vec!["__.*".to_string()]
}

impl Default for TimestampSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_ttl: default_cache_ttl(),
            max_concurrent_fetches: default_max_concurrent_fetches(),
        }
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_otel_endpoint(),
            export_interval: default_export_interval(),
        }
    }
}

impl Default for LeadershipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_leadership_provider(),
            lease_name: default_lease_name(),
            lease_namespace: default_lease_namespace(),
            identity: None,
            lease_duration_secs: default_lease_duration(),
            grace_period_secs: default_grace_period(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            kafka_timeout: default_kafka_timeout(),
            max_concurrent_groups: default_max_concurrent_groups(),
            describe_groups_batch_size: default_describe_groups_batch_size(),
            compacted_topics_cache_ttl: default_compacted_topics_cache_ttl(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> Result<Self> {
        let config_path = path.unwrap_or("config.toml");

        if !Path::new(config_path).exists() {
            return Err(KlagError::Config(format!(
                "Configuration file not found: {config_path}"
            )));
        }

        let content = std::fs::read_to_string(config_path)?;
        let content = Self::substitute_env_vars(&content);

        let config: Self = toml::from_str(&content)
            .map_err(|e| KlagError::Config(format!("TOML parse error: {e}")))?;

        config.validate()?;
        Ok(config)
    }

    fn substitute_env_vars(content: &str) -> String {
        // Supports:
        // - ${VAR} - replaced with env var value, empty string if not set
        // - ${VAR:-default} - replaced with env var value, or "default" if not set
        // - ${?VAR} - replaced with env var value if set, empty string if not set (same as ${VAR})
        let re = Regex::new(r"\$\{\??([^}:-]+)(?::-([^}]*))?\}")
            .expect("Hardcoded regex pattern to compile");
        re.replace_all(content, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let default_value = caps.get(2).map_or("", |m| m.as_str());
            std::env::var(var_name).unwrap_or_else(|_| default_value.to_string())
        })
        .to_string()
    }

    pub fn validate(&self) -> Result<()> {
        if self.clusters.is_empty() {
            return Err(KlagError::Config(
                "At least one cluster must be configured".to_string(),
            ));
        }

        for cluster in &self.clusters {
            cluster.validate()?;
        }

        // Validate performance config
        if self.exporter.performance.max_concurrent_groups == 0 {
            return Err(KlagError::Config(
                "performance.max_concurrent_groups must be at least 1".to_string(),
            ));
        }
        if self.exporter.performance.describe_groups_batch_size == 0 {
            return Err(KlagError::Config(
                "performance.describe_groups_batch_size must be at least 1".to_string(),
            ));
        }
        if self.exporter.performance.kafka_timeout.is_zero() {
            return Err(KlagError::Config(
                "performance.kafka_timeout must be greater than 0".to_string(),
            ));
        }
        if self
            .exporter
            .performance
            .compacted_topics_cache_ttl
            .is_zero()
        {
            return Err(KlagError::Config(
                "performance.compacted_topics_cache_ttl must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(KlagError::Config(
                "Cluster name cannot be empty".to_string(),
            ));
        }

        if self.bootstrap_servers.is_empty() {
            return Err(KlagError::Config(format!(
                "Cluster '{}': bootstrap_servers cannot be empty",
                self.name
            )));
        }

        self.compile_filters()?;
        Ok(())
    }

    pub fn compile_filters(&self) -> Result<CompiledFilters> {
        let group_whitelist = self
            .group_whitelist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let group_blacklist = self
            .group_blacklist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let topic_whitelist = self
            .topic_whitelist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let topic_blacklist = self
            .topic_blacklist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(CompiledFilters {
            group_whitelist,
            group_blacklist,
            topic_whitelist,
            topic_blacklist,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompiledFilters {
    pub group_whitelist: Vec<Regex>,
    pub group_blacklist: Vec<Regex>,
    pub topic_whitelist: Vec<Regex>,
    pub topic_blacklist: Vec<Regex>,
}

impl CompiledFilters {
    pub fn matches_group(&self, group: &str) -> bool {
        let matches_whitelist = self.group_whitelist.iter().any(|r| r.is_match(group));
        let matches_blacklist = self.group_blacklist.iter().any(|r| r.is_match(group));
        matches_whitelist && !matches_blacklist
    }

    pub fn matches_topic(&self, topic: &str) -> bool {
        let matches_whitelist = self.topic_whitelist.iter().any(|r| r.is_match(topic));
        let matches_blacklist = self.topic_blacklist.iter().any(|r| r.is_match(topic));
        matches_whitelist && !matches_blacklist
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_loads_from_file() {
        let config_content = r#"
[exporter]
poll_interval = "30s"
http_port = 8000

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(config.exporter.poll_interval, Duration::from_secs(30));
        assert_eq!(config.exporter.http_port, 8000);
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(config.clusters[0].name, "test");
    }

    #[test]
    fn test_config_env_with_default() {
        temp_env::with_var("TEST_NONEXISTENT_VAR", None::<&str>, || {
            let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "${TEST_NONEXISTENT_VAR:-localhost:9092}"
"#;

            let mut file = NamedTempFile::new().expect("create temp file");
            file.write_all(config_content.as_bytes())
                .expect("write config");

            let config = Config::load(Some(file.path().to_str().expect("path to str")))
                .expect("load config");
            assert_eq!(config.clusters[0].bootstrap_servers, "localhost:9092");
        });
    }

    #[test]
    fn test_config_validates_bootstrap_servers() {
        let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = ""
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let result = Config::load(Some(file.path().to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bootstrap_servers cannot be empty"));
    }

    #[test]
    fn test_default_config_values() {
        let config_content = r#"
[exporter]

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(config.exporter.poll_interval, Duration::from_secs(30));
        assert_eq!(config.exporter.http_port, 8000);
        assert_eq!(config.exporter.http_host, "0.0.0.0");
        assert_eq!(config.exporter.granularity, Granularity::Topic);
        assert!(config.exporter.timestamp_sampling.enabled);
        assert!(!config.exporter.otel.enabled);
        assert_eq!(
            config.exporter.performance.kafka_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.exporter.performance.max_concurrent_groups, 10);
        assert_eq!(config.exporter.performance.describe_groups_batch_size, 100);
        assert_eq!(
            config.exporter.performance.compacted_topics_cache_ttl,
            Duration::from_secs(300)
        );
    }

    #[test]
    fn test_performance_config_custom_values() {
        let config_content = r#"
[exporter]
poll_interval = "60s"

[exporter.performance]
kafka_timeout = "15s"
max_concurrent_groups = 20
describe_groups_batch_size = 250
compacted_topics_cache_ttl = "10m"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(
            config.exporter.performance.kafka_timeout,
            Duration::from_secs(15)
        );
        assert_eq!(config.exporter.performance.max_concurrent_groups, 20);
        assert_eq!(config.exporter.performance.describe_groups_batch_size, 250);
        assert_eq!(
            config.exporter.performance.compacted_topics_cache_ttl,
            Duration::from_secs(600)
        );
    }

    proptest! {
        /// Any env var value substitutes correctly via `${VAR}` syntax
        #[test]
        fn prop_env_substitution(
            value in "[a-zA-Z0-9:._-]{1,50}",
        ) {
            let result = temp_env::with_var("PROP_TEST_ENV_VAR", Some(&value), || {
                let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "${PROP_TEST_ENV_VAR}"
"#;
                let mut file = NamedTempFile::new().expect("create temp file");
                file.write_all(config_content.as_bytes()).expect("write");

                let config = Config::load(Some(file.path().to_str().expect("path")))
                    .expect("load config");
                config.clusters[0].bootstrap_servers.clone()
            });
            prop_assert_eq!(result.as_str(), value.as_str());
        }

        /// Any env var value overrides the default in `${VAR:-default}` syntax
        #[test]
        fn prop_env_override_default(
            value in "[a-zA-Z0-9:._-]{1,50}",
        ) {
            let result = temp_env::with_var("PROP_TEST_OVERRIDE", Some(&value), || {
                let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "${PROP_TEST_OVERRIDE:-fallback:9092}"
"#;
                let mut file = NamedTempFile::new().expect("create temp file");
                file.write_all(config_content.as_bytes()).expect("write");

                let config = Config::load(Some(file.path().to_str().expect("path")))
                    .expect("load config");
                config.clusters[0].bootstrap_servers.clone()
            });
            prop_assert_eq!(result.as_str(), value.as_str());
        }

        /// Whitelist regex: group matches iff regex matches
        #[test]
        fn prop_regex_whitelist_match(
            group in "[a-z][a-z0-9-]{0,20}",
        ) {
            let filters = CompiledFilters {
                group_whitelist: vec![Regex::new("^[a-z]").unwrap()],
                group_blacklist: vec![],
                topic_whitelist: vec![Regex::new(".*").unwrap()],
                topic_blacklist: vec![],
            };
            // All generated groups start with [a-z], so they should all match
            prop_assert!(filters.matches_group(&group));
        }

        /// Blacklist regex: group is rejected when blacklist matches
        #[test]
        fn prop_regex_blacklist_reject(
            suffix in "[a-z0-9]{1,20}",
        ) {
            let filters = CompiledFilters {
                group_whitelist: vec![Regex::new(".*").unwrap()],
                group_blacklist: vec![Regex::new("^internal-").unwrap()],
                topic_whitelist: vec![Regex::new(".*").unwrap()],
                topic_blacklist: vec![],
            };
            let internal = format!("internal-{suffix}");
            let external = format!("external-{suffix}");
            prop_assert!(!filters.matches_group(&internal));
            prop_assert!(filters.matches_group(&external));
        }

        /// Positive `max_concurrent_groups` values pass validation
        #[test]
        fn prop_positive_concurrency_accepted(val in 1usize..10_000) {
            let config_content = format!(
                r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
max_concurrent_groups = {val}

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#
            );
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(config_content.as_bytes()).unwrap();
            let result = Config::load(Some(file.path().to_str().unwrap()));
            prop_assert!(result.is_ok(), "max_concurrent_groups={} should be valid", val);
        }

        /// Positive `describe_groups_batch_size` values pass validation
        #[test]
        fn prop_positive_batch_size_accepted(val in 1usize..10_000) {
            let config_content = format!(
                r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
describe_groups_batch_size = {val}

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#
            );
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(config_content.as_bytes()).unwrap();
            let result = Config::load(Some(file.path().to_str().unwrap()));
            prop_assert!(result.is_ok(), "describe_groups_batch_size={} should be valid", val);
        }

        /// Positive `kafka_timeout` values pass validation
        #[test]
        fn prop_positive_kafka_timeout_accepted(secs in 1u64..3600) {
            let config_content = format!(
                r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
kafka_timeout = "{secs}s"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#
            );
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(config_content.as_bytes()).unwrap();
            let result = Config::load(Some(file.path().to_str().unwrap()));
            prop_assert!(result.is_ok(), "kafka_timeout={}s should be valid", secs);
        }

        /// Positive `compacted_topics_cache_ttl` values pass validation
        #[test]
        fn prop_positive_cache_ttl_accepted(secs in 1u64..86400) {
            let config_content = format!(
                r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
compacted_topics_cache_ttl = "{secs}s"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#
            );
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(config_content.as_bytes()).unwrap();
            let result = Config::load(Some(file.path().to_str().unwrap()));
            prop_assert!(result.is_ok(), "compacted_topics_cache_ttl={}s should be valid", secs);
        }
    }
}
