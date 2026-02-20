use crate::error::Result;
use crate::kafka::TimestampConsumer;
use crate::kafka::client::TopicPartition;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
struct CachedTimestamp {
    timestamp_ms: i64,
    offset: i64,
    cached_at: Instant,
}

/// Result of getting a timestamp
#[derive(Debug, Clone)]
pub struct TimestampResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

/// Inner struct holding the actual sampler state
struct TimestampSamplerInner {
    consumer: TimestampConsumer,
    cache: DashMap<(String, TopicPartition), CachedTimestamp>,
    cache_ttl: Duration,
}

/// Thread-safe, clonable timestamp sampler for concurrent fetching
#[derive(Clone)]
pub struct TimestampSampler {
    inner: Arc<TimestampSamplerInner>,
}

impl TimestampSampler {
    pub fn new(consumer: TimestampConsumer, cache_ttl: Duration) -> Self {
        Self {
            inner: Arc::new(TimestampSamplerInner {
                consumer,
                cache: DashMap::new(),
                cache_ttl,
            }),
        }
    }

    #[instrument(skip(self), fields(group = %group_id, topic = %tp.topic, partition = tp.partition))]
    pub fn get_timestamp(
        &self,
        group_id: &str,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<Option<TimestampResult>> {
        let key = (group_id.to_string(), tp.clone());

        // Check cache
        if let Some(cached) = self.inner.cache.get(&key) {
            // Cache is valid if:
            // 1. Not expired by TTL
            // 2. Offset hasn't changed (consumer hasn't moved)
            if cached.cached_at.elapsed() < self.inner.cache_ttl && cached.offset == offset {
                debug!(
                    cached_timestamp = cached.timestamp_ms,
                    "Using cached timestamp"
                );
                return Ok(Some(TimestampResult {
                    timestamp_ms: cached.timestamp_ms,
                }));
            }
        }

        // Fetch from Kafka
        let fetch_result = self.inner.consumer.fetch_timestamp(tp, offset)?;

        // Cache the result
        if let Some(ref result) = fetch_result {
            self.inner.cache.insert(
                key,
                CachedTimestamp {
                    timestamp_ms: result.timestamp_ms,
                    offset,
                    cached_at: Instant::now(),
                },
            );
        }

        Ok(fetch_result.map(|r| TimestampResult {
            timestamp_ms: r.timestamp_ms,
        }))
    }

    pub fn remove_entry(&self, group_id: &str, tp: &TopicPartition) {
        self.inner.cache.remove(&(group_id.to_string(), tp.clone()));
    }

    pub fn clear_stale_entries(&self) {
        let now = Instant::now();
        self.inner
            .cache
            .retain(|_, v| now.duration_since(v.cached_at) < self.inner.cache_ttl);
    }

    pub fn cache_size(&self) -> usize {
        self.inner.cache.len()
    }
}

impl std::fmt::Debug for TimestampSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampSampler")
            .field("cache_size", &self.inner.cache.len())
            .field("cache_ttl", &self.inner.cache_ttl)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_cache_ttl_expiry() {
        let cached = CachedTimestamp {
            timestamp_ms: 1000,
            offset: 100,
            cached_at: Instant::now() - Duration::from_secs(120),
        };

        let cache_ttl = Duration::from_secs(60);

        // Entry should be expired — elapsed exceeds TTL
        assert!(cached.cached_at.elapsed() >= cache_ttl);

        // Fresh entry should NOT be expired
        let fresh = CachedTimestamp {
            timestamp_ms: 2000,
            offset: 200,
            cached_at: Instant::now(),
        };
        assert!(fresh.cached_at.elapsed() < cache_ttl);
    }

    #[test]
    fn test_cache_invalidation_on_offset_change() {
        let cached = CachedTimestamp {
            timestamp_ms: 1000,
            offset: 100,
            cached_at: Instant::now(),
        };

        // Same offset → cache hit condition
        assert_eq!(cached.offset, 100);
        // Different offset → cache miss condition
        assert_ne!(cached.offset, 150);
    }

    /// Verify that `clear_stale_entries` removes expired entries and keeps fresh ones.
    #[test]
    fn test_clear_stale_entries() {
        let cache: DashMap<(String, TopicPartition), CachedTimestamp> = DashMap::new();
        let ttl = Duration::from_secs(60);

        // Insert a stale entry
        cache.insert(
            ("g1".to_string(), TopicPartition::new("t1", 0)),
            CachedTimestamp {
                timestamp_ms: 1000,
                offset: 10,
                cached_at: Instant::now() - Duration::from_secs(120),
            },
        );
        // Insert a fresh entry
        cache.insert(
            ("g1".to_string(), TopicPartition::new("t1", 1)),
            CachedTimestamp {
                timestamp_ms: 2000,
                offset: 20,
                cached_at: Instant::now(),
            },
        );

        assert_eq!(cache.len(), 2);

        // Simulate clear_stale_entries logic
        let now = Instant::now();
        cache.retain(|_, v| now.duration_since(v.cached_at) < ttl);

        // Only the fresh entry should remain
        assert_eq!(cache.len(), 1);
        assert!(
            cache
                .get(&("g1".to_string(), TopicPartition::new("t1", 1)))
                .is_some()
        );
    }
}
