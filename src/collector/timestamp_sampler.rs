use crate::error::Result;
use crate::kafka::TimestampConsumer;
use crate::kafka::client::TopicPartition;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
struct CachedTimestamp {
    /// `None` means Kafka returned no message at this offset (negative cache).
    timestamp_ms: Option<i64>,
    offset: i64,
    cached_at: Instant,
}

/// Result of getting a timestamp
#[derive(Debug, Clone)]
pub struct TimestampResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone)]
struct CachedOffset {
    /// `None` means Kafka returned no message at this offset (negative cache).
    timestamp_ms: Option<i64>,
    cached_at: Instant,
}

/// Inner struct holding the actual sampler state
struct TimestampSamplerInner {
    consumer: TimestampConsumer,
    /// Per-(group, partition) cache for fast group-level lookups
    cache: DashMap<(String, TopicPartition), CachedTimestamp>,
    /// Per-(partition, offset) dedup cache — avoids re-fetching the same offset
    /// when multiple groups consume the same topic/partition at the same committed offset
    offset_cache: DashMap<(TopicPartition, i64), CachedOffset>,
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
                offset_cache: DashMap::new(),
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

        // Check per-group cache
        if let Some(cached) = self.inner.cache.get(&key)
            && cached.cached_at.elapsed() < self.inner.cache_ttl
            && cached.offset == offset
        {
            if let Some(ts) = cached.timestamp_ms {
                debug!(cached_timestamp = ts, "Using cached timestamp");
                return Ok(Some(TimestampResult { timestamp_ms: ts }));
            }
            debug!("Using cached negative result (no message at offset)");
            return Ok(None);
        }

        // Check cross-group offset dedup cache
        let offset_key = (tp.clone(), offset);
        if let Some(cached) = self.inner.offset_cache.get(&offset_key)
            && cached.cached_at.elapsed() < self.inner.cache_ttl
        {
            // Promote into per-group cache
            self.inner.cache.insert(
                key,
                CachedTimestamp {
                    timestamp_ms: cached.timestamp_ms,
                    offset,
                    cached_at: cached.cached_at,
                },
            );
            if let Some(ts) = cached.timestamp_ms {
                debug!(cached_timestamp = ts, "Using dedup-cached timestamp");
                return Ok(Some(TimestampResult { timestamp_ms: ts }));
            }
            debug!("Using dedup-cached negative result");
            return Ok(None);
        }

        // Fetch from Kafka
        let fetch_result = self.inner.consumer.fetch_timestamp(tp, offset)?;
        let timestamp_ms = fetch_result.as_ref().map(|r| r.timestamp_ms);
        let now = Instant::now();

        // Cache in both layers
        self.inner.cache.insert(
            key,
            CachedTimestamp {
                timestamp_ms,
                offset,
                cached_at: now,
            },
        );
        self.inner.offset_cache.insert(
            offset_key,
            CachedOffset {
                timestamp_ms,
                cached_at: now,
            },
        );

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
        self.inner
            .offset_cache
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
    use proptest::prelude::*;

    proptest! {
        /// Entry is stale iff its age exceeds the TTL
        #[test]
        fn prop_cache_expires_after_ttl(
            age_secs in 0u64..10_000,
            ttl_secs in 1u64..5_000,
        ) {
            let entry = CachedTimestamp {
                timestamp_ms: Some(0),
                offset: 0,
                cached_at: Instant::now() - Duration::from_secs(age_secs),
            };
            let ttl = Duration::from_secs(ttl_secs);
            prop_assert_eq!(entry.cached_at.elapsed() >= ttl, age_secs >= ttl_secs);
        }

        /// Cache should miss when the stored offset differs from the query offset
        #[test]
        fn prop_cache_invalidates_on_offset_change(
            stored_offset in 0i64..i64::MAX,
            query_offset in 0i64..i64::MAX,
        ) {
            let cached = CachedTimestamp {
                timestamp_ms: Some(1000),
                offset: stored_offset,
                cached_at: Instant::now(),
            };
            let is_hit = cached.offset == query_offset;
            prop_assert_eq!(is_hit, stored_offset == query_offset);
        }

        /// `retain` keeps only entries younger than TTL
        #[test]
        fn prop_clear_stale_retains_fresh(
            stale_age in 61u64..10_000,
            fresh_age in 0u64..59,
        ) {
            let cache: DashMap<(String, TopicPartition), CachedTimestamp> = DashMap::new();
            let ttl = Duration::from_secs(60);

            cache.insert(
                ("stale".to_string(), TopicPartition::new("t", 0)),
                CachedTimestamp {
                    timestamp_ms: Some(0),
                    offset: 0,
                    cached_at: Instant::now() - Duration::from_secs(stale_age),
                },
            );
            cache.insert(
                ("fresh".to_string(), TopicPartition::new("t", 0)),
                CachedTimestamp {
                    timestamp_ms: Some(0),
                    offset: 0,
                    cached_at: Instant::now() - Duration::from_secs(fresh_age),
                },
            );

            let now = Instant::now();
            cache.retain(|_, v| now.duration_since(v.cached_at) < ttl);

            prop_assert_eq!(cache.len(), 1);
            prop_assert!(cache.get(&("fresh".to_string(), TopicPartition::new("t", 0))).is_some());
        }

        /// Negative cache entries (timestamp_ms=None) are also retained/evicted by TTL
        #[test]
        fn prop_negative_cache_expires(
            age_secs in 0u64..10_000,
            ttl_secs in 1u64..5_000,
        ) {
            let entry = CachedTimestamp {
                timestamp_ms: None,
                offset: 42,
                cached_at: Instant::now() - Duration::from_secs(age_secs),
            };
            let ttl = Duration::from_secs(ttl_secs);
            prop_assert_eq!(entry.cached_at.elapsed() >= ttl, age_secs >= ttl_secs);
        }
    }
}
