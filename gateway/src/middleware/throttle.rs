//! Per-source throttle middleware
//!
//! Like RateLimiter but maintains separate limits per source.
//! Useful when different sources have different throughput needs.
//!
//! # Memory Management
//!
//! To prevent unbounded memory growth from ephemeral sources, buckets are
//! evicted LRU-style when capacity is exceeded. Eviction is batched (10% at
//! a time) to amortize the O(n) scan cost. Configure `max_sources` based on
//! your expected source cardinality.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Per-source rate limiter
///
/// Each source gets its own token bucket with the configured rate/burst.
/// New sources are allocated a bucket on first message.
///
/// # Memory Bounds
///
/// Set `max_sources` to limit memory usage. When exceeded, the oldest 10%
/// of buckets are evicted in batch to amortize the O(n) scan cost.
/// Default is 10,000 sources.
pub struct Throttle {
    /// Rate per second for each source
    rate: u64,
    /// Burst capacity for each source
    burst: u64,
    /// Maximum number of sources to track (LRU eviction when exceeded)
    max_sources: usize,
    /// Per-source buckets with last-access time
    buckets: RwLock<HashMap<String, TrackedBucket>>,
    /// Count of messages dropped due to rate limiting
    dropped: AtomicU64,
}

/// Token bucket with last-access tracking for LRU eviction
struct TrackedBucket {
    bucket: TokenBucket,
    last_access_nanos: AtomicU64,
    created: Instant,
}

/// Individual token bucket (similar to RateLimiter internals)
struct TokenBucket {
    capacity: u64,
    refill_amount: u64,
    refill_nanos: u64,
    tokens: AtomicU64,
    last_refill: AtomicU64,
    start: Instant,
}

impl TokenBucket {
    fn new(rate: u64, burst: u64) -> Self {
        let refill_nanos = if rate == 0 {
            u64::MAX
        } else {
            1_000_000_000 / rate
        };

        let scaled_burst = burst.saturating_mul(1000);

        Self {
            capacity: scaled_burst,
            refill_amount: 1000,
            refill_nanos,
            tokens: AtomicU64::new(scaled_burst),
            last_refill: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    fn try_acquire(&self) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < 1000 {
                return false;
            }

            if self
                .tokens
                .compare_exchange_weak(current, current - 1000, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn refill(&self) {
        let now_nanos = self.start.elapsed().as_nanos() as u64;

        loop {
            let last = self.last_refill.load(Ordering::Acquire);
            let elapsed = now_nanos.saturating_sub(last);

            if elapsed < self.refill_nanos {
                return;
            }

            let intervals = elapsed / self.refill_nanos;
            if intervals == 0 {
                return;
            }

            let new_last = last + intervals * self.refill_nanos;

            match self.last_refill.compare_exchange_weak(
                last,
                new_last,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let tokens_to_add = intervals * self.refill_amount;
                    if tokens_to_add == 0 {
                        return;
                    }

                    loop {
                        let current = self.tokens.load(Ordering::Acquire);
                        let new_tokens = (current.saturating_add(tokens_to_add)).min(self.capacity);
                        if current == new_tokens {
                            break;
                        }
                        if self
                            .tokens
                            .compare_exchange_weak(
                                current,
                                new_tokens,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                    return;
                }
                Err(_) => continue,
            }
        }
    }
}

impl TrackedBucket {
    fn new(rate: u64, burst: u64) -> Self {
        let now = Instant::now();
        Self {
            bucket: TokenBucket::new(rate, burst),
            last_access_nanos: AtomicU64::new(0),
            created: now,
        }
    }

    fn try_acquire(&self) -> bool {
        // Update last access time
        let elapsed = self.created.elapsed().as_nanos() as u64;
        self.last_access_nanos.store(elapsed, Ordering::Relaxed);
        self.bucket.try_acquire()
    }

    fn last_access(&self) -> u64 {
        self.last_access_nanos.load(Ordering::Relaxed)
    }
}

impl Throttle {
    /// Create a new per-source throttle
    ///
    /// Each source gets its own bucket with the specified rate and burst.
    /// Default max_sources is 10,000.
    pub fn new(rate: u64, burst: u64) -> Self {
        Self {
            rate,
            burst,
            max_sources: 10_000,
            buckets: RwLock::new(HashMap::new()),
            dropped: AtomicU64::new(0),
        }
    }

    /// Set the maximum number of sources to track
    ///
    /// When exceeded, the least recently used bucket is evicted.
    pub fn max_sources(mut self, max: usize) -> Self {
        self.max_sources = max;
        self
    }

    /// Get or create a bucket for the given source
    fn get_or_create_bucket(&self, source: &str) -> bool {
        // Fast path: check if bucket exists (read lock)
        {
            let buckets = self.buckets.read();
            if let Some(tracked) = buckets.get(source) {
                return tracked.try_acquire();
            }
        }

        // Slow path: create new bucket (write lock)
        {
            let mut buckets = self.buckets.write();

            // Double-check after acquiring write lock
            if let Some(tracked) = buckets.get(source) {
                return tracked.try_acquire();
            }

            // Evict LRU buckets in batch if at capacity
            if buckets.len() >= self.max_sources {
                self.evict_lru_batch(&mut buckets);
            }

            // Insert new bucket
            let tracked = TrackedBucket::new(self.rate, self.burst);
            let result = tracked.try_acquire();
            buckets.insert(source.to_string(), tracked);
            result
        }
    }

    /// Evict the least recently used buckets in batch
    ///
    /// Evicts 10% of buckets (min 1) to amortize the O(n) scan cost.
    /// This means we don't scan on every insert, only when over capacity.
    fn evict_lru_batch(&self, buckets: &mut HashMap<String, TrackedBucket>) {
        if buckets.is_empty() {
            return;
        }

        // Evict 10% of max_sources, minimum 1
        let evict_count = (self.max_sources / 10).max(1);

        // Collect all sources with their last access times
        let mut sources_by_access: Vec<_> = buckets
            .iter()
            .map(|(source, tracked)| (source.clone(), tracked.last_access()))
            .collect();

        // Sort by access time (oldest first)
        sources_by_access.sort_by_key(|(_, access)| *access);

        // Evict the oldest N
        let to_evict = sources_by_access.into_iter().take(evict_count);
        for (source, _) in to_evict {
            tracing::debug!(source = %source, "evicting LRU throttle bucket");
            buckets.remove(&source);
        }
    }

    /// Get the number of tracked sources
    pub fn source_count(&self) -> usize {
        self.buckets.read().len()
    }

    /// Get the maximum number of sources
    pub fn max_sources_limit(&self) -> usize {
        self.max_sources
    }

    /// Get the count of dropped messages
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Middleware for Throttle {
    fn name(&self) -> &str {
        "throttle"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        if self.get_or_create_bucket(&msg.source) {
            Some(msg)
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                source = %msg.source,
                message_type = %msg.message_type,
                "throttled"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_throttle_allows_within_limit() {
        let throttle = Throttle::new(100, 10);
        let msg = Message::new("source-a", "evt", Bytes::new());

        let result = throttle.process(msg).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_throttle_blocks_over_burst() {
        let throttle = Throttle::new(100, 2); // 100/s, burst 2

        // Consume burst for source-a
        for _ in 0..2 {
            let msg = Message::new("source-a", "evt", Bytes::new());
            assert!(throttle.process(msg).await.is_some());
        }

        // Third from source-a should be blocked
        let msg = Message::new("source-a", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_none());
    }

    #[tokio::test]
    async fn test_throttle_independent_per_source() {
        let throttle = Throttle::new(100, 2); // 100/s, burst 2

        // Exhaust source-a
        for _ in 0..2 {
            let msg = Message::new("source-a", "evt", Bytes::new());
            assert!(throttle.process(msg).await.is_some());
        }
        let msg = Message::new("source-a", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_none());

        // source-b should still work (separate bucket)
        let msg = Message::new("source-b", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_some());

        // Verify we have 2 sources tracked
        assert_eq!(throttle.source_count(), 2);
    }

    #[tokio::test]
    async fn test_throttle_refills() {
        let throttle = Throttle::new(1000, 1); // 1000/s, burst 1

        // Consume the burst
        let msg = Message::new("source-a", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_some());

        // Immediately blocked
        let msg = Message::new("source-a", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_none());

        // Wait for refill
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;

        // Should be allowed again
        let msg = Message::new("source-a", "evt", Bytes::new());
        assert!(throttle.process(msg).await.is_some());
    }

    #[tokio::test]
    async fn test_throttle_concurrent_sources() {
        use std::sync::Arc;

        let throttle = Arc::new(Throttle::new(100, 5));
        let mut handles = vec![];

        // 10 concurrent sources
        for i in 0..10 {
            let throttle = Arc::clone(&throttle);
            handles.push(tokio::spawn(async move {
                let source = format!("source-{}", i);
                let mut passed = 0;
                for _ in 0..10 {
                    let msg = Message::new(source.as_str(), "evt", Bytes::new());
                    if throttle.process(msg).await.is_some() {
                        passed += 1;
                    }
                }
                passed
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Each source should pass exactly 5 (their burst)
        for (i, passed) in results.iter().enumerate() {
            assert_eq!(*passed, 5, "source-{} passed {} instead of 5", i, passed);
        }

        // All 10 sources should be tracked
        assert_eq!(throttle.source_count(), 10);
    }

    #[tokio::test]
    async fn test_throttle_lru_eviction() {
        // max_sources = 10, so 11th source should trigger batch eviction (10% = 1)
        let throttle = Throttle::new(100, 10).max_sources(10);

        // Add 10 sources
        for i in 0..10 {
            let msg = Message::new(format!("source-{}", i), "evt", Bytes::new());
            throttle.process(msg).await;
        }
        assert_eq!(throttle.source_count(), 10);

        // Wait a bit, then access sources 5-9 to update their LRU time
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        for i in 5..10 {
            let msg = Message::new(format!("source-{}", i), "evt", Bytes::new());
            throttle.process(msg).await;
        }

        // Add an 11th source - should trigger batch eviction (1 source evicted)
        let msg = Message::new("source-10", "evt", Bytes::new());
        throttle.process(msg).await;

        // Should have evicted 1 (10% of 10), so 10 remain
        assert_eq!(throttle.source_count(), 10);

        // source-0 was oldest, so it should be evicted
        // Adding it again will create a new bucket (triggering another eviction)
        let msg = Message::new("source-0", "evt", Bytes::new());
        throttle.process(msg).await;

        // Still 10 after another eviction
        assert_eq!(throttle.source_count(), 10);
    }

    #[test]
    fn test_throttle_max_sources_builder() {
        let throttle = Throttle::new(100, 10).max_sources(500);
        assert_eq!(throttle.max_sources_limit(), 500);
    }

    #[tokio::test]
    async fn test_throttle_dropped_count() {
        let throttle = Throttle::new(100, 2); // burst 2

        // First two pass
        for _ in 0..2 {
            let msg = Message::new("source-a", "evt", Bytes::new());
            assert!(throttle.process(msg).await.is_some());
        }
        assert_eq!(throttle.dropped_count(), 0);

        // Next 3 get dropped
        for _ in 0..3 {
            let msg = Message::new("source-a", "evt", Bytes::new());
            assert!(throttle.process(msg).await.is_none());
        }
        assert_eq!(throttle.dropped_count(), 3);
    }
}
