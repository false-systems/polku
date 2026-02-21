//! Token bucket rate limiter middleware
//!
//! Limits message throughput using the token bucket algorithm.
//! Thread-safe, lock-free, O(1) per message.

use super::token_bucket::TokenBucket;
use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

/// Token bucket rate limiter
///
/// Allows `rate` messages per second with a burst capacity of `burst` messages.
/// Implemented as a continuously refilling token bucket.
/// Thread-safe using atomics - no locks on hot path.
pub struct RateLimiter {
    bucket: TokenBucket,
    /// Count of messages dropped due to rate limiting
    dropped: AtomicU64,
}

impl RateLimiter {
    /// Create a new rate limiter
    ///
    /// # Arguments
    /// * `rate` - Messages per second allowed (0 = no refill)
    /// * `burst` - Max burst size (bucket capacity). If 0, no messages allowed.
    pub fn new(rate: u64, burst: u64) -> Self {
        Self {
            bucket: TokenBucket::new(rate, burst),
            dropped: AtomicU64::new(0),
        }
    }

    /// Get the count of dropped messages
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Try to acquire a token
    ///
    /// Returns true if token acquired, false if rate limited.
    pub fn try_acquire(&self) -> bool {
        self.bucket.try_acquire()
    }
}

#[async_trait]
impl Middleware for RateLimiter {
    fn name(&self) -> &str {
        "rate_limiter"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        if self.try_acquire() {
            Some(msg)
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                source = %msg.source,
                message_type = %msg.message_type,
                "rate limited"
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
    async fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(100, 10); // 100/s, burst 10
        let msg = Message::new("test", "evt", Bytes::new());

        // Should allow first message
        let result = limiter.process(msg).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_rate_limiter_blocks_over_burst() {
        let limiter = RateLimiter::new(100, 2); // 100/s, burst 2

        // Consume burst
        for _ in 0..2 {
            let msg = Message::new("test", "evt", Bytes::new());
            assert!(limiter.process(msg).await.is_some());
        }

        // Third should be blocked
        let msg = Message::new("test", "evt", Bytes::new());
        let result = limiter.process(msg).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_rate_limiter_refills() {
        let limiter = RateLimiter::new(1000, 1); // 1000/s, burst 1

        // Consume the burst
        let msg = Message::new("test", "evt", Bytes::new());
        assert!(limiter.process(msg).await.is_some());

        // Immediately blocked
        let msg = Message::new("test", "evt", Bytes::new());
        assert!(limiter.process(msg).await.is_none());

        // Wait for refill (2ms for 1000/s rate)
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;

        // Should be allowed again
        let msg = Message::new("test", "evt", Bytes::new());
        assert!(limiter.process(msg).await.is_some());
    }

    #[test]
    fn test_try_acquire_concurrent() {
        use std::sync::Arc;
        use std::thread;

        // Use rate=0 (no refill) for deterministic test
        let limiter = Arc::new(RateLimiter::new(0, 100));
        let mut handles = vec![];

        // Spawn threads to compete for tokens
        for _ in 0..10 {
            let limiter = Arc::clone(&limiter);
            handles.push(thread::spawn(move || {
                let mut acquired = 0;
                for _ in 0..20 {
                    if limiter.try_acquire() {
                        acquired += 1;
                    }
                }
                acquired
            }));
        }

        let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // With no refill, should acquire exactly 100 tokens (the burst)
        assert_eq!(total, 100, "expected exactly 100, acquired: {}", total);
    }

    #[test]
    fn test_zero_burst_blocks_all() {
        let limiter = RateLimiter::new(1000, 0);
        assert!(!limiter.try_acquire());
    }

    #[tokio::test]
    async fn test_rate_limiter_dropped_count() {
        let limiter = RateLimiter::new(100, 2); // burst 2

        // First two pass
        for _ in 0..2 {
            let msg = Message::new("test", "evt", Bytes::new());
            assert!(limiter.process(msg).await.is_some());
        }
        assert_eq!(limiter.dropped_count(), 0);

        // Next 3 get dropped
        for _ in 0..3 {
            let msg = Message::new("test", "evt", Bytes::new());
            assert!(limiter.process(msg).await.is_none());
        }
        assert_eq!(limiter.dropped_count(), 3);
    }
}
