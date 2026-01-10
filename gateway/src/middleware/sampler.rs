//! Probabilistic sampler middleware
//!
//! Passes through a configurable percentage of messages.
//! Uses fast xorshift PRNG - lock-free, no allocations.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

/// Probabilistic sampler
///
/// Samples messages with given probability (0.0 to 1.0).
/// Thread-safe using atomic xorshift64 PRNG.
pub struct Sampler {
    /// Threshold for sampling (0 = none, u64::MAX = all)
    threshold: u64,
    /// PRNG state
    state: AtomicU64,
    /// Count of messages dropped by sampling
    dropped: AtomicU64,
}

impl Sampler {
    /// Create a sampler with given rate (0.0 to 1.0)
    ///
    /// # Arguments
    /// * `rate` - Probability of keeping a message (0.0 = drop all, 1.0 = keep all)
    ///
    /// # Panics
    /// Panics if rate is not in [0.0, 1.0]
    ///
    /// # Note
    /// Seed is derived from system time. If system clock is before UNIX_EPOCH
    /// (misconfigured), falls back to a fixed seed. Use `with_seed` for
    /// deterministic behavior in tests.
    pub fn new(rate: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&rate),
            "sample rate must be between 0.0 and 1.0"
        );

        // Convert rate to threshold
        let threshold = if rate >= 1.0 {
            u64::MAX
        } else {
            (rate * u64::MAX as f64) as u64
        };

        // Seed from system time (fallback to fixed seed if clock is misconfigured)
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xDEADBEEF);

        Self {
            threshold,
            state: AtomicU64::new(seed | 1), // Ensure non-zero for xorshift
            dropped: AtomicU64::new(0),
        }
    }

    /// Create sampler with explicit seed (for testing)
    pub fn with_seed(rate: f64, seed: u64) -> Self {
        let mut sampler = Self::new(rate);
        sampler.state = AtomicU64::new(seed | 1);
        sampler
    }

    /// Get the count of dropped messages
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Generate next random number (xorshift64)
    ///
    /// Lock-free CAS loop. Under high contention, threads may retry
    /// but progress is always made (lock-free guarantee).
    fn next_random(&self) -> u64 {
        loop {
            // Load current state ONCE
            let old = self.state.load(Ordering::Acquire);

            // Compute next state from the loaded value
            let mut x = old;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;

            // CAS: if state unchanged, update it; otherwise retry
            if self
                .state
                .compare_exchange_weak(old, x, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return x;
            }
            // Another thread updated state; loop will reload and retry
        }
    }

    /// Check if message should be sampled (kept)
    pub fn should_sample(&self) -> bool {
        self.next_random() <= self.threshold
    }
}

#[async_trait]
impl Middleware for Sampler {
    fn name(&self) -> &'static str {
        "sampler"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        if self.should_sample() {
            Some(msg)
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::trace!(
                id = %msg.id,
                "sampled out"
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
    async fn test_sampler_always_pass() {
        let sampler = Sampler::new(1.0);

        for _ in 0..100 {
            let msg = Message::new("test", "evt", Bytes::new());
            assert!(sampler.process(msg).await.is_some());
        }
    }

    #[tokio::test]
    async fn test_sampler_always_drop() {
        let sampler = Sampler::new(0.0);

        for _ in 0..100 {
            let msg = Message::new("test", "evt", Bytes::new());
            assert!(sampler.process(msg).await.is_none());
        }
    }

    #[tokio::test]
    async fn test_sampler_rate_approximate() {
        let sampler = Sampler::with_seed(0.5, 42);
        let mut passed = 0;
        let total = 10000;

        for _ in 0..total {
            let msg = Message::new("test", "evt", Bytes::new());
            if sampler.process(msg).await.is_some() {
                passed += 1;
            }
        }

        // Should be roughly 50% (allow 10% variance)
        let ratio = passed as f64 / total as f64;
        assert!(
            (0.40..=0.60).contains(&ratio),
            "expected ~50%, got {:.1}%",
            ratio * 100.0
        );
    }

    #[tokio::test]
    async fn test_sampler_10_percent() {
        let sampler = Sampler::with_seed(0.1, 12345);
        let mut passed = 0;
        let total = 10000;

        for _ in 0..total {
            let msg = Message::new("test", "evt", Bytes::new());
            if sampler.process(msg).await.is_some() {
                passed += 1;
            }
        }

        // Should be roughly 10% (allow 5% variance)
        let ratio = passed as f64 / total as f64;
        assert!(
            (0.05..=0.15).contains(&ratio),
            "expected ~10%, got {:.1}%",
            ratio * 100.0
        );
    }

    #[test]
    fn test_sampler_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let sampler = Arc::new(Sampler::with_seed(0.5, 999));
        let mut handles = vec![];

        for _ in 0..4 {
            let sampler = Arc::clone(&sampler);
            handles.push(thread::spawn(move || {
                let mut passed = 0;
                for _ in 0..1000 {
                    if sampler.should_sample() {
                        passed += 1;
                    }
                }
                passed
            }));
        }

        let total_passed: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // 4 threads * 1000 samples = 4000 total
        // At 50% rate, expect ~2000 (allow 20% variance)
        assert!(
            (1600..=2400).contains(&total_passed),
            "expected ~2000, got {}",
            total_passed
        );
    }

    #[test]
    #[should_panic(expected = "sample rate must be between")]
    fn test_sampler_invalid_rate_high() {
        let _ = Sampler::new(1.5);
    }

    #[test]
    #[should_panic(expected = "sample rate must be between")]
    fn test_sampler_invalid_rate_negative() {
        let _ = Sampler::new(-0.1);
    }
}
