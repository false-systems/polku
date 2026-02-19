//! Lock-free token bucket algorithm
//!
//! Shared implementation used by both [`RateLimiter`](super::RateLimiter) (global)
//! and [`Throttle`](super::Throttle) (per-source). Thread-safe using atomics only
//! — no locks on the hot path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Lock-free token bucket
///
/// Allows `rate` operations per second with a burst capacity of `burst`.
/// Uses 1000x scaling internally for sub-token precision without floating point.
///
/// # Thread Safety
///
/// All operations use compare-and-swap loops — safe for concurrent access
/// from any number of threads without external synchronization.
pub(crate) struct TokenBucket {
    /// Max tokens (scaled by 1000)
    capacity: u64,
    /// Tokens added per refill interval (1000 = 1 token)
    refill_amount: u64,
    /// Nanoseconds between refills
    refill_nanos: u64,
    /// Current token count (scaled by 1000)
    tokens: AtomicU64,
    /// Last refill timestamp (nanos since `start`)
    last_refill: AtomicU64,
    /// Anchor instant for elapsed time
    start: Instant,
}

impl TokenBucket {
    /// Create a new token bucket
    ///
    /// # Arguments
    /// * `rate` - Operations per second (0 = no refill, tokens deplete permanently)
    /// * `burst` - Maximum burst capacity. If 0, no operations are allowed.
    pub(crate) fn new(rate: u64, burst: u64) -> Self {
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

    /// Try to acquire one token
    ///
    /// Returns `true` if acquired, `false` if the bucket is empty.
    pub(crate) fn try_acquire(&self) -> bool {
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

    /// Refill tokens based on elapsed time
    ///
    /// Uses CAS loop to ensure only one thread adds tokens for a given interval.
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn acquires_up_to_burst() {
        let bucket = TokenBucket::new(0, 5); // no refill, burst 5
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn zero_burst_blocks_all() {
        let bucket = TokenBucket::new(1000, 0);
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn concurrent_exact_drain() {
        use std::sync::Arc;
        use std::thread;

        let bucket = Arc::new(TokenBucket::new(0, 100));
        let mut handles = vec![];

        for _ in 0..10 {
            let bucket = Arc::clone(&bucket);
            handles.push(thread::spawn(move || {
                let mut acquired = 0u32;
                for _ in 0..20 {
                    if bucket.try_acquire() {
                        acquired += 1;
                    }
                }
                acquired
            }));
        }

        let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 100);
    }
}
