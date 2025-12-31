//! Retry emitter with exponential backoff
//!
//! Wraps an emitter to retry failed emissions with configurable backoff.

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::proto::Event;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Lock-free xorshift64 PRNG for jitter randomness
///
/// Uses atomic compare-exchange for thread-safe operation without locks.
/// Provides better distribution than naive system time approaches.
struct Xorshift64 {
    state: AtomicU64,
}

impl Xorshift64 {
    /// Create with seed from system time
    fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x853c49e6748fea9b); // Fallback seed
        // Ensure non-zero seed
        let seed = if seed == 0 { 0x853c49e6748fea9b } else { seed };
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Generate next random u64 using xorshift64 algorithm
    fn next(&self) -> u64 {
        loop {
            let old = self.state.load(Ordering::Acquire);
            let mut x = old;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if self
                .state
                .compare_exchange_weak(old, x, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return x;
            }
        }
    }

    /// Generate random f64 in range [0.0, 1.0)
    fn next_f64(&self) -> f64 {
        (self.next() as f64) / (u64::MAX as f64)
    }
}

/// Configuration for exponential backoff with jitter
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before first retry (e.g., 100ms)
    pub initial_delay: Duration,
    /// Maximum delay between retries (e.g., 30s)
    pub max_delay: Duration,
    /// Multiplier for each retry (e.g., 2.0 for doubling)
    pub multiplier: f64,
    /// Maximum number of retry attempts (0 = no retries, just initial attempt)
    pub max_attempts: u32,
    /// Jitter factor (0.0-1.0) - randomizes delay by +/- this percentage
    pub jitter_factor: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 3,
            jitter_factor: 0.25,
        }
    }
}

impl BackoffConfig {
    /// Calculate delay for attempt n (0-indexed)
    ///
    /// Attempt 0 returns zero (no delay for first try).
    /// Subsequent attempts use exponential backoff with jitter.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        self.delay_for_attempt_with_jitter(attempt, rand_jitter())
    }

    /// Calculate delay with explicit jitter value (for testing)
    pub fn delay_for_attempt_with_jitter(&self, attempt: u32, jitter: f64) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Use microseconds for precision with small delays
        let base_us =
            self.initial_delay.as_micros() as f64 * self.multiplier.powi((attempt - 1) as i32);
        let base_us = base_us.min(self.max_delay.as_micros() as f64);

        // Apply jitter: delay * (1 +/- jitter_factor * random)
        // jitter is in range [0.0, 1.0], we map to [-1.0, 1.0]
        let jitter_range = base_us * self.jitter_factor;
        let jitter_offset = (jitter * 2.0 - 1.0) * jitter_range;
        let final_us = (base_us + jitter_offset).max(1.0); // Minimum 1 microsecond

        Duration::from_micros(final_us as u64)
    }
}

/// Thread-local xorshift64 PRNG for jitter
///
/// Using lazy_static would add a dependency, so we use a simple static.
/// Each call advances the PRNG state atomically.
static JITTER_RNG: std::sync::LazyLock<Xorshift64> = std::sync::LazyLock::new(Xorshift64::new);

/// Generate random jitter value in range [0.0, 1.0)
///
/// Uses xorshift64 algorithm for proper randomness distribution.
fn rand_jitter() -> f64 {
    JITTER_RNG.next_f64()
}

/// Emitter wrapper that retries on failure with exponential backoff
pub struct RetryEmitter {
    inner: Arc<dyn Emitter>,
    config: BackoffConfig,
    /// Metrics: total retry attempts
    retry_count: AtomicU64,
    /// Metrics: successful retries (recovered from failure)
    recovered_count: AtomicU64,
}

impl RetryEmitter {
    /// Create a new RetryEmitter with the given configuration
    pub fn new(inner: Arc<dyn Emitter>, config: BackoffConfig) -> Self {
        Self {
            inner,
            config,
            retry_count: AtomicU64::new(0),
            recovered_count: AtomicU64::new(0),
        }
    }

    /// Create a RetryEmitter with default configuration
    pub fn with_defaults(inner: Arc<dyn Emitter>) -> Self {
        Self::new(inner, BackoffConfig::default())
    }

    /// Get total retry attempts
    pub fn retry_count(&self) -> u64 {
        self.retry_count.load(Ordering::Relaxed)
    }

    /// Get count of successful recoveries after failure
    pub fn recovered_count(&self) -> u64 {
        self.recovered_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Emitter for RetryEmitter {
    fn name(&self) -> &'static str {
        "retry"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_attempts {
            // Wait before retry (attempt 0 has no delay)
            let delay = self.config.delay_for_attempt(attempt);
            if !delay.is_zero() {
                self.retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    emitter = self.inner.name(),
                    attempt = attempt,
                    delay_ms = delay.as_millis() as u64,
                    "retrying emission"
                );
                tokio::time::sleep(delay).await;
            }

            match self.inner.emit(events).await {
                Ok(()) => {
                    if attempt > 0 {
                        self.recovered_count.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(
                            emitter = self.inner.name(),
                            attempt = attempt,
                            "emission recovered after retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        emitter = self.inner.name(),
                        attempt = attempt,
                        max_attempts = self.config.max_attempts,
                        error = %e,
                        "emission failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| PluginError::Send("all retries exhausted".into())))
    }

    async fn health(&self) -> bool {
        self.inner.health().await
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        self.inner.shutdown().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// Mock emitter that fails N times then succeeds
    struct FailingEmitter {
        fail_count: AtomicU32,
        max_failures: u32,
        call_count: AtomicU32,
    }

    impl FailingEmitter {
        fn new(max_failures: u32) -> Self {
            Self {
                fail_count: AtomicU32::new(0),
                max_failures,
                call_count: AtomicU32::new(0),
            }
        }

        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Emitter for FailingEmitter {
        fn name(&self) -> &'static str {
            "failing"
        }

        async fn emit(&self, _: &[Event]) -> Result<(), PluginError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let count = self.fail_count.fetch_add(1, Ordering::SeqCst);
            if count < self.max_failures {
                Err(PluginError::Connection("simulated failure".into()))
            } else {
                Ok(())
            }
        }

        async fn health(&self) -> bool {
            true
        }
    }

    fn make_test_event() -> Event {
        Event {
            id: "test-1".to_string(),
            timestamp_unix_ns: 0,
            source: "test".to_string(),
            event_type: "test".to_string(),
            metadata: std::collections::HashMap::new(),
            payload: vec![],
            route_to: vec![],
        }
    }

    #[test]
    fn test_backoff_attempt_zero_is_zero() {
        let config = BackoffConfig::default();
        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
    }

    #[test]
    fn test_backoff_exponential_growth() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_factor: 0.0, // No jitter for deterministic test
            max_attempts: 10,
        };

        // With jitter=0.5 (middle), no jitter applied
        assert_eq!(
            config.delay_for_attempt_with_jitter(1, 0.5),
            Duration::from_millis(100)
        );
        assert_eq!(
            config.delay_for_attempt_with_jitter(2, 0.5),
            Duration::from_millis(200)
        );
        assert_eq!(
            config.delay_for_attempt_with_jitter(3, 0.5),
            Duration::from_millis(400)
        );
        assert_eq!(
            config.delay_for_attempt_with_jitter(4, 0.5),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn test_backoff_caps_at_max() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            multiplier: 2.0,
            jitter_factor: 0.0,
            max_attempts: 10,
        };

        // attempt 4: 100 * 2^3 = 800ms, should cap at 500ms
        assert_eq!(
            config.delay_for_attempt_with_jitter(4, 0.5),
            Duration::from_millis(500)
        );
        assert_eq!(
            config.delay_for_attempt_with_jitter(10, 0.5),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_backoff_jitter_range() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_factor: 0.25,
            max_attempts: 10,
        };

        // At attempt 1, base is 100ms, jitter range is +/- 25ms
        // jitter=0.0 -> -25ms -> 75ms
        let min = config.delay_for_attempt_with_jitter(1, 0.0);
        assert_eq!(min, Duration::from_millis(75));

        // jitter=1.0 -> +25ms -> 125ms
        let max = config.delay_for_attempt_with_jitter(1, 1.0);
        assert_eq!(max, Duration::from_millis(125));
    }

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let inner = Arc::new(FailingEmitter::new(0)); // Never fails
        let retry = RetryEmitter::new(
            inner.clone(),
            BackoffConfig {
                max_attempts: 3,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            },
        );

        let result = retry.emit(&[make_test_event()]).await;

        assert!(result.is_ok());
        assert_eq!(inner.calls(), 1);
        assert_eq!(retry.retry_count(), 0);
        assert_eq!(retry.recovered_count(), 0);
    }

    #[tokio::test]
    async fn test_retry_recovers_from_transient_failure() {
        let inner = Arc::new(FailingEmitter::new(2)); // Fails twice
        let retry = RetryEmitter::new(
            inner.clone(),
            BackoffConfig {
                max_attempts: 3,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(10),
                ..Default::default()
            },
        );

        let result = retry.emit(&[make_test_event()]).await;

        assert!(result.is_ok());
        assert_eq!(inner.calls(), 3); // 2 failures + 1 success
        assert_eq!(retry.retry_count(), 2);
        assert_eq!(retry.recovered_count(), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let inner = Arc::new(FailingEmitter::new(10)); // Always fails
        let retry = RetryEmitter::new(
            inner.clone(),
            BackoffConfig {
                max_attempts: 3,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            },
        );

        let result = retry.emit(&[make_test_event()]).await;

        assert!(result.is_err());
        assert_eq!(inner.calls(), 4); // 1 initial + 3 retries
        assert_eq!(retry.retry_count(), 3);
        assert_eq!(retry.recovered_count(), 0);
    }

    #[tokio::test]
    async fn test_retry_health_passthrough() {
        let inner = Arc::new(FailingEmitter::new(0));
        let retry = RetryEmitter::with_defaults(inner);

        assert!(retry.health().await);
    }

    #[test]
    fn test_xorshift_produces_distinct_values() {
        use super::Xorshift64;
        let rng = Xorshift64::new();

        // Generate 100 values and check they're not all the same
        let values: Vec<u64> = (0..100).map(|_| rng.next()).collect();
        let unique_count = values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        // Should have at least 90% unique values (allows for rare collisions)
        assert!(
            unique_count > 90,
            "Expected >90 unique values, got {unique_count}"
        );
    }

    #[test]
    fn test_xorshift_f64_in_range() {
        use super::Xorshift64;
        let rng = Xorshift64::new();

        // All values should be in [0.0, 1.0)
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!(v >= 0.0, "Value {v} below 0.0");
            assert!(v < 1.0, "Value {v} >= 1.0");
        }
    }

    #[test]
    fn test_rand_jitter_produces_varied_values() {
        use super::rand_jitter;

        // Generate multiple jitter values
        let values: Vec<f64> = (0..100).map(|_| rand_jitter()).collect();

        // Check distribution - values should span the range reasonably
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Should have reasonable spread (not all clustered)
        assert!(max - min > 0.5, "Jitter range too narrow: {min} to {max}");
    }
}
