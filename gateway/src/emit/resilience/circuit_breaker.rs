//! Circuit breaker emitter
//!
//! Implements the circuit breaker pattern to fail fast when backend is unhealthy.

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Circuit breaker state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests flow through
    Closed,
    /// Circuit is open - requests fail fast
    Open,
    /// Testing if backend recovered - allowing limited requests
    HalfOpen,
}

impl CircuitState {
    /// Convert to Prometheus metric value (0=Closed, 1=Open, 2=HalfOpen)
    pub fn as_metric_value(self) -> f64 {
        match self {
            CircuitState::Closed => 0.0,
            CircuitState::Open => 1.0,
            CircuitState::HalfOpen => 2.0,
        }
    }
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures to open circuit
    pub failure_threshold: u32,
    /// Number of consecutive successes in half-open to close circuit
    pub success_threshold: u32,
    /// Time to wait before transitioning from Open to HalfOpen
    pub reset_timeout: Duration,
    /// Maximum concurrent requests allowed in HalfOpen state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            reset_timeout: Duration::from_secs(30),
            half_open_max_requests: 1,
        }
    }
}

/// Internal state tracking
struct CircuitBreakerState {
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    last_failure_time: Option<Instant>,
    half_open_requests: u32,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_failure_time: None,
            half_open_requests: 0,
        }
    }
}

/// Emitter wrapper implementing circuit breaker pattern
pub struct CircuitBreakerEmitter {
    inner: Arc<dyn Emitter>,
    config: CircuitBreakerConfig,
    state: RwLock<CircuitBreakerState>,
    /// Metrics: times circuit opened
    open_count: AtomicU64,
    /// Metrics: requests rejected by open circuit
    rejected_count: AtomicU64,
}

impl CircuitBreakerEmitter {
    /// Create a new CircuitBreakerEmitter with the given configuration
    pub fn new(inner: Arc<dyn Emitter>, config: CircuitBreakerConfig) -> Self {
        Self {
            inner,
            config,
            state: RwLock::new(CircuitBreakerState::new()),
            open_count: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
        }
    }

    /// Create a CircuitBreakerEmitter with default configuration
    pub fn with_defaults(inner: Arc<dyn Emitter>) -> Self {
        Self::new(inner, CircuitBreakerConfig::default())
    }

    /// Get current circuit state (for monitoring)
    pub fn current_state(&self) -> CircuitState {
        self.state.read().state
    }

    /// Get count of times circuit has opened
    pub fn open_count(&self) -> u64 {
        self.open_count.load(Ordering::Relaxed)
    }

    /// Get count of rejected requests
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(Ordering::Relaxed)
    }

    /// Check if request should be allowed and update state for time-based transitions
    fn should_allow_request(&self) -> bool {
        let mut state = self.state.write();

        match state.state {
            CircuitState::Closed => true,

            CircuitState::Open => {
                // Check if reset timeout has elapsed
                // Note: clippy suggests `if let ... &&` but that's unstable (RFC 2497)
                #[allow(clippy::collapsible_if)]
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= self.config.reset_timeout {
                        // Transition to HalfOpen
                        state.state = CircuitState::HalfOpen;
                        state.half_open_requests = 1; // Allow this request
                        state.consecutive_successes = 0;
                        tracing::info!(
                            emitter = self.inner.name(),
                            "circuit breaker transitioning to half-open"
                        );
                        return true;
                    }
                }
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                false
            }

            CircuitState::HalfOpen => {
                // Allow limited requests in half-open state
                if state.half_open_requests < self.config.half_open_max_requests {
                    state.half_open_requests += 1;
                    true
                } else {
                    self.rejected_count.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        }
    }

    /// Record successful request
    fn record_success(&self) {
        let mut state = self.state.write();
        state.consecutive_failures = 0;

        if state.state == CircuitState::HalfOpen {
            state.consecutive_successes += 1;
            if state.consecutive_successes >= self.config.success_threshold {
                // Transition back to Closed
                state.state = CircuitState::Closed;
                state.half_open_requests = 0;
                tracing::info!(
                    emitter = self.inner.name(),
                    "circuit breaker closed - backend recovered"
                );
            }
        }
    }

    /// Record failed request
    fn record_failure(&self) {
        let mut state = self.state.write();
        state.consecutive_successes = 0;
        state.consecutive_failures += 1;
        state.last_failure_time = Some(Instant::now());

        match state.state {
            CircuitState::Closed => {
                if state.consecutive_failures >= self.config.failure_threshold {
                    state.state = CircuitState::Open;
                    self.open_count.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        emitter = self.inner.name(),
                        failures = state.consecutive_failures,
                        "circuit breaker opened - too many failures"
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens circuit again
                state.state = CircuitState::Open;
                state.half_open_requests = 0;
                self.open_count.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    emitter = self.inner.name(),
                    "circuit breaker re-opened - probe failed"
                );
            }
            CircuitState::Open => {
                // Already open, just update failure time
            }
        }
    }
}

#[async_trait]
impl Emitter for CircuitBreakerEmitter {
    fn name(&self) -> &'static str {
        "circuit_breaker"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        if !self.should_allow_request() {
            return Err(PluginError::NotReady);
        }

        match self.inner.emit(messages).await {
            Ok(()) => {
                self.record_success();
                Ok(())
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    async fn health(&self) -> bool {
        // Report unhealthy when circuit is open
        match self.current_state() {
            CircuitState::Open => false,
            _ => self.inner.health().await,
        }
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

    /// Always failing emitter
    struct AlwaysFailingEmitter;

    #[async_trait]
    impl Emitter for AlwaysFailingEmitter {
        fn name(&self) -> &'static str {
            "always_failing"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            Err(PluginError::Connection("always fails".into()))
        }
        async fn health(&self) -> bool {
            false
        }
    }

    /// Emitter that fails N times then succeeds
    struct RecoverableEmitter {
        fail_count: AtomicU32,
        max_failures: u32,
    }

    impl RecoverableEmitter {
        fn new(max_failures: u32) -> Self {
            Self {
                fail_count: AtomicU32::new(0),
                max_failures,
            }
        }
    }

    #[async_trait]
    impl Emitter for RecoverableEmitter {
        fn name(&self) -> &'static str {
            "recoverable"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            let count = self.fail_count.fetch_add(1, Ordering::SeqCst);
            if count < self.max_failures {
                Err(PluginError::Connection("temporary failure".into()))
            } else {
                Ok(())
            }
        }
        async fn health(&self) -> bool {
            true
        }
    }

    /// Always succeeding emitter
    struct SuccessEmitter;

    #[async_trait]
    impl Emitter for SuccessEmitter {
        fn name(&self) -> &'static str {
            "success"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            Ok(())
        }
        async fn health(&self) -> bool {
            true
        }
    }

    fn make_test_message() -> Message {
        Message::new("test", "test", bytes::Bytes::new())
    }

    #[tokio::test]
    async fn test_circuit_starts_closed() {
        let inner = Arc::new(SuccessEmitter);
        let cb = CircuitBreakerEmitter::with_defaults(inner);

        assert_eq!(cb.current_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_allows_success() {
        let inner = Arc::new(SuccessEmitter);
        let cb = CircuitBreakerEmitter::with_defaults(inner);

        let result = cb.emit(&[make_test_message()]).await;

        assert!(result.is_ok());
        assert_eq!(cb.current_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_opens_after_threshold() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 3,
                ..Default::default()
            },
        );

        // First 3 failures should go through
        for _ in 0..3 {
            let _ = cb.emit(&[make_test_message()]).await;
        }

        // Circuit should now be open
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert_eq!(cb.open_count(), 1);
    }

    #[tokio::test]
    async fn test_open_circuit_rejects_requests() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 1,
                reset_timeout: Duration::from_secs(60), // Long timeout
                ..Default::default()
            },
        );

        // Open circuit
        let _ = cb.emit(&[make_test_message()]).await;
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Next request should fail fast with NotReady
        let result = cb.emit(&[make_test_message()]).await;
        assert!(matches!(result, Err(PluginError::NotReady)));
        assert_eq!(cb.rejected_count(), 1);
    }

    #[tokio::test]
    async fn test_circuit_transitions_to_half_open() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 1,
                reset_timeout: Duration::from_millis(10),
                ..Default::default()
            },
        );

        // Open circuit
        let _ = cb.emit(&[make_test_message()]).await;
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Next request should be allowed (half-open probe)
        let _ = cb.emit(&[make_test_message()]).await;

        // But it failed, so back to open
        assert_eq!(cb.current_state(), CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_closes_after_success() {
        let inner = Arc::new(RecoverableEmitter::new(2)); // Fails twice
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 1,
                success_threshold: 1,
                reset_timeout: Duration::from_millis(5),
                half_open_max_requests: 1,
            },
        );

        // Open circuit (1 failure)
        let _ = cb.emit(&[make_test_message()]).await;
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Wait for half-open
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Probe fails (2nd failure)
        let _ = cb.emit(&[make_test_message()]).await;
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Wait again
        tokio::time::sleep(Duration::from_millis(10)).await;

        // This probe succeeds (3rd attempt, recoverable allows it)
        let result = cb.emit(&[make_test_message()]).await;
        assert!(result.is_ok());
        assert_eq!(cb.current_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_health_reflects_circuit_state() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 1,
                ..Default::default()
            },
        );

        // Closed - health depends on inner
        assert!(!cb.health().await); // Inner always returns false

        // Open circuit
        let _ = cb.emit(&[make_test_message()]).await;
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Open - always unhealthy
        assert!(!cb.health().await);
    }

    #[tokio::test]
    async fn test_success_resets_failure_count() {
        let inner = Arc::new(RecoverableEmitter::new(2)); // Fails twice
        let cb = CircuitBreakerEmitter::new(
            inner,
            CircuitBreakerConfig {
                failure_threshold: 5, // High threshold
                ..Default::default()
            },
        );

        // Two failures
        let _ = cb.emit(&[make_test_message()]).await;
        let _ = cb.emit(&[make_test_message()]).await;

        // Third succeeds
        let result = cb.emit(&[make_test_message()]).await;
        assert!(result.is_ok());

        // Circuit should still be closed (failures reset)
        assert_eq!(cb.current_state(), CircuitState::Closed);
    }
}
