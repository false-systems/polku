//! ResilientEmitter builder for composing resilience wrappers
//!
//! Provides a fluent API for wrapping emitters with retry, circuit breaker, and failure capture.

use super::{
    BackoffConfig, CircuitBreakerConfig, CircuitBreakerEmitter, FailureBuffer,
    FailureCaptureConfig, FailureCaptureEmitter, RetryEmitter,
};
use crate::emit::Emitter;
use std::sync::Arc;

/// Builder for composing resilience wrappers around an emitter
///
/// # Example
///
/// ```ignore
/// use polku_gateway::emit::resilience::*;
///
/// let grpc = GrpcEmitter::new("http://backend:50051").await?;
/// let buffer = Arc::new(FailureBuffer::new(1000));
///
/// let resilient = ResilientEmitter::wrap(grpc)
///     .with_retry(BackoffConfig::default())
///     .with_circuit_breaker(CircuitBreakerConfig::default())
///     .with_failure_capture(buffer.clone(), FailureCaptureConfig::default())
///     .build();
///
/// hub.emitter_arc(resilient);
/// ```
pub struct ResilientEmitter {
    inner: Arc<dyn Emitter>,
    retry_config: Option<BackoffConfig>,
    circuit_breaker_config: Option<CircuitBreakerConfig>,
    failure_capture: Option<(Arc<FailureBuffer>, FailureCaptureConfig)>,
}

impl ResilientEmitter {
    /// Start building a resilient emitter stack
    pub fn wrap<E: Emitter + 'static>(emitter: E) -> Self {
        Self {
            inner: Arc::new(emitter),
            retry_config: None,
            circuit_breaker_config: None,
            failure_capture: None,
        }
    }

    /// Start building from an Arc<dyn Emitter>
    pub fn wrap_arc(emitter: Arc<dyn Emitter>) -> Self {
        Self {
            inner: emitter,
            retry_config: None,
            circuit_breaker_config: None,
            failure_capture: None,
        }
    }

    /// Add retry with exponential backoff
    pub fn with_retry(mut self, config: BackoffConfig) -> Self {
        self.retry_config = Some(config);
        self
    }

    /// Add retry with default configuration
    pub fn with_default_retry(self) -> Self {
        self.with_retry(BackoffConfig::default())
    }

    /// Add circuit breaker
    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker_config = Some(config);
        self
    }

    /// Add circuit breaker with default configuration
    pub fn with_default_circuit_breaker(self) -> Self {
        self.with_circuit_breaker(CircuitBreakerConfig::default())
    }

    /// Add failure capture buffer (in-memory, for debugging)
    pub fn with_failure_capture(
        mut self,
        buffer: Arc<FailureBuffer>,
        config: FailureCaptureConfig,
    ) -> Self {
        self.failure_capture = Some((buffer, config));
        self
    }

    /// Add failure capture with default configuration
    pub fn with_default_failure_capture(self, buffer: Arc<FailureBuffer>) -> Self {
        self.with_failure_capture(buffer, FailureCaptureConfig::default())
    }

    /// Build the resilient emitter stack
    ///
    /// Composition order (inside-out):
    /// 1. Inner emitter (actual destination)
    /// 2. RetryEmitter (if configured) - handles transient failures
    /// 3. CircuitBreakerEmitter (if configured) - prevents hammering
    /// 4. FailureCaptureEmitter (if configured) - captures failed events for debugging
    pub fn build(self) -> Arc<dyn Emitter> {
        let mut current: Arc<dyn Emitter> = self.inner;

        // Apply retry wrapper (innermost)
        if let Some(config) = self.retry_config {
            current = Arc::new(RetryEmitter::new(current, config));
        }

        // Apply circuit breaker
        if let Some(config) = self.circuit_breaker_config {
            current = Arc::new(CircuitBreakerEmitter::new(current, config));
        }

        // Apply failure capture (outermost)
        if let Some((buffer, config)) = self.failure_capture {
            current = Arc::new(FailureCaptureEmitter::new(current, buffer, config));
        }

        current
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::error::PluginError;
    use crate::message::Message;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

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
        fn name(&self) -> &str {
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

    /// Always failing emitter
    struct AlwaysFailingEmitter;

    #[async_trait]
    impl Emitter for AlwaysFailingEmitter {
        fn name(&self) -> &str {
            "always_failing"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            Err(PluginError::Connection("always fails".into()))
        }
        async fn health(&self) -> bool {
            false
        }
    }

    fn make_test_message() -> Message {
        Message::new("test", "test", bytes::Bytes::new())
    }

    #[tokio::test]
    async fn test_resilient_emitter_with_retry_recovers() {
        let inner = RecoverableEmitter::new(2); // Fails twice
        let buffer = Arc::new(FailureBuffer::new(100));

        let resilient = ResilientEmitter::wrap(inner)
            .with_retry(BackoffConfig {
                max_attempts: 3,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .with_default_failure_capture(buffer.clone())
            .build();

        // Should succeed after retries
        let result = resilient.emit(&[make_test_message()]).await;
        assert!(result.is_ok());
        assert!(buffer.is_empty()); // No events captured
    }

    #[tokio::test]
    async fn test_resilient_emitter_captures_after_retries_exhausted() {
        let inner = AlwaysFailingEmitter;
        let buffer = Arc::new(FailureBuffer::new(100));

        let resilient = ResilientEmitter::wrap(inner)
            .with_retry(BackoffConfig {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .with_default_failure_capture(buffer.clone())
            .build();

        let result = resilient.emit(&[make_test_message()]).await;
        assert!(result.is_err());
        assert_eq!(buffer.len(), 1); // Event captured to buffer
    }

    #[tokio::test]
    async fn test_resilient_emitter_circuit_breaker_opens() {
        let inner = AlwaysFailingEmitter;
        let buffer = Arc::new(FailureBuffer::new(100));

        let resilient = ResilientEmitter::wrap(inner)
            .with_retry(BackoffConfig {
                max_attempts: 0, // No retries
                ..Default::default()
            })
            .with_circuit_breaker(CircuitBreakerConfig {
                failure_threshold: 2,
                reset_timeout: Duration::from_secs(60),
                ..Default::default()
            })
            .with_default_failure_capture(buffer.clone())
            .build();

        // First 2 failures open circuit
        let _ = resilient.emit(&[make_test_message()]).await;
        let _ = resilient.emit(&[make_test_message()]).await;

        // Third request should fail fast with NotReady
        let result = resilient.emit(&[make_test_message()]).await;
        assert!(matches!(result, Err(PluginError::NotReady)));

        // Buffer should have events from first two failures + circuit open
        assert!(buffer.len() >= 2);
    }

    #[tokio::test]
    async fn test_resilient_emitter_full_stack() {
        let inner = RecoverableEmitter::new(1); // Fails once
        let buffer = Arc::new(FailureBuffer::new(100));

        let resilient = ResilientEmitter::wrap(inner)
            .with_retry(BackoffConfig {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .with_circuit_breaker(CircuitBreakerConfig {
                failure_threshold: 5, // High threshold
                ..Default::default()
            })
            .with_default_failure_capture(buffer.clone())
            .build();

        // Should recover after 1 retry
        let result = resilient.emit(&[make_test_message()]).await;
        assert!(result.is_ok());
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_builder_defaults() {
        let inner = RecoverableEmitter::new(0); // Never fails

        let resilient = ResilientEmitter::wrap(inner)
            .with_default_retry()
            .with_default_circuit_breaker()
            .build();

        let result = resilient.emit(&[make_test_message()]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_builder_arc_input() {
        let inner: Arc<dyn Emitter> = Arc::new(RecoverableEmitter::new(0));

        let resilient = ResilientEmitter::wrap_arc(inner)
            .with_default_retry()
            .build();

        let result = resilient.emit(&[make_test_message()]).await;
        assert!(result.is_ok());
    }
}
