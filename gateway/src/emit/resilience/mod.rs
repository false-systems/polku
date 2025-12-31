//! Resilience wrappers for emitters
//!
//! Provides composable fault-tolerance patterns:
//! - **RetryEmitter**: Exponential backoff with jitter
//! - **CircuitBreakerEmitter**: Fail-fast when backend unhealthy
//! - **FailureCaptureEmitter**: Capture failed events to in-memory buffer (for debugging)
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::emit::resilience::*;
//!
//! let buffer = Arc::new(FailureBuffer::new(1000));
//! let resilient = ResilientEmitter::wrap(grpc_emitter)
//!     .with_default_retry()
//!     .with_default_circuit_breaker()
//!     .with_default_failure_capture(buffer)
//!     .build();
//! ```

mod circuit_breaker;
mod config;
mod failure_buffer;
mod retry;

pub use circuit_breaker::{CircuitBreakerConfig, CircuitBreakerEmitter, CircuitState};
pub use config::ResilientEmitter;
pub use failure_buffer::{FailedEvent, FailureBuffer, FailureCaptureConfig, FailureCaptureEmitter};
pub use retry::{BackoffConfig, RetryEmitter};
