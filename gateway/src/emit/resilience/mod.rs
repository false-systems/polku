//! Resilience wrappers for emitters
//!
//! Provides composable fault-tolerance patterns:
//! - **RetryEmitter**: Exponential backoff with jitter
//! - **CircuitBreakerEmitter**: Fail-fast when backend unhealthy
//! - **DLQEmitter**: Capture undeliverable events to dead letter queue
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::emit::resilience::*;
//!
//! let dlq = Arc::new(DeadLetterQueue::new(1000));
//! let resilient = ResilientEmitter::wrap(grpc_emitter)
//!     .with_default_retry()
//!     .with_default_circuit_breaker()
//!     .with_default_dlq(dlq)
//!     .build();
//! ```

mod circuit_breaker;
mod config;
mod dlq;
mod retry;

pub use circuit_breaker::{CircuitBreakerConfig, CircuitBreakerEmitter, CircuitState};
pub use config::ResilientEmitter;
pub use dlq::{DLQConfig, DLQEmitter, DeadLetterQueue, FailedEvent};
pub use retry::{BackoffConfig, RetryEmitter};
