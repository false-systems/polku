//! Convenience re-exports for pipeline authors.
//!
//! ```rust
//! use polku_runtime::prelude::*;
//! ```

// Core types
pub use polku_core::{Message, MessageId};

// Pipeline builder
pub use polku_gateway::{Hub, HubRunner, MessageSender, RawSender};

// Emitters
pub use polku_gateway::{Emitter, GrpcEmitter, StdoutEmitter, WebhookEmitter};

// Resilience wrappers
pub use polku_gateway::{
    BackoffConfig, CircuitBreakerConfig, CircuitBreakerEmitter, ResilientEmitter, RetryEmitter,
};

// Ingestors
pub use polku_gateway::{IngestContext, Ingestor, JsonIngestor, PassthroughIngestor};

// Middleware
pub use polku_gateway::{
    Aggregator, Deduplicator, Enricher, Filter, Middleware, MiddlewareChain, PassThrough,
    RateLimiter, Router, Sampler, Throttle, Transform, Validator,
};

// Buffer strategies
pub use polku_gateway::BufferStrategy;

// Checkpointing
pub use polku_gateway::{CheckpointStore, MemoryCheckpointStore};

// Error types
pub use polku_gateway::{PluginError, PolkuError};

// Zero-copy payload
pub use bytes::Bytes;

// Runtime
pub use crate::RuntimeBuilder;
