//! POLKU - Programmatic Protocol Hub
//!
//! Infrastructure library for internal service communication.
//! Logic IS code - you have full Rust power to decide how data flows.
//!
//! # Triadic Plugin Architecture
//!
//! ```text
//! Ingestors ──► Middleware ──► Buffer ──► Emitters
//! ```
//!
//! All three components are pluggable via traits. Users provide their own
//! ingestors for protocol decoding and emitters for destination delivery.

#![deny(unsafe_code)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![warn(clippy::panic)]

pub mod buffer;
pub mod config;
pub mod emit;
pub mod error;
pub mod hub;
pub mod ingest;
pub mod message;
pub mod metrics;
pub mod middleware;
pub mod registry;
pub mod server;

// Proto types generated from polku/v1/gateway.proto
pub mod proto {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    #![allow(clippy::panic)]
    #![allow(clippy::derive_partial_eq_without_eq)]

    include!("proto/polku.v1.rs");

    pub use gateway_server::Gateway;
    pub use gateway_server::GatewayServer;
}

pub use config::Config;
pub use emit::resilience::{
    BackoffConfig, CircuitBreakerConfig, CircuitBreakerEmitter, CircuitState, DLQConfig,
    DLQEmitter, DeadLetterQueue, FailedEvent, ResilientEmitter, RetryEmitter,
};
pub use emit::{Emitter, GrpcEmitter, StdoutEmitter, WebhookEmitter};
pub use error::{PluginError, PolkuError, Result};
pub use hub::{Hub, HubRunner, MessageSender};
pub use ingest::{IngestContext, Ingestor};
pub use message::Message;
pub use middleware::{Filter, Middleware, MiddlewareChain, PassThrough, Transform};
pub use proto::Event;
pub use registry::PluginRegistry;
