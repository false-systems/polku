//! Emitter system for POLKU
//!
//! Emitters send Messages to various destinations (gRPC backends, Kafka, stdout, etc.)
//! All registered emitters receive messages in a fan-out pattern.

#[cfg(feature = "ahti")]
pub mod ahti;
pub mod external;
pub mod grpc;
pub mod registry;
pub mod resilience;
pub mod stdout;
pub mod webhook;

// Re-export Emitter trait and Event type from polku-core
// This is the canonical source - external plugins depend on polku-core directly
pub use polku_core::Emitter;
pub use polku_core::Event;
pub use polku_core::PluginError;
pub use registry::EmitterRegistry;

#[cfg(feature = "ahti")]
pub use ahti::AhtiEmitter;
pub use external::ExternalEmitter;
pub use grpc::GrpcEmitter;
pub use stdout::StdoutEmitter;
pub use webhook::WebhookEmitter;
