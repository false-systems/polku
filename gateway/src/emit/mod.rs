//! Emitter system for POLKU
//!
//! Emitters send Messages to various destinations (gRPC backends, Kafka, stdout, etc.)
//! All registered emitters receive messages in a fan-out pattern.

pub mod grpc;
pub mod resilience;
pub mod stdout;
pub mod webhook;

use crate::error::PluginError;
use crate::proto::Event;
use async_trait::async_trait;

pub use grpc::GrpcEmitter;
pub use stdout::StdoutEmitter;
pub use webhook::WebhookEmitter;

/// Emitter trait - sends Events to destinations
///
/// Each emitter handles forwarding events to a specific destination.
/// Multiple emitters can be registered and events will be sent to all of them.
///
/// # Example
///
/// ```ignore
/// struct MyDestinationEmitter {
///     client: MyGrpcClient,
/// }
///
/// #[async_trait]
/// impl Emitter for MyDestinationEmitter {
///     fn name(&self) -> &'static str { "my-destination" }
///
///     async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
///         self.client.send_events(events).await?;
///         Ok(())
///     }
///
///     async fn health(&self) -> bool {
///         self.client.ping().await.is_ok()
///     }
/// }
/// ```
#[async_trait]
pub trait Emitter: Send + Sync {
    /// Emitter name for identification and logging
    fn name(&self) -> &'static str;

    /// Emit events to the destination
    ///
    /// # Arguments
    /// * `events` - Slice of Events to emit
    ///
    /// # Returns
    /// Ok(()) on success, PluginError on failure
    async fn emit(&self, events: &[Event]) -> Result<(), PluginError>;

    /// Health check for the destination
    ///
    /// Returns true if the destination is healthy and accepting events.
    async fn health(&self) -> bool;

    /// Graceful shutdown
    ///
    /// Called when the gateway is shutting down to flush buffers, close connections, etc.
    async fn shutdown(&self) -> Result<(), PluginError> {
        Ok(())
    }
}
