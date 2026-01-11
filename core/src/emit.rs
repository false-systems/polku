//! Emitter trait for POLKU plugins
//!
//! The [`Emitter`] trait defines the interface for sending events to
//! external destinations. Emitters are the output side of the POLKU pipeline.

use crate::error::PluginError;
use crate::proto::Event;
use async_trait::async_trait;

/// Emitter trait - sends Events to destinations
///
/// Each emitter handles forwarding events to a specific destination.
/// Multiple emitters can be registered with POLKU and events will be
/// sent to all of them in a fan-out pattern.
///
/// # Implementation Requirements
///
/// - Emitters must be `Send + Sync` for use across async tasks
/// - The `emit` method receives a batch of events and should handle them atomically
/// - Health checks should be lightweight and not affect normal operation
/// - Shutdown should flush any pending data and release resources
///
/// # Example
///
/// ```ignore
/// use polku_core::{Emitter, Event, PluginError};
/// use async_trait::async_trait;
///
/// struct HttpEmitter {
///     client: reqwest::Client,
///     endpoint: String,
/// }
///
/// #[async_trait]
/// impl Emitter for HttpEmitter {
///     fn name(&self) -> &'static str {
///         "http"
///     }
///
///     async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
///         let body = serde_json::to_vec(events)
///             .map_err(|e| PluginError::Transform(e.to_string()))?;
///
///         self.client.post(&self.endpoint)
///             .body(body)
///             .send()
///             .await
///             .map_err(|e| PluginError::Send(e.to_string()))?;
///
///         Ok(())
///     }
///
///     async fn health(&self) -> bool {
///         self.client.get(&format!("{}/health", self.endpoint))
///             .send()
///             .await
///             .map(|r| r.status().is_success())
///             .unwrap_or(false)
///     }
/// }
/// ```
#[async_trait]
pub trait Emitter: Send + Sync {
    /// Returns the emitter's name for identification and logging
    ///
    /// This should be a short, descriptive name that uniquely identifies
    /// the emitter type. Examples: "stdout", "kafka", "ahti", "s3".
    fn name(&self) -> &'static str;

    /// Emit a batch of events to the destination
    ///
    /// # Arguments
    ///
    /// * `events` - Slice of Events to emit. May be empty.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All events were successfully sent
    /// * `Err(PluginError)` - One or more events failed to send
    ///
    /// # Error Handling
    ///
    /// Implementations should decide whether to fail the entire batch on
    /// any error, or to continue with remaining events. The returned error
    /// should describe the failure mode clearly.
    async fn emit(&self, events: &[Event]) -> Result<(), PluginError>;

    /// Check if the destination is healthy and accepting events
    ///
    /// This method is called periodically by POLKU to monitor emitter health.
    /// It should be lightweight and not block for extended periods.
    ///
    /// # Returns
    ///
    /// * `true` - Destination is healthy and ready
    /// * `false` - Destination is unhealthy or unavailable
    async fn health(&self) -> bool;

    /// Graceful shutdown
    ///
    /// Called when POLKU is shutting down. Implementations should:
    /// - Flush any buffered events
    /// - Close network connections
    /// - Release any held resources
    ///
    /// The default implementation returns `Ok(())` for emitters that
    /// don't need cleanup.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Shutdown completed successfully
    /// * `Err(PluginError::Shutdown(_))` - Shutdown failed
    async fn shutdown(&self) -> Result<(), PluginError> {
        Ok(())
    }
}
