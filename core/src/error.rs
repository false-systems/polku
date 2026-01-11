//! Error types for POLKU plugins

use thiserror::Error;

/// Error type for plugin operations
///
/// This is the standard error type used by all POLKU plugins including
/// ingestors, middleware, and emitters. It provides structured error
/// categories that help with debugging and error handling.
///
/// # Example
///
/// ```
/// use polku_core::PluginError;
///
/// fn connect_to_backend() -> Result<(), PluginError> {
///     // Simulate connection failure
///     Err(PluginError::Connection("refused".to_string()))
/// }
///
/// match connect_to_backend() {
///     Ok(_) => println!("Connected!"),
///     Err(PluginError::Connection(msg)) => println!("Connection failed: {}", msg),
///     Err(e) => println!("Other error: {}", e),
/// }
/// ```
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PluginError {
    /// Initialization failed
    ///
    /// Returned when a plugin fails to initialize, typically during startup.
    /// Examples: invalid configuration, failed to bind port, missing credentials.
    #[error("initialization failed: {0}")]
    Init(String),

    /// Transform failed
    ///
    /// Returned when an ingestor or middleware fails to transform data.
    /// Examples: invalid JSON, schema mismatch, encoding error.
    #[error("transform failed: {0}")]
    Transform(String),

    /// Send failed
    ///
    /// Returned when an emitter fails to send events to the destination.
    /// Examples: network timeout, server rejected request, quota exceeded.
    #[error("send failed: {0}")]
    Send(String),

    /// Connection error
    ///
    /// Returned when a network connection fails.
    /// Examples: DNS lookup failed, connection refused, TLS handshake error.
    #[error("connection error: {0}")]
    Connection(String),

    /// Not ready
    ///
    /// Returned when a plugin is accessed before it's ready to handle requests.
    /// This is typically a transient state during startup or recovery.
    #[error("plugin not ready")]
    NotReady,

    /// Shutdown error
    ///
    /// Returned when graceful shutdown fails.
    /// Examples: failed to flush buffers, timeout waiting for pending operations.
    #[error("shutdown error: {0}")]
    Shutdown(String),
}
