//! Error types for POLKU plugins

use thiserror::Error;

/// Pipeline stage where an error occurred
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineStage {
    /// Data ingestion / protocol decoding
    Ingest,
    /// Middleware processing (transform, filter, route)
    Middleware,
    /// Buffer operations (ring buffer, tiered storage)
    Buffer,
    /// Emitting to destinations
    Emit,
    /// Graceful shutdown
    Shutdown,
}

impl std::fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingest => write!(f, "ingest"),
            Self::Middleware => write!(f, "middleware"),
            Self::Buffer => write!(f, "buffer"),
            Self::Emit => write!(f, "emit"),
            Self::Shutdown => write!(f, "shutdown"),
        }
    }
}

/// Structured context for pipeline errors
///
/// Attached to `PluginError` via `.with_context()` to give AI agents
/// structured information about where and why an error occurred.
///
/// # Example
///
/// ```
/// use polku_core::{PluginError, ErrorContext, PipelineStage};
///
/// let err = PluginError::Send("connection refused".to_string())
///     .with_context(ErrorContext {
///         component: "grpc-emitter".to_string(),
///         stage: PipelineStage::Emit,
///         message_id: Some("01HQXYZ".to_string()),
///     });
///
/// // AI agent gets structured access:
/// if let Some(ctx) = err.context() {
///     assert_eq!(ctx.component, "grpc-emitter");
///     assert_eq!(ctx.stage, PipelineStage::Emit);
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorContext {
    /// Component that produced the error (e.g., "grpc-emitter", "json-ingestor")
    pub component: String,
    /// Pipeline stage where the error occurred
    pub stage: PipelineStage,
    /// Message ID being processed when error occurred (if available)
    pub message_id: Option<String>,
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}/{}]", self.component, self.stage)?;
        if let Some(ref id) = self.message_id {
            write!(f, " msg={}", id)?;
        }
        Ok(())
    }
}

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

    /// Error with structured pipeline context
    ///
    /// Wraps any PluginError variant with additional context about where
    /// in the pipeline the error occurred. AI agents can extract this
    /// context for structured debugging.
    #[error("{context} {source}")]
    WithContext {
        /// The underlying error
        #[source]
        source: Box<PluginError>,
        /// Structured pipeline context
        context: ErrorContext,
    },
}

impl PluginError {
    /// Attach structured pipeline context to this error
    pub fn with_context(self, context: ErrorContext) -> Self {
        Self::WithContext {
            source: Box::new(self),
            context,
        }
    }

    /// Extract the error context, if present
    pub fn context(&self) -> Option<&ErrorContext> {
        match self {
            Self::WithContext { context, .. } => Some(context),
            _ => None,
        }
    }

    /// Get the underlying error, unwrapping any context wrapper
    pub fn inner(&self) -> &PluginError {
        match self {
            Self::WithContext { source, .. } => source.inner(),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_with_context() {
        let err = PluginError::Send("connection refused".to_string()).with_context(ErrorContext {
            component: "grpc-emitter".to_string(),
            stage: PipelineStage::Emit,
            message_id: Some("msg-123".to_string()),
        });

        let ctx = err.context().unwrap();
        assert_eq!(ctx.component, "grpc-emitter");
        assert_eq!(ctx.stage, PipelineStage::Emit);
        assert_eq!(ctx.message_id.as_deref(), Some("msg-123"));
    }

    #[test]
    fn test_error_without_context() {
        let err = PluginError::Send("timeout".to_string());
        assert!(err.context().is_none());
    }

    #[test]
    fn test_inner_unwraps_context() {
        let err = PluginError::Connection("refused".to_string()).with_context(ErrorContext {
            component: "webhook".to_string(),
            stage: PipelineStage::Emit,
            message_id: None,
        });

        match err.inner() {
            PluginError::Connection(msg) => assert_eq!(msg, "refused"),
            _ => panic!("Expected Connection variant"),
        }
    }

    #[test]
    fn test_display_includes_context() {
        let err = PluginError::Send("timeout".to_string()).with_context(ErrorContext {
            component: "kafka".to_string(),
            stage: PipelineStage::Emit,
            message_id: Some("msg-456".to_string()),
        });

        let display = format!("{}", err);
        assert!(display.contains("kafka"));
        assert!(display.contains("emit"));
        assert!(display.contains("msg-456"));
        assert!(display.contains("timeout"));
    }

    #[test]
    fn test_pipeline_stage_display() {
        assert_eq!(PipelineStage::Ingest.to_string(), "ingest");
        assert_eq!(PipelineStage::Middleware.to_string(), "middleware");
        assert_eq!(PipelineStage::Buffer.to_string(), "buffer");
        assert_eq!(PipelineStage::Emit.to_string(), "emit");
        assert_eq!(PipelineStage::Shutdown.to_string(), "shutdown");
    }
}
