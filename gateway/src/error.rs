//! Error types for POLKU

use thiserror::Error;

// Re-export PluginError from polku-core
pub use polku_core::{ErrorContext, PipelineStage, PluginError};

/// Result type alias for POLKU operations
pub type Result<T> = std::result::Result<T, PolkuError>;

/// Main error type for POLKU
#[derive(Error, Debug)]
#[allow(clippy::result_large_err)]
pub enum PolkuError {
    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// gRPC transport error
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error (boxed to reduce enum size)
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// Plugin error
    #[error("plugin '{plugin}' error: {message}")]
    Plugin { plugin: String, message: String },

    /// Buffer full - events dropped
    #[error("buffer full, dropped {count} events")]
    BufferFull { count: usize },

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Metrics error
    #[error("metrics error: {0}")]
    Metrics(String),

    /// Shutdown requested
    #[error("shutdown requested")]
    Shutdown,
}

impl From<PluginError> for PolkuError {
    fn from(err: PluginError) -> Self {
        PolkuError::Plugin {
            plugin: "unknown".to_string(),
            message: err.to_string(),
        }
    }
}

impl From<PolkuError> for tonic::Status {
    fn from(err: PolkuError) -> Self {
        match err {
            PolkuError::Config(msg) => tonic::Status::invalid_argument(msg),
            PolkuError::Transport(e) => tonic::Status::unavailable(e.to_string()),
            PolkuError::Grpc(status) => status,
            PolkuError::Plugin { plugin, message } => {
                tonic::Status::internal(format!("plugin '{plugin}': {message}"))
            }
            PolkuError::BufferFull { count } => {
                tonic::Status::resource_exhausted(format!("buffer full, dropped {count} events"))
            }
            PolkuError::Io(e) => tonic::Status::internal(e.to_string()),
            PolkuError::Serialization(msg) => tonic::Status::invalid_argument(msg),
            PolkuError::Metrics(msg) => tonic::Status::internal(format!("metrics: {msg}")),
            PolkuError::Shutdown => tonic::Status::unavailable("shutting down"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_error_to_polku_error() {
        let plugin_err = PluginError::Init("failed to connect".to_string());
        let polku_err: PolkuError = plugin_err.into();
        assert!(matches!(polku_err, PolkuError::Plugin { .. }));
    }

    #[test]
    fn test_polku_error_to_grpc_status() {
        let err = PolkuError::BufferFull { count: 100 };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }
}
