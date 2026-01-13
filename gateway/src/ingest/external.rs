//! External ingestor - gRPC plugin for custom formats
//!
//! Delegates ingestion to an external gRPC service implementing
//! the IngestorPlugin protocol.

use super::{IngestContext, Ingestor};
use crate::emit::Event;
use crate::error::PluginError;

/// External ingestor that delegates to a gRPC plugin
///
/// Use this when you need to parse a custom format that isn't
/// supported by built-in ingestors. The plugin can be written
/// in any language that supports gRPC.
///
/// # Performance Note
///
/// gRPC calls add latency (~100-500μs per call). For high-volume
/// sources, consider implementing a built-in Rust ingestor instead.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::ingest::ExternalIngestor;
///
/// let hub = Hub::new()
///     .ingestor(ExternalIngestor::new("legacy-crm", "localhost:9001"))
///     .build();
/// ```
pub struct ExternalIngestor {
    /// Source identifier this ingestor handles
    source: String,
    /// gRPC address of the plugin
    address: String,
    /// Leaked static string for sources() return type
    /// (Required because trait returns &'static [&'static str])
    sources_static: &'static [&'static str],
    /// Leaked static string for name
    name_static: &'static str,
}

impl ExternalIngestor {
    /// Create a new external ingestor
    ///
    /// # Arguments
    /// * `source` - Source identifier to handle
    /// * `address` - gRPC address (e.g., "localhost:9001")
    pub fn new(source: impl Into<String>, address: impl Into<String>) -> Self {
        let source = source.into();
        let address = address.into();

        // Leak strings to create static references
        // This is fine because ingestors are long-lived (created once at startup)
        let source_leaked: &'static str = Box::leak(source.clone().into_boxed_str());
        let sources_vec: Vec<&'static str> = vec![source_leaked];
        let sources_static: &'static [&'static str] = Box::leak(sources_vec.into_boxed_slice());

        let name = format!("external:{}", source);
        let name_static: &'static str = Box::leak(name.into_boxed_str());

        Self {
            source,
            address,
            sources_static,
            name_static,
        }
    }

    /// Get the plugin address
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl Ingestor for ExternalIngestor {
    fn name(&self) -> &'static str {
        self.name_static
    }

    fn sources(&self) -> &'static [&'static str] {
        self.sources_static
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
        // TODO: Implement actual gRPC call to IngestorPlugin service
        //
        // The implementation would:
        // 1. Create/reuse a gRPC client connection to self.address
        // 2. Call IngestorPlugin.Ingest(IngestRequest { source, cluster, format, data })
        // 3. Return the Events from IngestResponse
        //
        // For now, return an error indicating the plugin isn't connected
        Err(PluginError::Connection(format!(
            "External ingestor '{}' not connected to plugin at {} (source: {}, {} bytes)",
            self.source,
            self.address,
            ctx.source,
            data.len()
        )))
    }
}
