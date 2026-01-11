//! Ingestor system for POLKU
//!
//! Ingestors decode raw bytes from various protocols into unified Messages.
//! Each source registers an Ingestor that knows how to deserialize that format.

use crate::emit::Event;
use crate::error::PluginError;

/// Context for ingestion
#[derive(Debug, Clone)]
pub struct IngestContext<'a> {
    /// Source identifier (e.g., "my-agent", "otel-collector")
    pub source: &'a str,
    /// Cluster/environment identifier
    pub cluster: &'a str,
    /// Format hint (e.g., "protobuf", "json", "msgpack")
    pub format: &'a str,
}

/// Ingestor trait - decodes protocol bytes into Events
///
/// Each ingestor handles a specific source format and transforms it
/// into the unified Event format that POLKU uses internally.
///
/// # Example
///
/// ```ignore
/// struct MyAgentIngestor;
///
/// impl Ingestor for MyAgentIngestor {
///     fn name(&self) -> &'static str { "my-agent" }
///
///     fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
///         let my_events: Vec<MyEvent> = deserialize(data)?;
///         Ok(my_events.into_iter().map(|e| e.into_event()).collect())
///     }
/// }
/// ```
pub trait Ingestor: Send + Sync {
    /// Ingestor name for identification and logging
    fn name(&self) -> &'static str;

    /// Decode raw bytes into Events
    ///
    /// # Arguments
    /// * `ctx` - Context with source, cluster, and format information
    /// * `data` - Raw bytes from the source
    ///
    /// # Returns
    /// Vector of Events or a PluginError
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError>;
}
