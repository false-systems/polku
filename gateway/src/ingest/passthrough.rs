//! Passthrough ingestor - for events already in polku.Event proto format
//!
//! Use this when the source is already sending properly formatted Events,
//! for example from an SDK or another Polku instance. Decodes the proto
//! Event and converts to Message, preserving severity/outcome/typed data
//! in metadata.

use super::{IngestContext, Ingestor};
use crate::error::PluginError;
use crate::message::Message;
use polku_core::Event;

/// Passthrough ingestor for pre-formatted Events
///
/// Expects data to be a protobuf-encoded `polku.event.v1.Event`.
/// Converts to Message at the wire boundary, preserving typed fields
/// in metadata using `polku.severity`, `polku.outcome`, etc.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::ingest::PassthroughIngestor;
///
/// let hub = Hub::new()
///     .ingestor(PassthroughIngestor::new())
///     .build();
/// ```
pub struct PassthroughIngestor;

impl PassthroughIngestor {
    /// Create a new passthrough ingestor
    pub fn new() -> Self {
        Self
    }
}

impl Default for PassthroughIngestor {
    fn default() -> Self {
        Self::new()
    }
}

impl Ingestor for PassthroughIngestor {
    fn name(&self) -> &'static str {
        "passthrough"
    }

    fn sources(&self) -> &'static [&'static str] {
        &["passthrough", "polku"]
    }

    fn ingest(&self, _ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        let event = <Event as prost::Message>::decode(data)
            .map_err(|e| PluginError::Transform(format!("Invalid protobuf Event: {}", e)))?;

        // Convert proto Event to Message (From impl handles the core mapping)
        let msg: Message = event.into();

        Ok(vec![msg])
    }
}
