//! Passthrough ingestor - for events already in polku.Event format
//!
//! Use this when the source is already sending properly formatted Events,
//! for example from an SDK or another Polku instance.

use super::{IngestContext, Ingestor};
use crate::emit::Event;
use crate::error::PluginError;
use prost::Message;

/// Passthrough ingestor for pre-formatted Events
///
/// Expects data to be a protobuf-encoded `polku.event.v1.Event`.
/// Useful for:
/// - SDK clients that produce Events directly
/// - Forwarding between Polku instances
/// - Testing
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

    fn ingest(&self, _ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
        let event = Event::decode(data)
            .map_err(|e| PluginError::Transform(format!("Invalid protobuf Event: {}", e)))?;

        Ok(vec![event])
    }
}
