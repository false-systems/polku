//! JSON ingestor - for JSON-formatted events
//!
//! Supports:
//! - Single JSON object
//! - JSON array of objects
//! - Newline-delimited JSON (NDJSON/JSON Lines)

use super::{IngestContext, Ingestor};
use crate::error::PluginError;
use crate::message::Message;
use base64::Engine as _;
use bytes::Bytes;
use polku_core::message::MessageId;
use serde::Deserialize;
use std::collections::HashMap;

/// JSON ingestor for flexible JSON event ingestion
///
/// Accepts JSON events in multiple formats:
/// - Single object: `{"id": "...", "source": "...", ...}`
/// - Array: `[{...}, {...}]`
/// - Newline-delimited: `{...}\n{...}\n{...}`
///
/// # JSON Schema
///
/// ```json
/// {
///   "id": "optional-ulid",           // Generated if missing
///   "timestamp_unix_ns": 0,          // Current time if missing
///   "source": "required",
///   "event_type": "required",
///   "metadata": {},                  // Optional key-value pairs
///   "payload": "base64-encoded",     // Optional binary payload
///   "route_to": ["emitter1"]         // Optional routing hints
/// }
/// ```
///
/// # Example
///
/// ```ignore
/// use polku_gateway::ingest::JsonIngestor;
///
/// let hub = Hub::new()
///     .ingestor(JsonIngestor::new())
///     .build();
/// ```
pub struct JsonIngestor;

impl JsonIngestor {
    /// Create a new JSON ingestor
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonIngestor {
    fn default() -> Self {
        Self::new()
    }
}

/// Intermediate JSON representation for flexible parsing
#[derive(Debug, Deserialize)]
struct JsonEvent {
    #[serde(default)]
    id: Option<String>,

    #[serde(default)]
    timestamp_unix_ns: Option<i64>,

    #[serde(default)]
    source: Option<String>,

    #[serde(default)]
    event_type: Option<String>,

    #[serde(default)]
    metadata: HashMap<String, String>,

    /// Payload as base64-encoded string
    #[serde(default)]
    payload: Option<String>,

    #[serde(default)]
    route_to: Vec<String>,

    #[serde(default)]
    severity: i32,

    #[serde(default)]
    outcome: i32,
}

impl JsonEvent {
    fn into_message(self, ctx: &IngestContext) -> Message {
        let id: MessageId = self
            .id
            .map(|s| MessageId::from_string(&s))
            .unwrap_or_default();

        let timestamp = self
            .timestamp_unix_ns
            .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));

        // Use source from JSON or fall back to context
        let source = self.source.unwrap_or_else(|| ctx.source.to_string());
        let event_type = self.event_type.unwrap_or_else(|| "unknown".to_string());

        // Decode base64 payload if present
        let payload = self
            .payload
            .and_then(|p| base64::engine::general_purpose::STANDARD.decode(&p).ok())
            .unwrap_or_default();

        let mut msg = Message {
            id,
            timestamp,
            source: source.into(),
            message_type: event_type.into(),
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(Box::new(self.metadata))
            },
            payload: Bytes::from(payload),
            route_to: smallvec::SmallVec::from_vec(self.route_to),
        };

        // Preserve severity/outcome in metadata if non-default
        if self.severity != 0 {
            msg.metadata_mut().insert(
                polku_core::metadata_keys::SEVERITY.to_string(),
                self.severity.to_string(),
            );
        }
        if self.outcome != 0 {
            msg.metadata_mut().insert(
                polku_core::metadata_keys::OUTCOME.to_string(),
                self.outcome.to_string(),
            );
        }

        msg
    }
}

impl Ingestor for JsonIngestor {
    fn name(&self) -> &'static str {
        "json"
    }

    fn sources(&self) -> &'static [&'static str] {
        &["json", "json-lines", "ndjson"]
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        let text = std::str::from_utf8(data)
            .map_err(|e| PluginError::Transform(format!("Invalid UTF-8: {}", e)))?;

        let trimmed = text.trim();

        // Detect format and parse accordingly
        if ctx.format == "json-lines" || ctx.format == "ndjson" {
            // Explicit NDJSON format
            self.parse_ndjson(trimmed, ctx)
        } else if trimmed.starts_with('[') {
            // JSON array
            self.parse_array(trimmed, ctx)
        } else if trimmed.starts_with('{') {
            // Try single JSON object first (handles multi-line formatted JSON)
            self.parse_single(trimmed, ctx)
        } else {
            // Fallback: try NDJSON if it looks like multiple lines of objects
            self.parse_ndjson(trimmed, ctx)
        }
    }
}

impl JsonIngestor {
    fn parse_single(&self, text: &str, ctx: &IngestContext) -> Result<Vec<Message>, PluginError> {
        let json_event: JsonEvent = serde_json::from_str(text)
            .map_err(|e| PluginError::Transform(format!("Invalid JSON: {}", e)))?;

        Ok(vec![json_event.into_message(ctx)])
    }

    fn parse_array(&self, text: &str, ctx: &IngestContext) -> Result<Vec<Message>, PluginError> {
        let json_events: Vec<JsonEvent> = serde_json::from_str(text)
            .map_err(|e| PluginError::Transform(format!("Invalid JSON array: {}", e)))?;

        Ok(json_events
            .into_iter()
            .map(|e| e.into_message(ctx))
            .collect())
    }

    fn parse_ndjson(&self, text: &str, ctx: &IngestContext) -> Result<Vec<Message>, PluginError> {
        let mut messages = Vec::new();

        for (line_num, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let json_event: JsonEvent = serde_json::from_str(trimmed).map_err(|e| {
                PluginError::Transform(format!("Invalid JSON on line {}: {}", line_num + 1, e))
            })?;

            messages.push(json_event.into_message(ctx));
        }

        Ok(messages)
    }
}
