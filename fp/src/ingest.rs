//! OccurrenceIngestor — decode FALSE Protocol Occurrences into Messages
//!
//! Parses Occurrence JSON (single or array), maps fields to Message,
//! and preserves the full JSON in `msg.payload` for downstream consumers.

use crate::occurrence::Occurrence;
use bytes::Bytes;
use polku_core::message::MessageId;
use polku_core::metadata_keys;
use polku_gateway::PluginError;
use polku_gateway::ingest::{IngestContext, Ingestor};
use polku_gateway::message::Message;

/// Ingestor for FALSE Protocol Occurrences
///
/// Accepts Occurrence JSON in two formats:
/// - Single object: `{"id": "...", "type": "ci.test.failed", ...}`
/// - Array: `[{...}, {...}]`
///
/// # Field Mapping
///
/// | Occurrence field | Message field |
/// |-----------------|---------------|
/// | `id` | `msg.id` (ULID) |
/// | `timestamp` | `msg.timestamp` (parsed to unix nanos) |
/// | `source` | `msg.source` |
/// | `type` | `msg.message_type` |
/// | `severity` | `metadata["polku.severity"]` |
/// | `outcome` | `metadata["polku.outcome"]` |
/// | Full JSON | `msg.payload` (zero-copy passthrough) |
pub struct OccurrenceIngestor;

impl OccurrenceIngestor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for OccurrenceIngestor {
    fn default() -> Self {
        Self::new()
    }
}

impl OccurrenceIngestor {
    fn occurrence_to_message(&self, occ: Occurrence, raw_json: Bytes) -> Message {
        let id = MessageId::from_string(&occ.id);

        let timestamp = chrono::DateTime::parse_from_rfc3339(&occ.timestamp)
            .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0))
            .unwrap_or_else(|_| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));

        let mut msg = Message {
            id,
            timestamp,
            source: occ.source.into(),
            message_type: occ.occurrence_type.into(),
            metadata: None,
            payload: raw_json,
            route_to: smallvec::SmallVec::new(),
        };

        // Map severity and outcome to metadata
        msg.metadata_mut().insert(
            metadata_keys::SEVERITY.to_string(),
            occ.severity.as_polku_value().to_string(),
        );
        msg.metadata_mut().insert(
            metadata_keys::OUTCOME.to_string(),
            occ.outcome.as_polku_value().to_string(),
        );
        msg.metadata_mut().insert(
            metadata_keys::CONTENT_TYPE.to_string(),
            "application/json".to_string(),
        );

        msg
    }
}

impl Ingestor for OccurrenceIngestor {
    fn name(&self) -> &str {
        "occurrence"
    }

    fn sources(&self) -> &[&str] {
        &["occurrence", "false-protocol"]
    }

    fn ingest(&self, _ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        let text = std::str::from_utf8(data)
            .map_err(|e| PluginError::Transform(format!("Invalid UTF-8: {e}")))?;

        let trimmed = text.trim();

        if trimmed.starts_with('[') {
            // Array of Occurrences — parse each, preserve individual JSON
            let occurrences: Vec<serde_json::Value> = serde_json::from_str(trimmed)
                .map_err(|e| PluginError::Transform(format!("Invalid JSON array: {e}")))?;

            let mut messages = Vec::with_capacity(occurrences.len());
            for val in occurrences {
                let raw = serde_json::to_vec(&val)
                    .map_err(|e| PluginError::Transform(format!("JSON re-serialization: {e}")))?;
                let occ: Occurrence = serde_json::from_value(val)
                    .map_err(|e| PluginError::Transform(format!("Invalid Occurrence: {e}")))?;
                messages.push(self.occurrence_to_message(occ, Bytes::from(raw)));
            }
            Ok(messages)
        } else {
            // Single Occurrence — use input bytes as payload (zero-copy)
            let occ: Occurrence = serde_json::from_str(trimmed)
                .map_err(|e| PluginError::Transform(format!("Invalid Occurrence JSON: {e}")))?;
            let payload = Bytes::copy_from_slice(data);
            Ok(vec![self.occurrence_to_message(occ, payload)])
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn ctx() -> IngestContext<'static> {
        IngestContext {
            source: "occurrence",
            cluster: "prod",
            format: "json",
        }
    }

    #[test]
    fn test_valid_occurrence_maps_fields() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"{
            "id": "01HWXYZ",
            "timestamp": "2024-01-15T10:30:00Z",
            "source": "sykli",
            "type": "ci.test.failed",
            "severity": "error",
            "outcome": "failure"
        }"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 1);

        let msg = &msgs[0];
        assert_eq!(msg.id, "01HWXYZ");
        assert_eq!(msg.source, "sykli");
        assert_eq!(msg.message_type, "ci.test.failed");
        assert_eq!(
            msg.metadata().get(metadata_keys::SEVERITY),
            Some(&"4".to_string())
        );
        assert_eq!(
            msg.metadata().get(metadata_keys::OUTCOME),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_occurrence_with_error_block() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"{
            "id": "01ERR",
            "timestamp": "2024-01-15T10:30:00Z",
            "source": "sykli",
            "type": "ci.build.failed",
            "severity": "critical",
            "outcome": "failure",
            "error": {
                "code": "BUILD_FAILURE",
                "what_failed": "Cargo build failed"
            }
        }"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0].metadata().get(metadata_keys::SEVERITY),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn test_invalid_json_returns_error() {
        let ingestor = OccurrenceIngestor::new();
        let result = ingestor.ingest(&ctx(), b"not valid json");
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Transform(_) => {}
            other => panic!("Expected Transform error, got: {other:?}"),
        }
    }

    #[test]
    fn test_array_of_occurrences() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"[
            {"id": "01A", "timestamp": "2024-01-15T10:30:00Z", "source": "sykli", "type": "ci.test.passed", "severity": "info", "outcome": "success"},
            {"id": "01B", "timestamp": "2024-01-15T10:31:00Z", "source": "tapio", "type": "kernel.syscall.blocked", "severity": "warning", "outcome": "unknown"}
        ]"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].source, "sykli");
        assert_eq!(msgs[0].message_type, "ci.test.passed");
        assert_eq!(msgs[1].source, "tapio");
        assert_eq!(msgs[1].message_type, "kernel.syscall.blocked");
    }

    #[test]
    fn test_minimal_occurrence_no_crash() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"{
            "id": "01MIN",
            "timestamp": "2024-01-15T10:30:00Z",
            "source": "test",
            "type": "test.event",
            "severity": "debug",
            "outcome": "unknown"
        }"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].message_type, "test.event");
    }

    #[test]
    fn test_full_json_preserved_in_payload() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"{"id":"01P","timestamp":"2024-01-15T10:30:00Z","source":"s","type":"t","severity":"info","outcome":"success","data":{"key":"value"}}"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        // Payload should contain the full JSON
        let payload_str = msgs[0].payload_str().unwrap();
        assert!(payload_str.contains("\"data\""));
        assert!(payload_str.contains("\"key\":\"value\""));
    }

    #[test]
    fn test_content_type_metadata_set() {
        let ingestor = OccurrenceIngestor::new();
        let json = r#"{"id":"01CT","timestamp":"2024-01-15T10:30:00Z","source":"s","type":"t","severity":"info","outcome":"success"}"#;

        let msgs = ingestor.ingest(&ctx(), json.as_bytes()).unwrap();
        assert_eq!(
            msgs[0].metadata().get(metadata_keys::CONTENT_TYPE),
            Some(&"application/json".to_string())
        );
    }
}
