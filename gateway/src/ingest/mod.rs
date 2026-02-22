//! Ingestor system for POLKU
//!
//! Ingestors transform raw bytes from various protocols into Messages.
//! Each source registers an Ingestor that knows how to deserialize that format.
//!
//! # Architecture
//!
//! ```text
//! Raw bytes ──► Ingestor ──► Vec<Message>
//!               (lookup by source)
//! ```
//!
//! # Built-in Ingestors
//!
//! - `PassthroughIngestor` - events already in polku.Event proto format
//! - `JsonIngestor` - JSON matching Event schema
//!
//! # Plugin Ingestors
//!
//! External ingestors communicate via gRPC using the IngestorPlugin service.

mod external;
mod json;
mod passthrough;
mod registry;

pub use external::ExternalIngestor;
pub use json::JsonIngestor;
pub use passthrough::PassthroughIngestor;
pub use registry::IngestorRegistry;

use crate::error::PluginError;
use crate::message::Message;

/// Context for ingestion
#[derive(Debug, Clone)]
pub struct IngestContext<'a> {
    /// Source identifier (e.g., "tapio", "portti", "my-agent")
    pub source: &'a str,
    /// Cluster/environment identifier
    pub cluster: &'a str,
    /// Format hint (e.g., "protobuf", "json", "msgpack")
    pub format: &'a str,
}

/// Ingestor trait - transforms raw bytes into Messages
///
/// Ingestors are the core abstraction for format transformation in Polku.
/// They take raw bytes from a source and produce Messages that flow
/// through the middleware pipeline.
///
/// # Implementing an Ingestor
///
/// ```ignore
/// use polku_gateway::ingest::{Ingestor, IngestContext};
/// use polku_gateway::message::Message;
/// use polku_gateway::error::PluginError;
/// use bytes::Bytes;
///
/// struct MyIngestor;
///
/// impl Ingestor for MyIngestor {
///     fn name(&self) -> &'static str {
///         "my-ingestor"
///     }
///
///     fn sources(&self) -> &'static [&'static str] {
///         &["my-source", "my-source-v2"]
///     }
///
///     fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
///         // Parse data and return Messages
///         Ok(vec![Message::new(ctx.source, "my.event", Bytes::copy_from_slice(data))])
///     }
/// }
/// ```
pub trait Ingestor: Send + Sync {
    /// Unique name for this ingestor (for logging and metrics)
    fn name(&self) -> &'static str;

    /// Source identifiers this ingestor handles
    ///
    /// When raw data arrives with a matching source, this ingestor is called.
    /// Multiple sources can map to the same ingestor (e.g., version aliases).
    fn sources(&self) -> &'static [&'static str];

    /// Transform raw bytes into Messages
    ///
    /// # Arguments
    /// * `ctx` - Context with source, cluster, and format information
    /// * `data` - Raw bytes from the source
    ///
    /// # Returns
    /// Vector of Messages or a PluginError
    ///
    /// # Errors
    /// - `PluginError::Transform` - if data cannot be parsed or transformation fails
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ==========================================================================
    // Ingestor trait tests
    // ==========================================================================

    use bytes::Bytes;

    struct TestIngestor;

    impl Ingestor for TestIngestor {
        fn name(&self) -> &'static str {
            "test-ingestor"
        }

        fn sources(&self) -> &'static [&'static str] {
            &["test-source", "test-alias"]
        }

        fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
            Ok(vec![Message::with_id(
                format!("{}:{}", ctx.source, data.len()),
                0,
                ctx.source,
                "test.event",
                Bytes::copy_from_slice(data),
            )])
        }
    }

    #[test]
    fn test_ingestor_name() {
        let ingestor = TestIngestor;
        assert_eq!(ingestor.name(), "test-ingestor");
    }

    #[test]
    fn test_ingestor_sources() {
        let ingestor = TestIngestor;
        let sources = ingestor.sources();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"test-source"));
        assert!(sources.contains(&"test-alias"));
    }

    #[test]
    fn test_ingestor_ingest() {
        let ingestor = TestIngestor;
        let ctx = IngestContext {
            source: "test-source",
            cluster: "prod",
            format: "json",
        };

        let msgs = ingestor.ingest(&ctx, b"hello world").unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].source, "test-source");
        assert_eq!(msgs[0].id, "test-source:11");
    }

    // ==========================================================================
    // PassthroughIngestor tests
    // ==========================================================================

    #[test]
    fn test_passthrough_ingestor_sources() {
        let ingestor = PassthroughIngestor::new();
        assert!(ingestor.sources().contains(&"passthrough"));
    }

    #[test]
    fn test_passthrough_ingestor_valid_protobuf() {
        use polku_core::Event;

        let ingestor = PassthroughIngestor::new();
        let ctx = IngestContext {
            source: "passthrough",
            cluster: "prod",
            format: "protobuf",
        };

        // Create a valid Event protobuf
        let event = Event {
            id: "evt-123".to_string(),
            timestamp_unix_ns: 1234567890,
            source: "my-app".to_string(),
            event_type: "user.created".to_string(),
            metadata: Default::default(),
            payload: b"test payload".to_vec(),
            route_to: vec![],
            severity: 0,
            outcome: 0,
        };
        let data = prost::Message::encode_to_vec(&event);

        let msgs = ingestor.ingest(&ctx, &data).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "evt-123");
        assert_eq!(msgs[0].source, "my-app");
    }

    #[test]
    fn test_passthrough_ingestor_preserves_severity() {
        use polku_core::Event;

        let ingestor = PassthroughIngestor::new();
        let ctx = IngestContext {
            source: "passthrough",
            cluster: "prod",
            format: "protobuf",
        };

        // Create an Event with severity and outcome set
        let event = Event {
            id: "sev-1".to_string(),
            timestamp_unix_ns: 1000,
            source: "tapio".to_string(),
            event_type: "network.connection".to_string(),
            metadata: Default::default(),
            payload: b"data".to_vec(),
            route_to: vec![],
            severity: 4, // ERROR
            outcome: 2,  // FAILURE
        };
        let data = prost::Message::encode_to_vec(&event);

        let msgs = ingestor.ingest(&ctx, &data).unwrap();
        assert_eq!(msgs.len(), 1);

        // Bug #5: PassthroughIngestor uses From<Event> which drops severity/outcome
        assert_eq!(
            msgs[0].metadata().get(polku_core::metadata_keys::SEVERITY),
            Some(&"4".to_string()),
            "PassthroughIngestor must preserve Event severity in metadata"
        );
        assert_eq!(
            msgs[0].metadata().get(polku_core::metadata_keys::OUTCOME),
            Some(&"2".to_string()),
            "PassthroughIngestor must preserve Event outcome in metadata"
        );
    }

    #[test]
    fn test_passthrough_ingestor_invalid_protobuf() {
        let ingestor = PassthroughIngestor::new();
        let ctx = IngestContext {
            source: "passthrough",
            cluster: "prod",
            format: "protobuf",
        };

        let result = ingestor.ingest(&ctx, b"not valid protobuf");
        assert!(result.is_err());
    }

    // ==========================================================================
    // JsonIngestor tests
    // ==========================================================================

    #[test]
    fn test_json_ingestor_sources() {
        let ingestor = JsonIngestor::new();
        let sources = ingestor.sources();
        assert!(sources.contains(&"json"));
    }

    #[test]
    fn test_json_ingestor_single_event() {
        let ingestor = JsonIngestor::new();
        let ctx = IngestContext {
            source: "json",
            cluster: "prod",
            format: "json",
        };

        let json = r#"{
            "id": "evt-456",
            "source": "my-service",
            "event_type": "order.placed",
            "payload": "dGVzdA=="
        }"#;

        let msgs = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "evt-456");
        assert_eq!(msgs[0].source, "my-service");
        assert_eq!(msgs[0].message_type, "order.placed");
    }

    #[test]
    fn test_json_ingestor_array_of_events() {
        let ingestor = JsonIngestor::new();
        let ctx = IngestContext {
            source: "json",
            cluster: "prod",
            format: "json",
        };

        let json = r#"[
            {"id": "evt-1", "source": "svc", "event_type": "a"},
            {"id": "evt-2", "source": "svc", "event_type": "b"}
        ]"#;

        let msgs = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].id, "evt-1");
        assert_eq!(msgs[1].id, "evt-2");
    }

    #[test]
    fn test_json_ingestor_newline_delimited() {
        let ingestor = JsonIngestor::new();
        let ctx = IngestContext {
            source: "json",
            cluster: "prod",
            format: "json-lines",
        };

        let ndjson = r#"{"id": "evt-1", "source": "svc", "event_type": "a"}
{"id": "evt-2", "source": "svc", "event_type": "b"}
{"id": "evt-3", "source": "svc", "event_type": "c"}"#;

        let msgs = ingestor.ingest(&ctx, ndjson.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 3);
    }

    #[test]
    fn test_json_ingestor_generates_id_if_missing() {
        let ingestor = JsonIngestor::new();
        let ctx = IngestContext {
            source: "json",
            cluster: "prod",
            format: "json",
        };

        let json = r#"{"source": "svc", "event_type": "test"}"#;

        let msgs = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(msgs.len(), 1);
        // MessageId is always non-empty
        assert!(!msgs[0].id.to_string().is_empty());
    }

    #[test]
    fn test_json_ingestor_invalid_json() {
        let ingestor = JsonIngestor::new();
        let ctx = IngestContext {
            source: "json",
            cluster: "prod",
            format: "json",
        };

        let result = ingestor.ingest(&ctx, b"not valid json");
        assert!(result.is_err());
    }

    // ==========================================================================
    // ExternalIngestor tests
    // ==========================================================================

    #[test]
    fn test_external_ingestor_sources() {
        let ingestor = ExternalIngestor::new("legacy-system", "localhost:9001");
        let sources = ingestor.sources();
        assert!(sources.contains(&"legacy-system"));
    }

    #[test]
    fn test_external_ingestor_name() {
        let ingestor = ExternalIngestor::new("custom-format", "localhost:9002");
        assert_eq!(ingestor.name(), "external:custom-format");
    }

    // Note: Full gRPC integration tests for ExternalIngestor require a running
    // plugin server. See integration tests in tests/external_ingestor_test.rs
}
