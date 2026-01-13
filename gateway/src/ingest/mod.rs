//! Ingestor system for POLKU
//!
//! Ingestors transform raw bytes from various protocols into typed Events.
//! Each source registers an Ingestor that knows how to deserialize that format.
//!
//! # Architecture
//!
//! ```text
//! Raw bytes ──► Ingestor ──► Vec<Event>
//!               (lookup by source)
//! ```
//!
//! # Built-in Ingestors
//!
//! - `PassthroughIngestor` - events already in polku.Event format
//! - `JsonIngestor` - JSON matching Event schema
//!
//! # Plugin Ingestors
//!
//! External ingestors communicate via gRPC using the IngestorPlugin service.

mod passthrough;
mod json;
mod external;

pub use passthrough::PassthroughIngestor;
pub use json::JsonIngestor;
pub use external::ExternalIngestor;

use crate::emit::Event;
use crate::error::PluginError;

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

/// Ingestor trait - transforms raw bytes into typed Events
///
/// Ingestors are the core abstraction for format transformation in Polku.
/// They take raw bytes from a source and produce typed Events that flow
/// through the middleware pipeline.
///
/// # Implementing an Ingestor
///
/// ```ignore
/// use polku_gateway::ingest::{Ingestor, IngestContext};
/// use polku_gateway::emit::Event;
/// use polku_gateway::error::PluginError;
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
///     fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
///         // Parse data and return Events
///         todo!()
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

    /// Transform raw bytes into Events
    ///
    /// # Arguments
    /// * `ctx` - Context with source, cluster, and format information
    /// * `data` - Raw bytes from the source
    ///
    /// # Returns
    /// Vector of Events or a PluginError
    ///
    /// # Errors
    /// - `PluginError::Decode` - if data cannot be parsed
    /// - `PluginError::Transform` - if transformation logic fails
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ==========================================================================
    // Ingestor trait tests
    // ==========================================================================

    struct TestIngestor;

    impl Ingestor for TestIngestor {
        fn name(&self) -> &'static str {
            "test-ingestor"
        }

        fn sources(&self) -> &'static [&'static str] {
            &["test-source", "test-alias"]
        }

        fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
            Ok(vec![Event {
                id: format!("{}:{}", ctx.source, data.len()),
                timestamp_unix_ns: 0,
                source: ctx.source.to_string(),
                event_type: "test.event".to_string(),
                metadata: Default::default(),
                payload: data.to_vec(),
                route_to: vec![],
                severity: 0,
                outcome: 0,
                data: None,
            }])
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

        let events = ingestor.ingest(&ctx, b"hello world").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, "test-source");
        assert_eq!(events[0].id, "test-source:11");
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
        use prost::Message;

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
            data: None,
        };
        let data = event.encode_to_vec();

        let events = ingestor.ingest(&ctx, &data).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "evt-123");
        assert_eq!(events[0].source, "my-app");
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

        let events = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "evt-456");
        assert_eq!(events[0].source, "my-service");
        assert_eq!(events[0].event_type, "order.placed");
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

        let events = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, "evt-1");
        assert_eq!(events[1].id, "evt-2");
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

        let events = ingestor.ingest(&ctx, ndjson.as_bytes()).unwrap();
        assert_eq!(events.len(), 3);
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

        let events = ingestor.ingest(&ctx, json.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(!events[0].id.is_empty());
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
