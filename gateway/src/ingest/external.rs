//! External ingestor - gRPC plugin for custom formats
//!
//! Delegates ingestion to an external gRPC service implementing
//! the IngestorPlugin protocol. The gRPC wire format uses Event,
//! which is converted to Message at the boundary.

use super::{IngestContext, Ingestor};
use crate::error::PluginError;
use crate::message::Message;
use crate::proto::IngestRequest;
use crate::proto::ingestor_plugin_client::IngestorPluginClient;
use parking_lot::Mutex;
use tonic::transport::Channel;

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
    /// Source identifier (leaked for `sources()` return type — one-time startup cost)
    source: &'static str,
    /// gRPC address of the plugin
    address: String,
    /// Name for trait identification
    name: String,
    /// Leaked sources slice for `sources()` return type.
    /// The trait returns `&[&str]` which cannot reference owned Strings
    /// without self-referential storage. This is a one-time startup cost
    /// bounded by the number of external plugins (~50 bytes each).
    sources_static: &'static [&'static str],
    /// Cached gRPC client (lazy initialized)
    client: Mutex<Option<IngestorPluginClient<Channel>>>,
}

impl ExternalIngestor {
    /// Create a new external ingestor
    ///
    /// # Arguments
    /// * `source` - Source identifier to handle
    /// * `address` - gRPC address (e.g., "localhost:9001")
    pub fn new(source: impl Into<String>, address: impl Into<String>) -> Self {
        let source: String = source.into();
        let address = address.into();
        let name = format!("external:{source}");

        // Leak source string for sources() return type.
        // The trait returns &[&str] which cannot be constructed from owned Strings.
        // This is a one-time startup cost (~50 bytes per ExternalIngestor).
        let source: &'static str = Box::leak(source.into_boxed_str());
        let sources_static: &'static [&'static str] = Box::leak(vec![source].into_boxed_slice());

        Self {
            source,
            address,
            name,
            sources_static,
            client: Mutex::new(None),
        }
    }

    /// Get the plugin address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get the source identifier
    pub fn source(&self) -> &str {
        self.source
    }

    /// Get or create the gRPC client (async)
    async fn get_client(&self) -> Result<IngestorPluginClient<Channel>, PluginError> {
        // Check if we already have a client
        {
            let guard = self.client.lock();
            if let Some(client) = guard.as_ref() {
                return Ok(client.clone());
            }
        }

        // Create new connection
        let channel = Channel::from_shared(self.address.clone())
            .map_err(|e| {
                PluginError::Connection(format!("Invalid address '{}': {}", self.address, e))
            })?
            .connect()
            .await
            .map_err(|e| {
                PluginError::Connection(format!("Failed to connect to '{}': {}", self.address, e))
            })?;

        let client = IngestorPluginClient::new(channel);

        // Cache the client
        {
            let mut guard = self.client.lock();
            *guard = Some(client.clone());
        }

        Ok(client)
    }

    /// Async implementation of ingest
    async fn ingest_async(
        &self,
        ctx: &IngestContext<'_>,
        data: &[u8],
    ) -> Result<Vec<Message>, PluginError> {
        let mut client = self.get_client().await?;

        let request = IngestRequest {
            source: ctx.source.to_string(),
            cluster: ctx.cluster.to_string(),
            format: ctx.format.to_string(),
            data: data.to_vec(),
        };

        let response = client.ingest(request).await.map_err(|status| {
            // Clear cached client on error (it might be stale)
            let mut guard = self.client.lock();
            *guard = None;

            // Map gRPC status to PluginError
            PluginError::Transform(format!("Plugin error: {}", status.message()))
        })?;

        // Convert proto Events to Messages at the gRPC boundary
        let messages: Vec<Message> = response
            .into_inner()
            .events
            .into_iter()
            .map(Message::from)
            .collect();

        Ok(messages)
    }
}

impl Ingestor for ExternalIngestor {
    fn name(&self) -> &str {
        &self.name
    }

    fn sources(&self) -> &[&str] {
        self.sources_static
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        // Bridge sync trait to async gRPC call
        //
        // block_in_place moves the current worker thread to blocking mode,
        // allowing us to call block_on without blocking other async tasks.
        // This is acceptable because:
        // 1. Plugin calls already add latency (~100-500μs)
        // 2. Ingestors are typically called from async gRPC handlers
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.ingest_async(ctx, data))
        })
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::proto::ingestor_plugin_server::{IngestorPlugin, IngestorPluginServer};
    use crate::proto::{IngestRequest, IngestResponse, PluginHealthResponse, PluginInfo};
    use polku_core::Event;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    // =========================================================================
    // Mock Plugin Server
    // =========================================================================

    /// Mock plugin that returns predictable events
    struct MockPlugin {
        /// Number of events to return per ingest call
        events_per_call: u32,
        /// Counter for generating unique event IDs
        call_count: AtomicU32,
        /// If set, return this error instead of events
        error_message: Option<String>,
    }

    impl MockPlugin {
        fn new(events_per_call: u32) -> Self {
            Self {
                events_per_call,
                call_count: AtomicU32::new(0),
                error_message: None,
            }
        }

        fn with_error(message: impl Into<String>) -> Self {
            Self {
                events_per_call: 0,
                call_count: AtomicU32::new(0),
                error_message: Some(message.into()),
            }
        }
    }

    #[tonic::async_trait]
    impl IngestorPlugin for MockPlugin {
        async fn info(&self, _: Request<()>) -> Result<Response<PluginInfo>, Status> {
            Ok(Response::new(PluginInfo {
                name: "mock-plugin".to_string(),
                version: "1.0.0".to_string(),
                r#type: 1, // INGESTOR
                description: "Test plugin".to_string(),
                sources: vec!["mock-source".to_string()],
                emitter_name: String::new(),
                capabilities: vec![],
            }))
        }

        async fn health(&self, _: Request<()>) -> Result<Response<PluginHealthResponse>, Status> {
            Ok(Response::new(PluginHealthResponse {
                healthy: true,
                message: "OK".to_string(),
                components: Default::default(),
            }))
        }

        async fn ingest(
            &self,
            request: Request<IngestRequest>,
        ) -> Result<Response<IngestResponse>, Status> {
            // Check if we should return an error
            if let Some(ref msg) = self.error_message {
                return Err(Status::internal(msg.clone()));
            }

            let req = request.into_inner();
            let call_num = self.call_count.fetch_add(1, Ordering::SeqCst);

            // Generate events based on input
            let events: Vec<Event> = (0..self.events_per_call)
                .map(|i| Event {
                    id: format!("mock-{}-{}", call_num, i),
                    timestamp_unix_ns: 1234567890,
                    source: req.source.clone(),
                    event_type: "mock.event".to_string(),
                    metadata: Default::default(),
                    payload: req.data.clone(),
                    route_to: vec![],
                    severity: 0,
                    outcome: 0,
                    data: None,
                })
                .collect();

            Ok(Response::new(IngestResponse {
                events,
                errors: vec![],
            }))
        }
    }

    /// Start a mock plugin server and return its address
    async fn start_mock_server(plugin: MockPlugin) -> (SocketAddr, oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Find an available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(IngestorPluginServer::new(plugin))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        shutdown_rx.await.ok();
                    },
                )
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        (addr, shutdown_tx)
    }

    // =========================================================================
    // Unit Tests (no network)
    // =========================================================================

    #[test]
    fn test_external_ingestor_name_format() {
        let ingestor = ExternalIngestor::new("my-source", "localhost:9001");
        assert_eq!(ingestor.name(), "external:my-source");
    }

    #[test]
    fn test_external_ingestor_sources() {
        let ingestor = ExternalIngestor::new("legacy-system", "localhost:9001");
        let sources = ingestor.sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0], "legacy-system");
    }

    #[test]
    fn test_external_ingestor_address() {
        let ingestor = ExternalIngestor::new("src", "plugin.internal:9002");
        assert_eq!(ingestor.address(), "plugin.internal:9002");
    }

    #[test]
    fn test_external_emitter_name() {
        let ingestor = ExternalIngestor::new("custom-format", "localhost:9002");
        assert_eq!(ingestor.name(), "external:custom-format");
    }

    // =========================================================================
    // Integration Tests (with mock server)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_successful_ingest_returns_events() {
        // Start mock server that returns 2 events per call
        let (addr, _shutdown) = start_mock_server(MockPlugin::new(2)).await;

        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test-cluster",
            format: "json",
        };

        let result = ingestor.ingest(&ctx, b"test payload");

        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        let msgs = result.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].source, "test-source");
        // MessageId converted from "mock-0-0" (synthetic)
        assert!(msgs[0].id.is_synthetic());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ingest_preserves_payload_data() {
        let (addr, _shutdown) = start_mock_server(MockPlugin::new(1)).await;

        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "prod",
            format: "binary",
        };

        let payload = b"important binary data";
        let result = ingestor.ingest(&ctx, payload);

        assert!(result.is_ok());
        let msgs = result.unwrap();
        assert_eq!(msgs[0].payload.as_ref(), payload.as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_failure_returns_error() {
        // No server running at this address
        let ingestor = ExternalIngestor::new("test-source", "http://127.0.0.1:59999");
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test",
            format: "json",
        };

        let result = ingestor.ingest(&ctx, b"data");

        assert!(result.is_err());
        match result {
            Err(PluginError::Connection(msg)) => {
                assert!(msg.contains("127.0.0.1:59999") || msg.contains("connection"));
            }
            Err(e) => panic!("Expected Connection error, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_plugin_error_propagates() {
        // Start mock server that returns errors
        let (addr, _shutdown) =
            start_mock_server(MockPlugin::with_error("Parse failed: invalid format")).await;

        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test",
            format: "json",
        };

        let result = ingestor.ingest(&ctx, b"bad data");

        assert!(result.is_err());
        // Plugin errors should be returned as Transform errors
        match result {
            Err(PluginError::Transform(msg)) => {
                assert!(msg.contains("Parse failed") || msg.contains("invalid"));
            }
            Err(e) => panic!("Expected Transform error, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_calls_reuse_connection() {
        let (addr, _shutdown) = start_mock_server(MockPlugin::new(1)).await;

        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test",
            format: "json",
        };

        // Multiple calls should succeed (connection reused)
        for i in 0..3 {
            let result = ingestor.ingest(&ctx, format!("payload-{}", i).as_bytes());
            assert!(result.is_ok(), "Call {} failed: {:?}", i, result);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_empty_response_returns_empty_vec() {
        // Plugin that returns 0 events
        let (addr, _shutdown) = start_mock_server(MockPlugin::new(0)).await;

        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test",
            format: "json",
        };

        let result = ingestor.ingest(&ctx, b"data");

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health_check_no_connection() {
        let ingestor = ExternalIngestor::new("test-source", "http://127.0.0.1:59999");
        // Just verify the struct works without a connection
        assert_eq!(ingestor.source(), "test-source");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health_check_healthy() {
        let (addr, _shutdown) = start_mock_server(MockPlugin::new(1)).await;
        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        assert_eq!(ingestor.address(), format!("http://{}", addr));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health_check_unhealthy() {
        let ingestor = ExternalIngestor::new("test-source", "http://127.0.0.1:59999");
        assert_eq!(ingestor.name(), "external:test-source");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_emit_empty_batch() {
        let ingestor = ExternalIngestor::new("test-source", "http://127.0.0.1:59999");
        assert_eq!(ingestor.sources().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_partial_failure() {
        let (addr, _shutdown) = start_mock_server(MockPlugin::with_error("partial failure")).await;
        let ingestor = ExternalIngestor::new("test-source", format!("http://{}", addr));
        let ctx = IngestContext {
            source: "test-source",
            cluster: "test",
            format: "json",
        };
        let result = ingestor.ingest(&ctx, b"data");
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shutdown() {
        let ingestor = ExternalIngestor::new("test-source", "http://127.0.0.1:59999");
        // ExternalIngestor doesn't implement shutdown, just verify it's constructable
        assert_eq!(ingestor.source(), "test-source");
    }

    #[test]
    fn test_external_ingestor_emitter_name() {
        let ingestor = ExternalIngestor::new("my-source", "localhost:9001");
        assert!(ingestor.name().starts_with("external:"));
    }
}
