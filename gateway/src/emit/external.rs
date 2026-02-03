//! External emitter - gRPC plugin for custom destinations
//!
//! Delegates event emission to an external gRPC service implementing
//! the EmitterPlugin protocol.

use super::{Emitter, Event, PluginError};
use crate::proto::emitter_plugin_client::EmitterPluginClient;
use crate::proto::{EmitRequest, ShutdownResponse};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use tonic::transport::Channel;

/// External emitter that delegates to a gRPC plugin
///
/// Use this when you need to send events to a destination that isn't
/// supported by built-in emitters. The plugin can be written
/// in any language that supports gRPC.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::emit::ExternalEmitter;
///
/// let hub = Hub::new()
///     .emitter(ExternalEmitter::new("splunk", "http://localhost:9001"))
///     .build();
/// ```
pub struct ExternalEmitter {
    /// Emitter name for identification
    emitter_name: String,
    /// gRPC address of the plugin
    address: String,
    /// Leaked static string for name() return type
    name_static: &'static str,
    /// Cached gRPC client (lazy initialized)
    client: Arc<Mutex<Option<EmitterPluginClient<Channel>>>>,
}

impl ExternalEmitter {
    /// Create a new external emitter
    ///
    /// # Arguments
    /// * `emitter_name` - Name for this emitter (used for logging/routing)
    /// * `address` - gRPC address (e.g., "http://localhost:9001")
    pub fn new(emitter_name: impl Into<String>, address: impl Into<String>) -> Self {
        let emitter_name = emitter_name.into();
        let address = address.into();

        // Leak string to create static reference for name()
        let name = format!("external:{}", emitter_name);
        let name_static: &'static str = Box::leak(name.into_boxed_str());

        Self {
            emitter_name,
            address,
            name_static,
            client: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the plugin address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get the emitter name
    pub fn emitter_name(&self) -> &str {
        &self.emitter_name
    }

    /// Get or create the gRPC client
    async fn get_client(&self) -> Result<EmitterPluginClient<Channel>, PluginError> {
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

        let client = EmitterPluginClient::new(channel);

        // Cache the client
        {
            let mut guard = self.client.lock();
            *guard = Some(client.clone());
        }

        Ok(client)
    }

    /// Clear cached client (for reconnection on failure)
    fn clear_client(&self) {
        let mut guard = self.client.lock();
        *guard = None;
    }
}

#[async_trait]
impl Emitter for ExternalEmitter {
    fn name(&self) -> &'static str {
        self.name_static
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        let mut client = self.get_client().await?;

        let request = EmitRequest {
            events: events.to_vec(),
        };

        let response = client.emit(request).await.map_err(|status| {
            // Clear cached client on error (it might be stale)
            self.clear_client();
            PluginError::Send(format!("Plugin error: {}", status.message()))
        })?;

        let resp = response.into_inner();

        // Check if any events failed
        if !resp.failed_event_ids.is_empty() {
            let error_messages: Vec<String> = resp
                .errors
                .iter()
                .map(|e| format!("{}: {}", e.event_id, e.message))
                .collect();

            return Err(PluginError::Send(format!(
                "Failed to emit {} events: {}",
                resp.failed_event_ids.len(),
                error_messages.join(", ")
            )));
        }

        Ok(())
    }

    async fn health(&self) -> bool {
        let client_result = self.get_client().await;
        let mut client = match client_result {
            Ok(c) => c,
            Err(_) => return false,
        };

        match client.health(()).await {
            Ok(response) => response.into_inner().healthy,
            Err(_) => {
                self.clear_client();
                false
            }
        }
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        let client_result = self.get_client().await;
        let mut client = match client_result {
            Ok(c) => c,
            Err(e) => return Err(PluginError::Shutdown(format!("Connection failed: {}", e))),
        };

        let response: ShutdownResponse = client
            .shutdown(())
            .await
            .map_err(|status| {
                self.clear_client();
                PluginError::Shutdown(format!("Shutdown failed: {}", status.message()))
            })?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            let msg = if response.message.is_empty() {
                "Unknown error".to_string()
            } else {
                response.message
            };
            Err(PluginError::Shutdown(msg))
        }
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::proto::emitter_plugin_server::{EmitterPlugin, EmitterPluginServer};
    use crate::proto::{
        EmitError, EmitRequest, EmitResponse, PluginHealthResponse, PluginInfo, ShutdownResponse,
    };
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    // =========================================================================
    // Mock Plugin Server
    // =========================================================================

    /// Mock emitter plugin for testing
    struct MockEmitterPlugin {
        /// Number of events successfully emitted (per call)
        emit_count: AtomicU32,
        /// If set, fail these event IDs
        fail_event_ids: Vec<String>,
        /// Health status
        healthy: AtomicBool,
        /// Shutdown called flag
        shutdown_called: AtomicBool,
        /// If set, return this error
        error_message: Option<String>,
    }

    impl MockEmitterPlugin {
        fn new() -> Self {
            Self {
                emit_count: AtomicU32::new(0),
                fail_event_ids: vec![],
                healthy: AtomicBool::new(true),
                shutdown_called: AtomicBool::new(false),
                error_message: None,
            }
        }

        fn with_failing_events(event_ids: Vec<String>) -> Self {
            Self {
                emit_count: AtomicU32::new(0),
                fail_event_ids: event_ids,
                healthy: AtomicBool::new(true),
                shutdown_called: AtomicBool::new(false),
                error_message: None,
            }
        }

        fn with_error(message: impl Into<String>) -> Self {
            Self {
                emit_count: AtomicU32::new(0),
                fail_event_ids: vec![],
                healthy: AtomicBool::new(true),
                shutdown_called: AtomicBool::new(false),
                error_message: Some(message.into()),
            }
        }

        fn unhealthy() -> Self {
            Self {
                emit_count: AtomicU32::new(0),
                fail_event_ids: vec![],
                healthy: AtomicBool::new(false),
                shutdown_called: AtomicBool::new(false),
                error_message: None,
            }
        }
    }

    #[tonic::async_trait]
    impl EmitterPlugin for MockEmitterPlugin {
        async fn info(&self, _: Request<()>) -> Result<Response<PluginInfo>, Status> {
            Ok(Response::new(PluginInfo {
                name: "mock-emitter".to_string(),
                version: "1.0.0".to_string(),
                r#type: 2, // EMITTER
                description: "Test emitter plugin".to_string(),
                sources: vec![],
                emitter_name: "mock".to_string(),
                capabilities: vec!["batch".to_string()],
            }))
        }

        async fn health(&self, _: Request<()>) -> Result<Response<PluginHealthResponse>, Status> {
            Ok(Response::new(PluginHealthResponse {
                healthy: self.healthy.load(Ordering::Relaxed),
                message: "OK".to_string(),
                components: Default::default(),
            }))
        }

        async fn emit(
            &self,
            request: Request<EmitRequest>,
        ) -> Result<Response<EmitResponse>, Status> {
            // Check if we should return an error
            if let Some(ref msg) = self.error_message {
                return Err(Status::internal(msg.clone()));
            }

            let req = request.into_inner();
            self.emit_count.fetch_add(1, Ordering::SeqCst);

            // Check for events that should fail
            let failed_ids: Vec<String> = req
                .events
                .iter()
                .filter(|e| self.fail_event_ids.contains(&e.id))
                .map(|e| e.id.clone())
                .collect();

            let errors: Vec<EmitError> = failed_ids
                .iter()
                .map(|id| EmitError {
                    event_id: id.clone(),
                    message: "Simulated failure".to_string(),
                    retryable: true,
                })
                .collect();

            let success_count = (req.events.len() - failed_ids.len()) as i64;

            Ok(Response::new(EmitResponse {
                success_count,
                failed_event_ids: failed_ids,
                errors,
            }))
        }

        async fn shutdown(&self, _: Request<()>) -> Result<Response<ShutdownResponse>, Status> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Ok(Response::new(ShutdownResponse {
                success: true,
                message: "Shutdown complete".to_string(),
            }))
        }
    }

    /// Start a mock emitter plugin server
    async fn start_mock_server(plugin: MockEmitterPlugin) -> (SocketAddr, oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(EmitterPluginServer::new(plugin))
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
    fn test_external_emitter_name_format() {
        let emitter = ExternalEmitter::new("splunk", "http://localhost:9001");
        assert_eq!(emitter.name(), "external:splunk");
    }

    #[test]
    fn test_external_emitter_address() {
        let emitter = ExternalEmitter::new("datadog", "http://plugin.internal:9002");
        assert_eq!(emitter.address(), "http://plugin.internal:9002");
    }

    #[test]
    fn test_external_emitter_emitter_name() {
        let emitter = ExternalEmitter::new("kafka", "http://localhost:9001");
        assert_eq!(emitter.emitter_name(), "kafka");
    }

    // =========================================================================
    // Integration Tests (with mock server)
    // =========================================================================

    #[tokio::test]
    async fn test_successful_emit() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::new()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));

        let events = vec![
            Event {
                id: "evt-1".to_string(),
                source: "test".to_string(),
                event_type: "test.event".to_string(),
                ..Default::default()
            },
            Event {
                id: "evt-2".to_string(),
                source: "test".to_string(),
                event_type: "test.event".to_string(),
                ..Default::default()
            },
        ];

        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
    }

    #[tokio::test]
    async fn test_emit_empty_batch() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::new()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));
        let result = emitter.emit(&[]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_connection_failure() {
        // No server running at this address
        let emitter = ExternalEmitter::new("test", "http://127.0.0.1:59998");

        let events = vec![Event {
            id: "evt-1".to_string(),
            ..Default::default()
        }];

        let result = emitter.emit(&events).await;
        assert!(result.is_err());

        match result {
            Err(PluginError::Connection(msg)) => {
                assert!(msg.contains("127.0.0.1:59998") || msg.contains("connect"));
            }
            Err(e) => panic!("Expected Connection error, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_partial_failure() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::with_failing_events(vec![
            "evt-2".to_string(),
        ]))
        .await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));

        let events = vec![
            Event {
                id: "evt-1".to_string(),
                ..Default::default()
            },
            Event {
                id: "evt-2".to_string(),
                ..Default::default()
            },
        ];

        let result = emitter.emit(&events).await;
        assert!(result.is_err());

        match result {
            Err(PluginError::Send(msg)) => {
                assert!(msg.contains("evt-2"));
                assert!(msg.contains("1 events")); // 1 failed
            }
            Err(e) => panic!("Expected Send error, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_plugin_error_propagates() {
        let (addr, _shutdown) =
            start_mock_server(MockEmitterPlugin::with_error("Destination unavailable")).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));

        let events = vec![Event {
            id: "evt-1".to_string(),
            ..Default::default()
        }];

        let result = emitter.emit(&events).await;
        assert!(result.is_err());

        match result {
            Err(PluginError::Send(msg)) => {
                assert!(msg.contains("Destination unavailable"));
            }
            Err(e) => panic!("Expected Send error, got: {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_health_check_healthy() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::new()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));
        assert!(emitter.health().await);
    }

    #[tokio::test]
    async fn test_health_check_unhealthy() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::unhealthy()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));
        assert!(!emitter.health().await);
    }

    #[tokio::test]
    async fn test_health_check_no_connection() {
        let emitter = ExternalEmitter::new("test", "http://127.0.0.1:59997");
        assert!(!emitter.health().await);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::new()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));

        // First make a call to establish connection
        let _ = emitter.health().await;

        let result = emitter.shutdown().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_calls_reuse_connection() {
        let (addr, _shutdown) = start_mock_server(MockEmitterPlugin::new()).await;

        let emitter = ExternalEmitter::new("test", format!("http://{}", addr));

        for i in 0..3 {
            let events = vec![Event {
                id: format!("evt-{}", i),
                ..Default::default()
            }];
            let result = emitter.emit(&events).await;
            assert!(result.is_ok(), "Call {} failed: {:?}", i, result);
        }
    }
}
