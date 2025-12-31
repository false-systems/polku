//! gRPC emitter for forwarding events to another POLKU Gateway
//!
//! Uses the generated GatewayClient to forward events to a remote Gateway service.
//!
//! # Example
//!
//! ```ignore
//! let emitter = GrpcEmitter::new("http://downstream:50051").await?;
//! registry.register_emitter(Arc::new(emitter));
//! ```

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::proto::gateway_client::GatewayClient;
use crate::proto::{Event, HealthRequest, IngestEvent, ingest_event};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error};

/// Default connect timeout (10 seconds)
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default request timeout (30 seconds)
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// gRPC emitter that forwards events to another POLKU Gateway
pub struct GrpcEmitter {
    /// The gRPC client (wrapped in Mutex for interior mutability)
    client: Mutex<GatewayClient<Channel>>,
    /// Target endpoint for logging/debugging
    endpoint: String,
}

impl GrpcEmitter {
    /// Create a new GrpcEmitter connected to the given endpoint
    ///
    /// Uses default timeouts: 10s connect, 30s request.
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC endpoint URL (e.g., "http://localhost:50051")
    pub async fn new(endpoint: impl Into<String>) -> Result<Self, PluginError> {
        let endpoint_str = endpoint.into();

        let channel = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| PluginError::Init(format!("Invalid endpoint URL: {}", e)))?
            .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS))
            .connect()
            .await
            .map_err(|e| {
                PluginError::Connection(format!("Failed to connect to {}: {}", endpoint_str, e))
            })?;

        let client = GatewayClient::new(channel);

        debug!(endpoint = %endpoint_str, "gRPC emitter connected");

        Ok(Self {
            client: Mutex::new(client),
            endpoint: endpoint_str,
        })
    }
}

#[async_trait]
impl Emitter for GrpcEmitter {
    fn name(&self) -> &'static str {
        "grpc"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        if events.is_empty() {
            return Ok(());
        }

        // Clone client and release lock immediately - tonic clients are cheap to clone
        // (just Arc increment) and this allows concurrent emit() calls
        let base_client = self.client.lock().await.clone();
        let endpoint = self.endpoint.clone();

        let mut handles = Vec::with_capacity(events.len());

        for event in events.iter().cloned() {
            let mut client = base_client.clone();
            let endpoint = endpoint.clone();

            let handle = tokio::spawn(async move {
                let request = IngestEvent {
                    source: event.source.clone(),
                    cluster: String::new(),
                    format: String::new(),
                    payload: Some(ingest_event::Payload::Event(event.clone())),
                };

                match client.send_event(request).await {
                    Ok(response) => {
                        let ack = response.into_inner();
                        debug!(
                            event_id = %event.id,
                            acked = ack.event_ids.len(),
                            "Event forwarded"
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            event_id = %event.id,
                            error = %e,
                            endpoint = %endpoint,
                            "Failed to forward event"
                        );
                        Err(PluginError::Send(format!(
                            "Failed to forward event {}: {}",
                            event.id, e
                        )))
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            // If the task panics or is cancelled, map it to a PluginError
            let result = handle
                .await
                .map_err(|e| PluginError::Send(format!("Failed to join send_event task: {}", e)))?;
            // Propagate any send_event error
            result?;
        }
        Ok(())
    }

    async fn health(&self) -> bool {
        let mut client = self.client.lock().await.clone();
        match client.health(HealthRequest {}).await {
            Ok(response) => response.into_inner().healthy,
            Err(e) => {
                debug!(endpoint = %self.endpoint, error = %e, "Health check failed");
                false
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::buffer::RingBuffer;
    use crate::server::GatewayService;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tonic::transport::Server;

    /// Helper to create a test event
    fn make_event(id: &str, event_type: &str) -> Event {
        Event {
            id: id.to_string(),
            timestamp_unix_ns: 1234567890,
            source: "test-source".to_string(),
            event_type: event_type.to_string(),
            metadata: HashMap::new(),
            payload: vec![1, 2, 3],
            route_to: vec![],
        }
    }

    /// Start a test gateway server, return its address
    async fn start_test_server() -> (SocketAddr, Arc<RingBuffer>) {
        let buffer = Arc::new(RingBuffer::new(100));
        let service = GatewayService::new(Arc::clone(&buffer));

        // Bind to random available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        (addr, buffer)
    }

    #[tokio::test]
    async fn test_grpc_emitter_connects() {
        let (addr, _buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await;
        assert!(emitter.is_ok(), "Should connect to server");
    }

    #[tokio::test]
    async fn test_grpc_emitter_emits_events() {
        let (addr, buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await.unwrap();

        let events = vec![
            make_event("e1", "test.created"),
            make_event("e2", "test.updated"),
        ];

        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Should emit events successfully");

        // Verify events landed in the server's buffer
        // Give a moment for async processing
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert_eq!(buffer.len(), 2, "Server should have received 2 events");
    }

    #[tokio::test]
    async fn test_grpc_emitter_health_check() {
        let (addr, _buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await.unwrap();

        assert!(emitter.health().await, "Health check should pass");
    }

    #[tokio::test]
    async fn test_grpc_emitter_handles_empty_events() {
        let (addr, _buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await.unwrap();

        let result = emitter.emit(&[]).await;
        assert!(result.is_ok(), "Should handle empty event slice");
    }

    #[tokio::test]
    async fn test_grpc_emitter_connect_failure() {
        // Try to connect to a non-existent server; this should fail eagerly.
        let result = GrpcEmitter::new("http://127.0.0.1:1").await;
        assert!(
            result.is_err(),
            "Connecting to an unavailable gRPC endpoint should fail"
        );
    }

    #[tokio::test]
    async fn test_grpc_emitter_concurrent_emits() {
        let (addr, buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = Arc::new(GrpcEmitter::new(&endpoint).await.unwrap());

        // Spawn multiple concurrent emit tasks
        let mut handles = vec![];
        for i in 0..5 {
            let emitter = Arc::clone(&emitter);
            let events = vec![make_event(&format!("concurrent-{}", i), "test.concurrent")];
            handles.push(tokio::spawn(async move { emitter.emit(&events).await }));
        }

        // All should complete without blocking each other
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Concurrent emit should succeed");
        }

        // All events should have arrived
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(buffer.len(), 5, "All 5 concurrent events should arrive");
    }
}
