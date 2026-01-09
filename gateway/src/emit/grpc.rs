//! gRPC emitter for forwarding events to another POLKU Gateway
//!
//! Uses batch streaming for high-throughput event forwarding. Supports intelligent
//! load balancing that uses actual downstream buffer pressure (from Ack responses)
//! to route traffic to the least loaded backend.
//!
//! # Features
//!
//! - **Batch streaming**: Uses `stream_events` RPC for high throughput
//! - **Least-loaded routing**: Picks endpoint with lowest buffer fill ratio
//! - **Health-aware failover**: Skips endpoints after consecutive failures
//! - **Backpressure signals**: Learns actual load from Ack responses
//!
//! # Example
//!
//! ```ignore
//! // Single endpoint
//! let emitter = GrpcEmitter::new("http://downstream:50051").await?;
//!
//! // Multiple endpoints with least-loaded balancing
//! let emitter = GrpcEmitter::with_endpoints(vec![
//!     "http://polku-1:50051",
//!     "http://polku-2:50051",
//!     "http://polku-3:50051",
//! ]).await?;
//! ```

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::proto::gateway_client::GatewayClient;
use crate::proto::{Event, EventPayload, HealthRequest, IngestBatch, ingest_batch};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

/// Default connect timeout (10 seconds)
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default request timeout (30 seconds)
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// How long to mark an endpoint unhealthy after consecutive failures (5 seconds)
const UNHEALTHY_DURATION_MS: u64 = 5000;

/// Number of consecutive failures before marking endpoint unhealthy
const FAILURE_THRESHOLD: u32 = 3;

/// Per-endpoint state for smart load balancing
struct EndpointState {
    /// Buffer fill ratio (0-1000, scaled to avoid floats in atomics)
    /// 0 = empty, 1000 = full
    fill_ratio: AtomicU32,
    /// Timestamp when endpoint became unhealthy (0 = healthy)
    unhealthy_until: AtomicU64,
    /// Consecutive failure count
    consecutive_failures: AtomicU32,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            fill_ratio: AtomicU32::new(500), // Start at 50% (unknown)
            unhealthy_until: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
        }
    }

    /// Update fill ratio from Ack response
    fn update_fill_ratio(&self, buffer_size: i64, buffer_capacity: i64) {
        if buffer_capacity > 0 {
            let ratio = ((buffer_size as f64 / buffer_capacity as f64) * 1000.0) as u32;
            self.fill_ratio.store(ratio.min(1000), Ordering::Relaxed);
        }
        // Success - reset failures
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.unhealthy_until.store(0, Ordering::Relaxed);
    }

    /// Record a failure, possibly marking endpoint unhealthy
    fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= FAILURE_THRESHOLD {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.unhealthy_until
                .store(now + UNHEALTHY_DURATION_MS, Ordering::Relaxed);
        }
    }

    /// Check if endpoint is currently healthy
    fn is_healthy(&self) -> bool {
        let unhealthy_until = self.unhealthy_until.load(Ordering::Relaxed);
        if unhealthy_until == 0 {
            return true;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now >= unhealthy_until
    }

    /// Get fill ratio as float (0.0 - 1.0)
    fn get_fill_ratio(&self) -> f32 {
        self.fill_ratio.load(Ordering::Relaxed) as f32 / 1000.0
    }
}

/// gRPC emitter that forwards events to another POLKU Gateway
///
/// Uses batch streaming (`stream_events`) for high throughput instead of
/// per-event unary calls. Supports multiple endpoints with intelligent
/// load balancing based on downstream buffer pressure.
pub struct GrpcEmitter {
    /// Multiple clients for load balancing (single client if one endpoint)
    clients: Vec<Mutex<GatewayClient<Channel>>>,
    /// Per-endpoint state (fill ratio, health)
    states: Vec<EndpointState>,
    /// Target endpoints for logging/debugging
    endpoints: Vec<String>,
    /// Source identifier for batches
    source: String,
}

impl GrpcEmitter {
    /// Create a new GrpcEmitter connected to the given endpoint
    ///
    /// Uses default timeouts: 10s connect, 30s request.
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC endpoint URL (e.g., "http://localhost:50051")
    pub async fn new(endpoint: impl Into<String>) -> Result<Self, PluginError> {
        Self::with_endpoints(vec![endpoint.into()]).await
    }

    /// Create a GrpcEmitter with multiple endpoints using least-loaded balancing
    ///
    /// Uses intelligent load balancing: picks the endpoint with lowest buffer
    /// fill ratio (learned from Ack responses).
    ///
    /// # Arguments
    /// * `endpoints` - List of gRPC endpoint URLs
    pub async fn with_endpoints(endpoints: Vec<String>) -> Result<Self, PluginError> {
        if endpoints.is_empty() {
            return Err(PluginError::Init("No endpoints provided".to_string()));
        }

        let mut clients = Vec::with_capacity(endpoints.len());
        let mut states = Vec::with_capacity(endpoints.len());

        for endpoint_str in &endpoints {
            let channel = Endpoint::from_shared(endpoint_str.clone())
                .map_err(|e| PluginError::Init(format!("Invalid endpoint URL: {}", e)))?
                .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
                .timeout(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS))
                .connect()
                .await
                .map_err(|e| {
                    PluginError::Connection(format!("Failed to connect to {}: {}", endpoint_str, e))
                })?;

            clients.push(Mutex::new(GatewayClient::new(channel)));
            states.push(EndpointState::new());
            debug!(endpoint = %endpoint_str, "gRPC emitter connected");
        }

        Ok(Self {
            clients,
            states,
            endpoints,
            source: "polku-emitter".to_string(),
        })
    }

    /// Set the source identifier used in batch messages
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Select endpoint with lowest buffer fill ratio
    ///
    /// Falls back to first endpoint if all are unhealthy (give them a chance to recover)
    fn select_endpoint(&self) -> usize {
        let mut best_idx = None;
        let mut best_ratio = f32::MAX;

        for (idx, state) in self.states.iter().enumerate() {
            if state.is_healthy() {
                let ratio = state.get_fill_ratio();
                if ratio < best_ratio {
                    best_ratio = ratio;
                    best_idx = Some(idx);
                }
            }
        }

        match best_idx {
            Some(idx) => {
                debug!(
                    endpoint = %self.endpoints[idx],
                    fill_ratio = %format!("{:.1}%", best_ratio * 100.0),
                    "Selected least-loaded endpoint"
                );
                idx
            }
            None => {
                // All unhealthy - try first endpoint (give it a chance to recover)
                warn!("All endpoints unhealthy, trying first endpoint");
                0
            }
        }
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

        // Select least-loaded endpoint
        let client_idx = self.select_endpoint();
        let mut client = self.clients[client_idx].lock().await.clone();
        let endpoint = &self.endpoints[client_idx];
        let state = &self.states[client_idx];

        // Build a single batch with all events
        let batch = IngestBatch {
            source: self.source.clone(),
            cluster: String::new(),
            payload: Some(ingest_batch::Payload::Events(EventPayload {
                events: events.to_vec(),
            })),
        };

        // Use streaming RPC - send batch, receive ack
        let stream = tokio_stream::once(batch);

        match client.stream_events(stream).await {
            Ok(response) => {
                let mut ack_stream = response.into_inner();

                // Consume acks from the stream
                while let Ok(Some(ack)) = ack_stream.message().await {
                    debug!(
                        endpoint = %endpoint,
                        acked = ack.event_ids.len(),
                        buffer_size = ack.buffer_size,
                        "Batch forwarded"
                    );

                    // Update endpoint state from Ack (this is the magic - we learn actual load)
                    state.update_fill_ratio(ack.buffer_size, ack.buffer_capacity);

                    // Log backpressure warning
                    if ack.buffer_size > 0 && ack.buffer_capacity > 0 {
                        let fill_ratio = ack.buffer_size as f64 / ack.buffer_capacity as f64;
                        if fill_ratio > 0.8 {
                            warn!(
                                endpoint = %endpoint,
                                fill_ratio = %format!("{:.1}%", fill_ratio * 100.0),
                                "Downstream buffer nearing capacity"
                            );
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                // Record failure - may mark endpoint unhealthy after threshold
                state.record_failure();
                let failures = state.consecutive_failures.load(Ordering::Relaxed);

                if failures >= FAILURE_THRESHOLD {
                    info!(
                        endpoint = %endpoint,
                        failures = failures,
                        "Endpoint marked unhealthy after consecutive failures"
                    );
                }

                error!(
                    endpoint = %endpoint,
                    event_count = events.len(),
                    error = %e,
                    "Failed to forward batch"
                );
                Err(PluginError::Send(format!(
                    "Failed to forward batch of {} events: {}",
                    events.len(),
                    e
                )))
            }
        }
    }

    async fn health(&self) -> bool {
        // Check health of all endpoints, return true if any is healthy
        for (idx, client_mutex) in self.clients.iter().enumerate() {
            let mut client = client_mutex.lock().await.clone();
            match client.health(HealthRequest {}).await {
                Ok(response) => {
                    if response.into_inner().healthy {
                        return true;
                    }
                }
                Err(e) => {
                    debug!(
                        endpoint = %self.endpoints[idx],
                        error = %e,
                        "Health check failed"
                    );
                }
            }
        }
        false
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

    /// Start a test gateway server, return its address and buffer
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
    async fn test_grpc_emitter_emits_batch() {
        let (addr, buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await.unwrap();

        // Send a batch of events
        let events = vec![
            make_event("e1", "test.created"),
            make_event("e2", "test.updated"),
            make_event("e3", "test.deleted"),
        ];

        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Should emit batch successfully");

        // Verify all events landed in the server's buffer
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert_eq!(buffer.len(), 3, "Server should have received 3 events");
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

    #[tokio::test]
    async fn test_grpc_emitter_least_loaded() {
        // Start two test servers with different buffer sizes
        let buffer1 = Arc::new(RingBuffer::new(100)); // Will be "emptier"
        let buffer2 = Arc::new(RingBuffer::new(100));

        // Pre-fill buffer2 to make it "more loaded"
        let pre_fill: Vec<_> = (0..50)
            .map(|_| {
                crate::message::Message::new("prefill", "prefill", bytes::Bytes::from_static(b""))
            })
            .collect();
        buffer2.push(pre_fill);

        let service1 = GatewayService::new(Arc::clone(&buffer1));
        let service2 = GatewayService::new(Arc::clone(&buffer2));

        let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let addr2 = listener2.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(service1.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener1))
                .await
                .unwrap();
        });
        tokio::spawn(async move {
            Server::builder()
                .add_service(service2.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener2))
                .await
                .unwrap();
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Use default LeastLoaded strategy
        let emitter = GrpcEmitter::with_endpoints(vec![
            format!("http://{}", addr1),
            format!("http://{}", addr2),
        ])
        .await
        .unwrap();

        // Send batches - should prefer server1 (less loaded)
        for i in 0..4 {
            let events = vec![make_event(&format!("ll-{}", i), "test.leastloaded")];
            emitter.emit(&events).await.unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Server1 should get more events since it's less loaded
        // First emit goes to unknown (50%/50%), but after Ack, server1 is 1/100 = 1%
        // and server2 is 51/100 = 51%, so subsequent emits prefer server1
        assert!(
            buffer1.len() >= 3,
            "Less loaded server should get most events, got {}",
            buffer1.len()
        );
    }

    #[tokio::test]
    async fn test_grpc_emitter_large_batch() {
        let (addr, buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint).await.unwrap();

        // Send a large batch of events
        let events: Vec<Event> = (0..100)
            .map(|i| make_event(&format!("large-{}", i), "test.large"))
            .collect();

        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Should emit large batch successfully");

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(buffer.len(), 100, "Server should have received all 100 events");
    }

    #[tokio::test]
    async fn test_grpc_emitter_with_source() {
        let (addr, _buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = GrpcEmitter::new(&endpoint)
            .await
            .unwrap()
            .with_source("my-custom-source");

        assert_eq!(emitter.source, "my-custom-source");
    }

    #[tokio::test]
    async fn test_grpc_emitter_empty_endpoints() {
        let result = GrpcEmitter::with_endpoints(vec![]).await;
        assert!(result.is_err(), "Should fail with empty endpoints");
    }
}
