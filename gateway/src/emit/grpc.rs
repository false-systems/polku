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
use crate::metrics::Metrics;
use crate::proto::gateway_client::GatewayClient;
use crate::proto::{EventPayload, HealthRequest, IngestBatch, ingest_batch};
use async_trait::async_trait;
use polku_core::Event;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
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

    /// Update fill ratio from Ack response and report to metrics
    fn update_fill_ratio(&self, buffer_size: i64, buffer_capacity: i64, endpoint: &str) {
        if buffer_capacity > 0 {
            let ratio = ((buffer_size as f64 / buffer_capacity as f64) * 1000.0) as u32;
            self.fill_ratio.store(ratio.min(1000), Ordering::Relaxed);

            // Report to Prometheus
            if let Some(m) = Metrics::get() {
                m.set_grpc_endpoint_fill_ratio(endpoint, ratio as f64 / 1000.0);
            }
        }
        // Success - reset failures
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.unhealthy_until.store(0, Ordering::Relaxed);

        // Report health restored
        if let Some(m) = Metrics::get() {
            m.set_grpc_endpoint_health(endpoint, true);
            m.set_grpc_endpoint_failures(endpoint, 0);
        }
    }

    /// Record a failure, possibly marking endpoint unhealthy
    fn record_failure(&self, endpoint: &str) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= FAILURE_THRESHOLD {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.unhealthy_until
                .store(now + UNHEALTHY_DURATION_MS, Ordering::Relaxed);

            // Report unhealthy to Prometheus
            if let Some(m) = Metrics::get() {
                m.set_grpc_endpoint_health(endpoint, false);
            }
        }

        // Report failure count
        if let Some(m) = Metrics::get() {
            m.set_grpc_endpoint_failures(endpoint, failures);
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
///
/// # Failover Behavior
///
/// When an emit fails, the emitter automatically tries the next healthy endpoint
/// before returning an error. This provides resilience against transient failures.
pub struct GrpcEmitter {
    /// Multiple clients for load balancing (no Mutex - tonic clients are thread-safe)
    clients: Vec<GatewayClient<Channel>>,
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
    /// Connects eagerly - fails if endpoint is unreachable.
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC endpoint URL (e.g., "http://localhost:50051")
    pub async fn new(endpoint: impl Into<String>) -> Result<Self, PluginError> {
        Self::with_endpoints(vec![endpoint.into()]).await
    }

    /// Create a GrpcEmitter with multiple endpoints using least-loaded balancing
    ///
    /// Connects eagerly to all endpoints - fails if any endpoint is unreachable.
    /// Use `with_endpoints_lazy` for lazy connection.
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

            clients.push(GatewayClient::new(channel));
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

    /// Create a GrpcEmitter with lazy connection to endpoints
    ///
    /// Does not connect immediately - connection happens on first emit.
    /// This allows creating an emitter even if some endpoints are temporarily down.
    /// Useful for failover scenarios where not all endpoints need to be up at startup.
    ///
    /// # Arguments
    /// * `endpoints` - List of gRPC endpoint URLs
    pub async fn with_endpoints_lazy(endpoints: Vec<String>) -> Result<Self, PluginError> {
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
                .connect_lazy();

            clients.push(GatewayClient::new(channel));
            states.push(EndpointState::new());
            debug!(endpoint = %endpoint_str, "gRPC emitter configured (lazy)");
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

    /// Select endpoint with lowest buffer fill ratio, excluding already-tried indices
    ///
    /// Returns None if all endpoints have been tried or are unhealthy
    fn select_endpoint(&self, exclude: &[bool]) -> Option<usize> {
        debug_assert_eq!(
            exclude.len(),
            self.states.len(),
            "exclude slice length must match number of endpoint states"
        );

        let mut best_idx = None;
        let mut best_ratio = f32::MAX;

        for (idx, state) in self.states.iter().enumerate() {
            // Skip already-tried endpoints
            if exclude[idx] {
                continue;
            }

            if state.is_healthy() {
                let ratio = state.get_fill_ratio();
                if ratio < best_ratio {
                    best_ratio = ratio;
                    best_idx = Some(idx);
                }
            }
        }

        if let Some(idx) = best_idx {
            debug!(
                endpoint = %self.endpoints[idx],
                fill_ratio = %format!("{:.1}%", best_ratio * 100.0),
                "Selected least-loaded endpoint"
            );

            // Record selection to metrics
            if let Some(m) = Metrics::get() {
                m.record_grpc_endpoint_selected(&self.endpoints[idx]);
            }
        }

        best_idx
    }

    /// Select any untried endpoint (fallback when all healthy endpoints failed)
    fn select_any_untried(&self, exclude: &[bool]) -> Option<usize> {
        for (idx, excluded) in exclude.iter().enumerate() {
            if !excluded {
                return Some(idx);
            }
        }
        None
    }

    /// Try to emit to a specific endpoint
    async fn try_emit_to(&self, idx: usize, events: &[Event]) -> Result<(), PluginError> {
        let mut client = self.clients[idx].clone();
        let endpoint = &self.endpoints[idx];
        let state = &self.states[idx];

        // Build batch
        let batch = IngestBatch {
            source: self.source.clone(),
            cluster: String::new(),
            payload: Some(ingest_batch::Payload::Events(EventPayload {
                events: events.to_vec(),
            })),
        };

        let stream = tokio_stream::once(batch);

        match client.stream_events(stream).await {
            Ok(response) => {
                let mut ack_stream = response.into_inner();

                // Consume acks - properly handle errors
                loop {
                    match ack_stream.message().await {
                        Ok(Some(ack)) => {
                            debug!(
                                endpoint = %endpoint,
                                acked = ack.event_ids.len(),
                                buffer_size = ack.buffer_size,
                                "Batch forwarded"
                            );

                            // Update endpoint state from Ack
                            state.update_fill_ratio(ack.buffer_size, ack.buffer_capacity, endpoint);

                            // Log backpressure warning
                            if ack.buffer_size > 0 && ack.buffer_capacity > 0 {
                                let fill_ratio =
                                    ack.buffer_size as f64 / ack.buffer_capacity as f64;
                                if fill_ratio > 0.8 {
                                    warn!(
                                        endpoint = %endpoint,
                                        fill_ratio = %format!("{:.1}%", fill_ratio * 100.0),
                                        "Downstream buffer nearing capacity"
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            // Stream ended normally
                            break;
                        }
                        Err(e) => {
                            // Mid-stream error - record failure
                            state.record_failure(endpoint);
                            return Err(PluginError::Send(format!(
                                "Stream error from {}: {}",
                                endpoint, e
                            )));
                        }
                    }
                }

                // Record successful emit to metrics
                if let Some(m) = Metrics::get() {
                    m.record_grpc_endpoint_events(endpoint, events.len() as u64);
                }

                Ok(())
            }
            Err(e) => {
                state.record_failure(endpoint);
                Err(PluginError::Send(format!(
                    "Failed to send to {}: {}",
                    endpoint, e
                )))
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

        // Track which endpoints we've tried
        let mut tried = vec![false; self.clients.len()];
        let mut last_error = None;
        let mut attempts = 0;

        // Try healthy endpoints first (least-loaded order)
        while let Some(idx) = self.select_endpoint(&tried) {
            tried[idx] = true;
            attempts += 1;

            match self.try_emit_to(idx, events).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(
                        endpoint = %self.endpoints[idx],
                        error = %e,
                        "Emit failed, trying next endpoint"
                    );
                    last_error = Some(e);

                    // Record failover (not first attempt)
                    if attempts > 1 {
                        if let Some(m) = Metrics::get() {
                            m.record_grpc_failover();
                        }
                    }
                }
            }
        }

        // All healthy endpoints failed - try any remaining unhealthy ones
        // (they might have recovered)
        while let Some(idx) = self.select_any_untried(&tried) {
            tried[idx] = true;
            attempts += 1;

            // Record failover for trying unhealthy endpoints
            if let Some(m) = Metrics::get() {
                m.record_grpc_failover();
            }

            match self.try_emit_to(idx, events).await {
                Ok(()) => {
                    info!(
                        endpoint = %self.endpoints[idx],
                        "Unhealthy endpoint recovered"
                    );
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        // All endpoints failed
        error!(
            event_count = events.len(),
            endpoints_tried = tried.iter().filter(|&&t| t).count(),
            "All endpoints failed"
        );

        Err(last_error.unwrap_or_else(|| PluginError::Send("No endpoints available".to_string())))
    }

    async fn health(&self) -> bool {
        // Check health of all endpoints, return true if any is healthy
        for (idx, client) in self.clients.iter().enumerate() {
            let mut client = client.clone();
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
            severity: 0,
            outcome: 0,
            data: None,
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
        assert_eq!(
            buffer.len(),
            100,
            "Server should have received all 100 events"
        );
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

    #[tokio::test]
    async fn test_grpc_emitter_failover_on_emit_failure() {
        // Start two servers
        let (addr1, buffer1) = start_test_server().await;
        let (addr2, buffer2) = start_test_server().await;

        let emitter = GrpcEmitter::with_endpoints(vec![
            format!("http://{}", addr1),
            format!("http://{}", addr2),
        ])
        .await
        .unwrap();

        // Manually mark first endpoint as unhealthy (simulating failures)
        emitter.states[0]
            .consecutive_failures
            .store(10, Ordering::Relaxed);
        emitter.states[0].unhealthy_until.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 60000, // Unhealthy for 60s
            Ordering::Relaxed,
        );

        // Emit should failover to second endpoint
        let events = vec![make_event("failover-1", "test.failover")];
        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Should succeed via failover");

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Events should have gone to server2, not server1
        assert_eq!(
            buffer1.len(),
            0,
            "Unhealthy server should not receive events"
        );
        assert_eq!(buffer2.len(), 1, "Healthy server should receive events");
    }

    #[tokio::test]
    async fn test_grpc_emitter_tries_all_endpoints_before_failing() {
        // This test verifies that when the initially selected endpoint fails during emit,
        // the emitter automatically tries the next healthy endpoint instead of immediately failing.
        //
        // In other words, we should attempt all healthy endpoints before giving up.

        let (addr, buffer) = start_test_server().await;

        // Create emitter with one bad endpoint and one good endpoint
        // Bad endpoint will fail on emit (wrong port, nothing listening)
        let emitter = GrpcEmitter::with_endpoints_lazy(vec![
            "http://127.0.0.1:1".to_string(), // Bad - nothing listening
            format!("http://{}", addr),       // Good
        ])
        .await
        .unwrap();

        // Emit should try bad endpoint, fail, then try good endpoint and succeed
        let events = vec![make_event("retry-1", "test.retry")];
        let result = emitter.emit(&events).await;

        assert!(
            result.is_ok(),
            "Should succeed after failover to good endpoint"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(
            buffer.len(),
            1,
            "Good server should receive event after failover"
        );
    }

    #[tokio::test]
    async fn test_grpc_emitter_no_mutex_contention() {
        // Verify that concurrent emits all succeed without blocking each other.
        // This tests that we removed the unnecessary Mutex - tonic clients are thread-safe.
        let (addr, buffer) = start_test_server().await;
        let endpoint = format!("http://{}", addr);

        let emitter = Arc::new(GrpcEmitter::new(&endpoint).await.unwrap());

        // Spawn many concurrent emit tasks
        let mut handles = vec![];
        for i in 0..20 {
            let emitter = Arc::clone(&emitter);
            let events = vec![make_event(&format!("concurrent-{}", i), "test.concurrent")];
            handles.push(tokio::spawn(async move { emitter.emit(&events).await }));
        }

        // All should complete successfully
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Concurrent emit should succeed");
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(buffer.len(), 20, "All 20 events should arrive");
    }
}
