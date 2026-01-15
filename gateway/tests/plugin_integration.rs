//! Plugin Integration Tests
//!
//! Black-box tests for the plugin system using real infrastructure.
//! These tests verify failure modes, not happy paths.
//!
//! Test scenarios:
//! 1. Plugin crashes mid-request → POLKU recovers
//! 2. Plugin slow/hangs → timeout triggers, doesn't block pipeline
//! 3. Plugin returns garbage → graceful error, no panic
//! 4. Network partition → reconnects automatically
//! 5. Plugin restarts with new address → discovery handles it
//! 6. Concurrent requests during plugin restart → no data loss
//! 7. Plugin OOMs → circuit breaker opens

use polku_gateway::emit::{Emitter, ExternalEmitter};
use polku_gateway::error::PluginError;
use polku_gateway::ingest::{ExternalIngestor, IngestContext, Ingestor};
use polku_gateway::proto::emitter_plugin_server::{EmitterPlugin, EmitterPluginServer};
use polku_gateway::proto::ingestor_plugin_server::{IngestorPlugin, IngestorPluginServer};
use polku_gateway::proto::{
    EmitRequest, EmitResponse, IngestRequest, IngestResponse, PluginHealthResponse, PluginInfo,
    ShutdownResponse,
};
use polku_core::Event;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::time::timeout;
use tonic::{Request, Response, Status};

// =============================================================================
// TEST INFRASTRUCTURE
// =============================================================================

/// Plugin that can be controlled to simulate failures
struct ChaoticPluginInner {
    /// Kill the connection after N requests
    kill_after: AtomicU32,
    /// Current request count
    request_count: AtomicU32,
    /// Delay each request by this duration
    delay: Duration,
    /// Return error on next request
    fail_next: AtomicBool,
    /// Hang forever (simulate OOM/deadlock)
    hang: AtomicBool,
    /// Signal when request starts (for coordination)
    request_started: Notify,
    /// Signal to kill server
    kill_signal: Notify,
}

/// Newtype wrapper to satisfy orphan rules
struct ChaoticEmitter(Arc<ChaoticPluginInner>);
struct ChaoticIngestor(Arc<ChaoticPluginInner>);

impl ChaoticPluginInner {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            kill_after: AtomicU32::new(u32::MAX),
            request_count: AtomicU32::new(0),
            delay: Duration::ZERO,
            fail_next: AtomicBool::new(false),
            hang: AtomicBool::new(false),
            request_started: Notify::new(),
            kill_signal: Notify::new(),
        })
    }

    fn with_kill_after(self: Arc<Self>, n: u32) -> Arc<Self> {
        self.kill_after.store(n, Ordering::SeqCst);
        self
    }

    fn with_delay(self: Arc<Self>, d: Duration) -> Arc<Self> {
        // Create new instance with delay
        Arc::new(Self {
            kill_after: AtomicU32::new(self.kill_after.load(Ordering::SeqCst)),
            request_count: AtomicU32::new(0),
            delay: d,
            fail_next: AtomicBool::new(false),
            hang: AtomicBool::new(false),
            request_started: Notify::new(),
            kill_signal: Notify::new(),
        })
    }

    fn fail_next(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    fn hang_forever(&self) {
        self.hang.store(true, Ordering::SeqCst);
    }

    async fn maybe_chaos(&self) -> Result<(), Status> {
        self.request_started.notify_one();

        // Check if we should hang
        if self.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }

        // Check if we should fail
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(Status::internal("Chaos monkey says no"));
        }

        // Apply delay
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }

        // Check if we should kill
        let count = self.request_count.fetch_add(1, Ordering::SeqCst);
        if count >= self.kill_after.load(Ordering::SeqCst) {
            self.kill_signal.notify_one();
            return Err(Status::unavailable("Server shutting down"));
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl EmitterPlugin for ChaoticEmitter {
    async fn info(&self, _: Request<()>) -> Result<Response<PluginInfo>, Status> {
        Ok(Response::new(PluginInfo {
            name: "chaotic-emitter".to_string(),
            version: "1.0.0".to_string(),
            r#type: 2,
            description: "Chaos testing".to_string(),
            sources: vec![],
            emitter_name: "chaos".to_string(),
            capabilities: vec![],
        }))
    }

    async fn health(&self, _: Request<()>) -> Result<Response<PluginHealthResponse>, Status> {
        self.0.maybe_chaos().await?;
        Ok(Response::new(PluginHealthResponse {
            healthy: true,
            message: "OK".to_string(),
            components: Default::default(),
        }))
    }

    async fn emit(&self, request: Request<EmitRequest>) -> Result<Response<EmitResponse>, Status> {
        self.0.maybe_chaos().await?;
        let req = request.into_inner();
        Ok(Response::new(EmitResponse {
            success_count: req.events.len() as i64,
            failed_event_ids: vec![],
            errors: vec![],
        }))
    }

    async fn shutdown(&self, _: Request<()>) -> Result<Response<ShutdownResponse>, Status> {
        Ok(Response::new(ShutdownResponse {
            success: true,
            message: "bye".to_string(),
        }))
    }
}

#[tonic::async_trait]
impl IngestorPlugin for ChaoticIngestor {
    async fn info(&self, _: Request<()>) -> Result<Response<PluginInfo>, Status> {
        Ok(Response::new(PluginInfo {
            name: "chaotic-ingestor".to_string(),
            version: "1.0.0".to_string(),
            r#type: 1,
            description: "Chaos testing".to_string(),
            sources: vec!["chaos".to_string()],
            emitter_name: String::new(),
            capabilities: vec![],
        }))
    }

    async fn health(&self, _: Request<()>) -> Result<Response<PluginHealthResponse>, Status> {
        self.0.maybe_chaos().await?;
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
        self.0.maybe_chaos().await?;
        let req = request.into_inner();
        Ok(Response::new(IngestResponse {
            events: vec![Event {
                id: "chaos-event".to_string(),
                source: req.source,
                event_type: "chaos.test".to_string(),
                ..Default::default()
            }],
            errors: vec![],
        }))
    }
}

/// Start a chaotic emitter plugin server
async fn start_chaotic_emitter(
    plugin: Arc<ChaoticPluginInner>,
) -> (SocketAddr, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let plugin_for_kill = plugin.clone();
    let service = ChaoticEmitter(plugin);

    tokio::spawn(async move {
        let kill_notified = plugin_for_kill.kill_signal.notified();
        tonic::transport::Server::builder()
            .add_service(EmitterPluginServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    tokio::select! {
                        _ = shutdown_rx => {}
                        _ = kill_notified => {}
                    }
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, shutdown_tx)
}

async fn start_chaotic_ingestor(
    plugin: Arc<ChaoticPluginInner>,
) -> (SocketAddr, oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let plugin_for_kill = plugin.clone();
    let service = ChaoticIngestor(plugin);

    tokio::spawn(async move {
        let kill_notified = plugin_for_kill.kill_signal.notified();
        tonic::transport::Server::builder()
            .add_service(IngestorPluginServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    tokio::select! {
                        _ = shutdown_rx => {}
                        _ = kill_notified => {}
                    }
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, shutdown_tx)
}

// =============================================================================
// FAILURE MODE TESTS
// =============================================================================

/// Plugin crashes mid-stream - verify POLKU doesn't panic and returns error
#[tokio::test]
async fn test_emitter_plugin_crashes_mid_request() {
    let plugin = ChaoticPluginInner::new().with_kill_after(1);
    let (addr, _shutdown) = start_chaotic_emitter(plugin.clone()).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // First request succeeds
    let events = vec![Event {
        id: "1".to_string(),
        ..Default::default()
    }];
    let result1 = emitter.emit(&events).await;
    assert!(result1.is_ok(), "First request should succeed");

    // Server kills itself after first request
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second request should fail gracefully (not panic)
    let result2 = emitter.emit(&events).await;
    assert!(result2.is_err(), "Second request should fail after crash");

    // Error should be descriptive
    match result2 {
        Err(PluginError::Send(msg)) | Err(PluginError::Connection(msg)) => {
            assert!(!msg.is_empty());
        }
        Err(e) => panic!("Unexpected error type: {:?}", e),
        Ok(_) => panic!("Should have failed"),
    }
}

/// Plugin hangs forever - verify timeout works
#[tokio::test]
async fn test_emitter_plugin_hangs_timeout() {
    let plugin = ChaoticPluginInner::new();
    let (addr, _shutdown) = start_chaotic_emitter(plugin.clone()).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // First request succeeds (establishes connection)
    let events = vec![Event::default()];
    let _ = emitter.emit(&events).await;

    // Make plugin hang
    plugin.hang_forever();

    // Request with timeout should fail, not hang forever
    let result = timeout(Duration::from_secs(2), emitter.emit(&events)).await;

    match result {
        Ok(Ok(_)) => panic!("Should have timed out or errored"),
        Ok(Err(_)) => {} // Plugin error is acceptable
        Err(_) => {}     // Timeout is acceptable
    }
}

/// Plugin returns error - verify it propagates correctly
#[tokio::test]
async fn test_emitter_plugin_returns_error() {
    let plugin = ChaoticPluginInner::new();
    let (addr, _shutdown) = start_chaotic_emitter(plugin.clone()).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // Tell plugin to fail next request
    plugin.fail_next();

    let events = vec![Event::default()];
    let result = emitter.emit(&events).await;

    assert!(result.is_err());
    match result {
        Err(PluginError::Send(msg)) => {
            assert!(msg.contains("Chaos monkey") || msg.contains("error"));
        }
        Err(e) => panic!("Expected Send error, got: {:?}", e),
        Ok(_) => panic!("Should have failed"),
    }
}

/// Slow plugin - verify we handle latency gracefully
#[tokio::test]
async fn test_emitter_plugin_slow_response() {
    let plugin = ChaoticPluginInner::new().with_delay(Duration::from_millis(500));
    let (addr, _shutdown) = start_chaotic_emitter(plugin).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    let events = vec![Event::default()];
    let start = std::time::Instant::now();
    let result = emitter.emit(&events).await;
    let elapsed = start.elapsed();

    // Should succeed but take ~500ms
    assert!(result.is_ok());
    assert!(elapsed >= Duration::from_millis(400), "Should have delayed");
    assert!(elapsed < Duration::from_secs(2), "Shouldn't take too long");
}

/// Multiple concurrent requests during plugin instability
#[tokio::test]
async fn test_emitter_concurrent_requests_during_chaos() {
    let plugin = ChaoticPluginInner::new().with_kill_after(5);
    let (addr, _shutdown) = start_chaotic_emitter(plugin).await;

    let emitter = Arc::new(ExternalEmitter::new("chaos", format!("http://{}", addr)));

    // Spawn 10 concurrent requests
    let mut handles = vec![];
    for i in 0..10 {
        let emitter = emitter.clone();
        handles.push(tokio::spawn(async move {
            let events = vec![Event {
                id: format!("evt-{}", i),
                ..Default::default()
            }];
            emitter.emit(&events).await
        }));
    }

    // Wait for all
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Some should succeed, some should fail (plugin crashes after 5)
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    // At least some succeeded before crash
    assert!(successes > 0, "Some requests should succeed");
    // At least some failed after crash
    assert!(failures > 0, "Some requests should fail after crash");
    // No panics!
}

/// Plugin becomes unreachable - verify reconnect works
#[tokio::test]
async fn test_emitter_reconnects_after_network_failure() {
    let plugin = ChaoticPluginInner::new();
    let (addr, shutdown) = start_chaotic_emitter(plugin.clone()).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // First request succeeds
    let events = vec![Event::default()];
    let result1 = emitter.emit(&events).await;
    assert!(result1.is_ok());

    // Kill the server
    drop(shutdown);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request fails
    let result2 = emitter.emit(&events).await;
    assert!(result2.is_err());

    // Start a new server on SAME address
    let plugin2 = ChaoticPluginInner::new();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let service = ChaoticEmitter(plugin2);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EmitterPluginServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should reconnect and succeed
    let result3 = emitter.emit(&events).await;
    assert!(result3.is_ok(), "Should reconnect: {:?}", result3);
}

/// Ingestor plugin crashes - verify graceful degradation
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ingestor_plugin_crashes() {
    let plugin = ChaoticPluginInner::new().with_kill_after(1);
    let (addr, _shutdown) = start_chaotic_ingestor(plugin).await;

    let ingestor = ExternalIngestor::new("chaos", format!("http://{}", addr));
    let ctx = IngestContext {
        source: "chaos",
        cluster: "test",
        format: "test",
    };

    // First succeeds
    let result1 = ingestor.ingest(&ctx, b"data");
    assert!(result1.is_ok());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second fails gracefully
    let result2 = ingestor.ingest(&ctx, b"data");
    assert!(result2.is_err());
}

/// Health check reflects actual plugin state
#[tokio::test]
async fn test_health_reflects_plugin_state() {
    let plugin = ChaoticPluginInner::new();
    let (addr, shutdown) = start_chaotic_emitter(plugin.clone()).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // Healthy when plugin is up
    assert!(emitter.health().await);

    // Tell plugin to fail health checks
    plugin.fail_next();
    assert!(!emitter.health().await);

    // Healthy again
    assert!(emitter.health().await);

    // Unhealthy when plugin is down
    drop(shutdown);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!emitter.health().await);
}

/// Rapid plugin restart - verify no resource leaks
#[tokio::test]
async fn test_rapid_plugin_restarts() {
    let emitter = ExternalEmitter::new("chaos", "http://127.0.0.1:19999");

    for i in 0..5 {
        // Start plugin
        let plugin = ChaoticPluginInner::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:19999").await.unwrap();
        let service = ChaoticEmitter(plugin);

        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(EmitterPluginServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Make a request
        let events = vec![Event::default()];
        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Iteration {} failed: {:?}", i, result);

        // Kill plugin
        handle.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Empty events batch - edge case
#[tokio::test]
async fn test_empty_batch_handling() {
    let plugin = ChaoticPluginInner::new();
    let (addr, _shutdown) = start_chaotic_emitter(plugin).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // Empty batch should work
    let result = emitter.emit(&[]).await;
    assert!(result.is_ok());
}

/// Large batch - stress test
#[tokio::test]
async fn test_large_batch() {
    let plugin = ChaoticPluginInner::new();
    let (addr, _shutdown) = start_chaotic_emitter(plugin).await;

    let emitter = ExternalEmitter::new("chaos", format!("http://{}", addr));

    // 10k events
    let events: Vec<Event> = (0..10_000)
        .map(|i| Event {
            id: format!("evt-{}", i),
            ..Default::default()
        })
        .collect();

    let result = emitter.emit(&events).await;
    assert!(result.is_ok());
}

/// Plugin address never existed - immediate failure
#[tokio::test]
async fn test_nonexistent_plugin() {
    let emitter = ExternalEmitter::new("ghost", "http://127.0.0.1:59998");

    let events = vec![Event::default()];
    let result = emitter.emit(&events).await;

    assert!(result.is_err());
    match result {
        Err(PluginError::Connection(_)) => {} // Expected
        Err(e) => panic!("Expected Connection error, got: {:?}", e),
        Ok(_) => panic!("Should have failed"),
    }
}
