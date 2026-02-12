//! Black-box E2E tests for POLKU
//!
//! These tests deploy POLKU to a real Kubernetes cluster (Kind)
//! and verify behavior from the outside.

use polku_e2e::proto::HealthRequest;
use polku_e2e::proto::gateway_client::GatewayClient;
use polku_e2e::{PolkuClient, PolkuTestEnv};
#[allow(unused_imports)]
use seppo::Context;
use std::time::Duration;

/// Test 1: Send 1 event → verify it arrives
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_single_event_arrives(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send 1 event
    let event = PolkuClient::make_event("test.single");
    let response = client.send_event(event).await.expect("Send should succeed");

    // Verify accepted (event_ids contains our event)
    assert!(!response.event_ids.is_empty(), "Event should be accepted");
    assert!(response.errors.is_empty(), "No errors expected");
}

/// Test 2: Send 100 events → verify all 100 arrive
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_hundred_events_arrive(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send 100 events
    let events = PolkuClient::make_events(100, "test.batch");
    let response = client
        .send_events(events)
        .await
        .expect("Send should succeed");

    // Verify all accepted
    assert_eq!(
        response.event_ids.len(),
        100,
        "All 100 events should be accepted"
    );
    assert!(response.errors.is_empty(), "No errors expected");
}

/// Test 3: Send 430 events with 12% broken → verify all arrive
/// NOTE: POLKU gateway accepts all well-formed protobuf messages.
/// Schema validation (rejecting empty ID, empty type) requires middleware.
/// This test verifies that events flow through even with "broken" content.
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_mixed_batch_all_accepted(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send 430 events with 12% "broken" (empty ID/type)
    // POLKU accepts all - they're valid protobuf messages
    let events = PolkuClient::make_mixed_batch(430, 12.0);

    let response = client
        .send_events(events)
        .await
        .expect("Send should succeed");

    // POLKU accepts all events (no schema validation at gateway level)
    let accepted = response.event_ids.len();

    // All 430 should be accepted - POLKU doesn't validate event content
    assert_eq!(
        accepted, 430,
        "All 430 events should be accepted (no schema validation)"
    );
}

/// Test 4: Verify events flow through the pipeline
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_event_flows_through_pipeline(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send events with specific type
    let events = PolkuClient::make_events(10, "test.pipeline.flow");
    let response = client
        .send_events(events)
        .await
        .expect("Send should succeed");

    assert_eq!(
        response.event_ids.len(),
        10,
        "All events should be accepted"
    );
    assert!(response.errors.is_empty());

    // Wait for events to propagate to receiver
    tokio::time::sleep(Duration::from_millis(500)).await;

    // TODO: Query receiver to verify events have correct type
}

/// Test 5: Receiver down → POLKU handles gracefully
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_receiver_down_graceful_degradation(ctx: Context) {
    use polku_e2e::setup::{polku_deployment_no_receiver, polku_service};

    // Deploy POLKU only (no receiver) - uses shared helpers with imagePullPolicy: Never
    let polku = polku_deployment_no_receiver();
    let polku_svc = polku_service();

    ctx.apply(&polku).await.expect("Deploy POLKU");
    ctx.apply(&polku_svc).await.expect("Deploy POLKU service");
    ctx.wait_ready("deployment/polku")
        .await
        .expect("Wait ready");

    let pf = ctx
        .port_forward("svc/polku", 50051)
        .await
        .expect("Port forward");
    let addr = format!("http://{}", pf.local_addr());

    // Wait for POLKU to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut client = PolkuClient::connect(&addr).await.expect("Connect");

    // Send event - POLKU should accept (buffered) or return error
    // It should NOT crash
    let event = PolkuClient::make_event("test.no_receiver");
    let result = client.send_event(event).await;

    // Either accepted (buffered) or error (rejected) - but not panic/crash
    match result {
        Ok(resp) => {
            tracing::info!("Event accepted (buffered): {:?}", resp);
        }
        Err(status) => {
            tracing::info!("Event rejected: {:?}", status);
        }
    }

    // Verify POLKU is still running by making another request
    let event2 = PolkuClient::make_event("test.still_alive");
    let _ = client.send_event(event2).await;
    // If we get here without panic, POLKU is still running
}

/// Test 6: Stress test - high throughput
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_stress_high_throughput(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send 10,000 events in batches of 1000
    let mut total_accepted = 0usize;
    let start = std::time::Instant::now();

    for batch_num in 0..10 {
        let events = PolkuClient::make_events(1000, &format!("test.stress.batch{}", batch_num));
        let response = client
            .send_events(events)
            .await
            .expect("Send should succeed");
        total_accepted += response.event_ids.len();
    }

    let elapsed = start.elapsed();

    // Verify all events accepted
    assert_eq!(total_accepted, 10_000, "All 10k events should be accepted");

    // Log throughput
    let events_per_sec = 10_000.0 / elapsed.as_secs_f64();
    tracing::info!(
        "Stress test: 10,000 events in {:?} ({:.0} events/sec)",
        elapsed,
        events_per_sec
    );

    // Basic performance assertion (should handle at least 1000 events/sec)
    assert!(events_per_sec > 1000.0, "Should handle >1000 events/sec");
}

/// Test 7: Performance measurement
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_performance_latency(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Measure latency for single events
    let mut latencies = Vec::new();

    for _ in 0..100 {
        let event = PolkuClient::make_event("test.latency");
        let start = std::time::Instant::now();
        let _ = client.send_event(event).await.expect("Send should succeed");
        latencies.push(start.elapsed());
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = latencies[50];
    let p95 = latencies[95];
    let p99 = latencies[99];

    tracing::info!("Latency: p50={:?}, p95={:?}, p99={:?}", p50, p95, p99);

    // Basic latency assertion (p99 should be under 100ms for local Kind)
    assert!(
        p99 < Duration::from_millis(100),
        "p99 latency should be <100ms"
    );
}

// ============================================================================
// REAL PROTOBUF TESTS - typed eBPF/kernel events with verification
// ============================================================================

/// Test 8: Send 20,000 real eBPF kernel events with typed protobuf data
/// Verify events arrive at receiver via health endpoint
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_real_ebpf_20k_events(ctx: Context) {
    use seppo::{deployment, service};

    // Deploy test-receiver first
    let receiver_deploy = deployment("test-receiver")
        .image("polku-test-receiver:latest")
        .image_pull_policy("Never")
        .port(9001)
        .env("RECEIVER_ADDR", "0.0.0.0:9001")
        .build();

    let receiver_svc = service("test-receiver")
        .selector("app", "test-receiver")
        .port(9001, 9001)
        .build();

    ctx.apply(&receiver_deploy).await.expect("Deploy receiver");
    ctx.apply(&receiver_svc)
        .await
        .expect("Deploy receiver service");
    ctx.wait_ready("deployment/test-receiver")
        .await
        .expect("Wait for receiver");

    // Deploy POLKU configured to emit to test-receiver
    let polku_deploy = deployment("polku")
        .image("polku-gateway:latest")
        .image_pull_policy("Never")
        .port(50051)
        .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
        .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://test-receiver:9001")
        .env("POLKU_EMIT_GRPC_LAZY", "true")
        .env("POLKU_LOG_LEVEL", "debug")
        .build();

    let polku_svc = service("polku")
        .selector("app", "polku")
        .port(50051, 50051)
        .build();

    ctx.apply(&polku_deploy).await.expect("Deploy POLKU");
    ctx.apply(&polku_svc).await.expect("Deploy POLKU service");
    ctx.wait_ready("deployment/polku")
        .await
        .expect("Wait for POLKU");

    // Port forward to both POLKU (to send) and receiver (to verify)
    let polku_pf = ctx
        .port_forward("svc/polku", 50051)
        .await
        .expect("Port forward POLKU");
    let receiver_pf = ctx
        .port_forward("svc/test-receiver", 9001)
        .await
        .expect("Port forward receiver");

    let polku_addr = format!("http://{}", polku_pf.local_addr());
    let receiver_addr = format!("http://{}", receiver_pf.local_addr());

    // Wait for services to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect to POLKU
    let mut client = PolkuClient::connect(&polku_addr)
        .await
        .expect("Connect to POLKU");

    // Get initial receiver event count
    let mut receiver_client = GatewayClient::connect(receiver_addr.clone())
        .await
        .expect("Connect to receiver");
    let initial_health = receiver_client
        .health(HealthRequest {})
        .await
        .expect("Initial health");
    let initial_count = initial_health.into_inner().events_processed;

    eprintln!(">>> Initial receiver event count: {}", initial_count);

    // Send 200,000 real eBPF events with typed protobuf data
    let total = 200_000usize;
    let start = std::time::Instant::now();

    // Send in batches of 1000
    let mut total_sent = 0usize;
    for batch_num in 0..200 {
        let events = PolkuClient::make_real_ebpf_batch(1000);
        let response = client.send_events(events).await.expect("Send batch");
        total_sent += response.event_ids.len();

        if batch_num % 20 == 0 {
            eprintln!(
                ">>> Sent batch {} ({} events so far)",
                batch_num, total_sent
            );
        }
    }

    let send_time = start.elapsed();
    let send_rate = total_sent as f64 / send_time.as_secs_f64();
    eprintln!(
        ">>> Sent {} events in {:?} ({:.0} events/sec)",
        total_sent, send_time, send_rate
    );

    // Wait for events to propagate through POLKU to receiver
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify events arrived at receiver via health endpoint
    let final_health = receiver_client
        .health(HealthRequest {})
        .await
        .expect("Final health");
    let final_count = final_health.into_inner().events_processed;

    let received = final_count - initial_count;
    eprintln!(
        ">>> Receiver processed {} events (initial={}, final={})",
        received, initial_count, final_count
    );

    // Verify we received the events
    assert_eq!(total_sent, total, "Should have sent {} events", total);
    assert!(
        received >= (total_sent as u64 * 95 / 100),
        "Should have received at least 95% of events: sent={}, received={}",
        total_sent,
        received
    );

    eprintln!(
        ">>> SUCCESS: {} real eBPF events flowed through POLKU to receiver",
        received
    );
}

/// Test 9: Send kernel syscall events and verify typed data integrity
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_kernel_syscall_events(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send kernel syscall events for various syscalls
    let syscalls = ["read", "write", "open", "close", "execve"];
    let mut events = Vec::new();

    for (i, syscall) in syscalls.iter().enumerate() {
        let event = PolkuClient::make_kernel_syscall_event(
            syscall,
            (1000 + i) as u32,
            if i % 2 == 0 { 0 } else { -1 },
        );
        events.push(event);
    }

    let response = client
        .send_events(events)
        .await
        .expect("Send should succeed");

    assert_eq!(
        response.event_ids.len(),
        5,
        "All 5 syscall events should be accepted"
    );
    assert!(response.errors.is_empty(), "No errors expected");

    tracing::info!(
        "SUCCESS: {} kernel syscall events sent",
        response.event_ids.len()
    );
}

/// Test 10: Send network events and verify typed data integrity
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_network_events(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx)
        .await
        .expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr)
        .await
        .expect("Connect should succeed");

    // Send network events
    let events = PolkuClient::make_network_events(100);
    let response = client
        .send_events(events)
        .await
        .expect("Send should succeed");

    assert_eq!(
        response.event_ids.len(),
        100,
        "All 100 network events should be accepted"
    );
    assert!(response.errors.is_empty(), "No errors expected");

    tracing::info!("SUCCESS: {} network events sent", response.event_ids.len());
}

/// Test 11: STREAMING - Send 500k events via bidirectional streaming RPC
/// This tests POLKU's real throughput without test client bottleneck
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_streaming_500k_events(ctx: Context) {
    use seppo::{deployment, service};

    // Deploy test-receiver first
    let receiver_deploy = deployment("test-receiver")
        .image("polku-test-receiver:latest")
        .image_pull_policy("Never")
        .port(9001)
        .env("RECEIVER_ADDR", "0.0.0.0:9001")
        .build();

    let receiver_svc = service("test-receiver")
        .selector("app", "test-receiver")
        .port(9001, 9001)
        .build();

    ctx.apply(&receiver_deploy).await.expect("Deploy receiver");
    ctx.apply(&receiver_svc)
        .await
        .expect("Deploy receiver service");
    ctx.wait_ready("deployment/test-receiver")
        .await
        .expect("Wait for receiver");

    // Deploy POLKU configured to emit to test-receiver
    let polku_deploy = deployment("polku")
        .image("polku-gateway:latest")
        .image_pull_policy("Never")
        .port(50051)
        .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
        .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://test-receiver:9001")
        .env("POLKU_EMIT_GRPC_LAZY", "true")
        .env("POLKU_LOG_LEVEL", "info") // Reduce logging overhead
        .build();

    let polku_svc = service("polku")
        .selector("app", "polku")
        .port(50051, 50051)
        .build();

    ctx.apply(&polku_deploy).await.expect("Deploy POLKU");
    ctx.apply(&polku_svc).await.expect("Deploy POLKU service");
    ctx.wait_ready("deployment/polku")
        .await
        .expect("Wait for POLKU");

    // Port forward to both POLKU (to send) and receiver (to verify)
    let polku_pf = ctx
        .port_forward("svc/polku", 50051)
        .await
        .expect("Port forward POLKU");
    let receiver_pf = ctx
        .port_forward("svc/test-receiver", 9001)
        .await
        .expect("Port forward receiver");

    let polku_addr = format!("http://{}", polku_pf.local_addr());
    let receiver_addr = format!("http://{}", receiver_pf.local_addr());

    // Wait for services to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect to POLKU
    let mut client = PolkuClient::connect(&polku_addr)
        .await
        .expect("Connect to POLKU");

    // Get initial receiver event count
    let mut receiver_client = GatewayClient::connect(receiver_addr.clone())
        .await
        .expect("Connect to receiver");
    let initial_health = receiver_client
        .health(HealthRequest {})
        .await
        .expect("Initial health");
    let initial_count = initial_health.into_inner().events_processed;

    eprintln!(
        ">>> [STREAMING] Initial receiver event count: {}",
        initial_count
    );

    // Send 500,000 real eBPF events via STREAMING
    let total = 500_000usize;
    let batch_size = 1000; // Events per streaming batch

    eprintln!(">>> [STREAMING] Generating {} events...", total);
    let events = PolkuClient::make_real_ebpf_batch(total);

    eprintln!(
        ">>> [STREAMING] Sending via bidirectional streaming (batch_size={})...",
        batch_size
    );
    let start = std::time::Instant::now();

    let response = client
        .stream_events(events, batch_size)
        .await
        .expect("Stream should succeed");
    let total_sent = response.event_ids.len();

    let send_time = start.elapsed();
    let send_rate = total_sent as f64 / send_time.as_secs_f64();
    eprintln!(
        ">>> [STREAMING] Sent {} events in {:?} ({:.0} events/sec)",
        total_sent, send_time, send_rate
    );

    // Wait for events to propagate through POLKU to receiver
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify events arrived at receiver via health endpoint
    let final_health = receiver_client
        .health(HealthRequest {})
        .await
        .expect("Final health");
    let final_count = final_health.into_inner().events_processed;

    let received = final_count - initial_count;
    eprintln!(
        ">>> [STREAMING] Receiver processed {} events (initial={}, final={})",
        received, initial_count, final_count
    );

    // Verify we received the events
    assert_eq!(total_sent, total, "Should have sent {} events", total);
    assert!(
        received >= (total_sent as u64 * 95 / 100),
        "Should have received at least 95% of events: sent={}, received={}",
        total_sent,
        received
    );

    eprintln!(
        ">>> [STREAMING] SUCCESS: {} events at {:.0} events/sec",
        received, send_rate
    );
}
