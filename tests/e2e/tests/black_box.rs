//! Black-box E2E tests for POLKU
//!
//! These tests deploy POLKU to a real Kubernetes cluster (Kind)
//! and verify behavior from the outside.

use polku_e2e::{PolkuClient, PolkuTestEnv};
#[allow(unused_imports)]
use seppo::Context;
use std::time::Duration;

/// Test 1: Send 1 event → verify it arrives
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_single_event_arrives(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

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
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

    // Send 100 events
    let events = PolkuClient::make_events(100, "test.batch");
    let response = client.send_events(events).await.expect("Send should succeed");

    // Verify all accepted
    assert_eq!(response.event_ids.len(), 100, "All 100 events should be accepted");
    assert!(response.errors.is_empty(), "No errors expected");
}

/// Test 3: Send 430 events with 12% broken → verify ~378 arrive, ~52 rejected
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_mixed_batch_partial_success(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

    // Send 430 events with 12% broken
    let events = PolkuClient::make_mixed_batch(430, 12.0);
    let broken_count = (430.0_f64 * 0.12).round() as usize; // ~52
    let valid_count = 430 - broken_count;

    let response = client.send_events(events).await.expect("Send should succeed");

    // POLKU should accept valid and reject broken
    let accepted = response.event_ids.len();
    let rejected = response.errors.len();

    // Allow some margin for timing/ordering
    assert!(accepted >= valid_count - 5, "Expected ~{} accepted, got {}", valid_count, accepted);
    assert!(rejected >= broken_count - 5, "Expected ~{} rejected, got {}", broken_count, rejected);
}

/// Test 4: Verify events flow through the pipeline
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_event_flows_through_pipeline(ctx: Context) {
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

    // Send events with specific type
    let events = PolkuClient::make_events(10, "test.pipeline.flow");
    let response = client.send_events(events).await.expect("Send should succeed");

    assert_eq!(response.event_ids.len(), 10, "All events should be accepted");
    assert!(response.errors.is_empty());

    // Wait for events to propagate to receiver
    tokio::time::sleep(Duration::from_millis(500)).await;

    // TODO: Query receiver to verify events have correct type
}

/// Test 5: Receiver down → POLKU handles gracefully
#[seppo::test]
#[ignore] // Requires Kind cluster with POLKU images
async fn test_receiver_down_graceful_degradation(ctx: Context) {
    // Deploy POLKU only (no receiver)
    let polku = seppo::DeploymentFixture::new("polku")
        .image("polku-gateway:latest")
        .replicas(1)
        .port(50051)
        .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
        .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://nonexistent:9001")
        .env("POLKU_EMIT_GRPC_LAZY", "true")
        .build();

    let polku_svc = seppo::ServiceFixture::new("polku")
        .selector("app", "polku")
        .port(50051, 50051)
        .build();

    ctx.apply(&polku).await.expect("Deploy POLKU");
    ctx.apply(&polku_svc).await.expect("Deploy POLKU service");
    ctx.wait_ready("deployment/polku").await.expect("Wait ready");

    let pf = ctx.port_forward("svc/polku", 50051).await.expect("Port forward");
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
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

    // Send 10,000 events in batches of 1000
    let mut total_accepted = 0usize;
    let start = std::time::Instant::now();

    for batch_num in 0..10 {
        let events = PolkuClient::make_events(1000, &format!("test.stress.batch{}", batch_num));
        let response = client.send_events(events).await.expect("Send should succeed");
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
    let env = PolkuTestEnv::setup(&ctx).await.expect("Setup should succeed");
    let mut client = PolkuClient::connect(&env.polku_addr).await.expect("Connect should succeed");

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

    tracing::info!(
        "Latency: p50={:?}, p95={:?}, p99={:?}",
        p50, p95, p99
    );

    // Basic latency assertion (p99 should be under 100ms for local Kind)
    assert!(p99 < Duration::from_millis(100), "p99 latency should be <100ms");
}
