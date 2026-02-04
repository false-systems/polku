//! Integration tests for resilience layer
//!
//! These tests verify that resilience wrappers work correctly together
//! and integrate properly with the Hub.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::len_zero,
    clippy::inconsistent_digit_grouping
)]

use async_trait::async_trait;
use bytes::Bytes;
use polku_gateway::{
    BackoffConfig, CircuitBreakerConfig, Emitter, FailureBuffer, FailureCaptureConfig, Hub,
    Message, PluginError, ResilientEmitter,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

// ============================================================================
// Test Emitters
// ============================================================================

/// Emitter that fails a configurable number of times then succeeds
struct FailNTimesEmitter {
    failures_remaining: AtomicU32,
    emit_count: AtomicU64,
}

impl FailNTimesEmitter {
    fn new(fail_count: u32) -> Self {
        Self {
            failures_remaining: AtomicU32::new(fail_count),
            emit_count: AtomicU64::new(0),
        }
    }

    fn emit_count(&self) -> u64 {
        self.emit_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Emitter for FailNTimesEmitter {
    fn name(&self) -> &'static str {
        "fail_n_times"
    }

    async fn emit(&self, _events: &[polku_gateway::Event]) -> Result<(), PluginError> {
        self.emit_count.fetch_add(1, Ordering::SeqCst);
        let remaining = self.failures_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
            Err(PluginError::Connection("simulated failure".into()))
        } else {
            Ok(())
        }
    }

    async fn health(&self) -> bool {
        self.failures_remaining.load(Ordering::SeqCst) == 0
    }
}

/// Emitter that always fails
struct AlwaysFailEmitter {
    emit_count: AtomicU64,
}

impl AlwaysFailEmitter {
    fn new() -> Self {
        Self {
            emit_count: AtomicU64::new(0),
        }
    }

    fn emit_count(&self) -> u64 {
        self.emit_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Emitter for AlwaysFailEmitter {
    fn name(&self) -> &'static str {
        "always_fail"
    }

    async fn emit(&self, _: &[polku_gateway::Event]) -> Result<(), PluginError> {
        self.emit_count.fetch_add(1, Ordering::SeqCst);
        Err(PluginError::Connection("always fails".into()))
    }

    async fn health(&self) -> bool {
        false
    }
}

/// Emitter that tracks all successful emissions
struct TrackingEmitter {
    event_count: AtomicU64,
}

impl TrackingEmitter {
    fn new() -> Self {
        Self {
            event_count: AtomicU64::new(0),
        }
    }

    fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Emitter for TrackingEmitter {
    fn name(&self) -> &'static str {
        "tracking"
    }

    async fn emit(&self, events: &[polku_gateway::Event]) -> Result<(), PluginError> {
        self.event_count
            .fetch_add(events.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

// ============================================================================
// Integration Tests: Typed Event Data
// ============================================================================

#[tokio::test]
async fn test_typed_event_data_flows_through_resilient_emitter() {
    // Verify that typed event data (network, kernel, etc.) flows correctly
    // through the resilience layer without corruption
    let inner = Arc::new(TrackingEmitter::new());

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .build();

    // Create event with typed network data
    let event = make_typed_network_event("typed-net-1");

    // Verify the event has typed data populated
    assert!(event.data.is_some(), "Event should have typed data");
    assert_eq!(event.severity, 2, "Severity should be Info (2)");
    assert_eq!(event.outcome, 1, "Outcome should be Success (1)");

    // Emit through resilient wrapper
    let result = resilient.emit(&[event]).await;
    assert!(result.is_ok(), "Emit should succeed");
    assert_eq!(inner.event_count(), 1, "One event should be tracked");
}

// ============================================================================
// Integration Tests: Retry + Circuit Breaker
// ============================================================================

#[tokio::test]
async fn test_retry_prevents_circuit_breaker_opening_on_transient_failure() {
    // Scenario: Backend fails twice, then recovers
    // Expected: Retry handles it, circuit breaker never opens
    let inner = Arc::new(FailNTimesEmitter::new(2));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            ..Default::default()
        })
        .with_circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        })
        .build();

    let event = make_test_event("evt-1");

    // Should succeed after retries
    let result = resilient.emit(&[event]).await;
    assert!(result.is_ok(), "Should succeed after retries");

    // Inner emitter was called 3 times (2 failures + 1 success)
    assert_eq!(inner.emit_count(), 3);

    // Health should be true (circuit never opened)
    assert!(resilient.health().await);
}

#[tokio::test]
async fn test_circuit_breaker_prevents_retry_storms() {
    // Scenario: Backend is completely down
    // Expected: Circuit opens after threshold, preventing retry storms
    let inner = Arc::new(AlwaysFailEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_secs(60),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    // First two requests exhaust retries and open circuit
    for i in 0..2 {
        let result = resilient
            .emit(&[make_test_event(&format!("evt-{i}"))])
            .await;
        assert!(result.is_err());
    }

    let calls_before = inner.emit_count();

    // Next request should fail fast (NotReady) without calling inner
    let result = resilient.emit(&[make_test_event("evt-fast-fail")]).await;
    assert!(
        matches!(result, Err(PluginError::NotReady)),
        "Should fail fast with NotReady"
    );

    // Inner emitter should NOT have been called
    assert_eq!(
        inner.emit_count(),
        calls_before,
        "Should not retry when circuit is open"
    );

    // Buffer should have captured the failures
    assert!(buffer.len() >= 2, "Buffer should have captured failures");
}

// ============================================================================
// Integration Tests: Retry + Failure Capture
// ============================================================================

#[tokio::test]
async fn test_failure_capture_after_all_retries_exhausted() {
    let inner = Arc::new(AlwaysFailEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    let events = vec![make_test_event("evt-1"), make_test_event("evt-2")];
    let result = resilient.emit(&events).await;

    assert!(result.is_err());

    // Retry should have tried 4 times (1 initial + 3 retries)
    assert_eq!(inner.emit_count(), 4);

    // Buffer should have both events
    assert_eq!(buffer.len(), 2);

    let failed = buffer.drain(10);
    assert_eq!(failed[0].event.id, "evt-1");
    assert_eq!(failed[1].event.id, "evt-2");
    assert_eq!(failed[0].attempts, 1); // Buffer sees it as first attempt from its perspective
}

#[tokio::test]
async fn test_successful_retry_means_no_capture() {
    let inner = Arc::new(FailNTimesEmitter::new(2)); // Fails twice, succeeds on third
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    let result = resilient.emit(&[make_test_event("evt-1")]).await;

    assert!(result.is_ok());
    assert!(buffer.is_empty(), "Buffer should be empty on success");
}

// ============================================================================
// Integration Tests: Full Stack
// ============================================================================

#[tokio::test]
async fn test_full_resilience_stack_recovery_scenario() {
    // Scenario: Backend fails enough to open circuit, then recovers
    // With max_attempts=1 (2 calls per emission) and failure_threshold=3,
    // we need 3 failed emissions (6 total calls) to open circuit.
    // So inner must fail at least 6 times.
    let inner = Arc::new(FailNTimesEmitter::new(8));
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 1, // 2 total attempts per emission
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 1,
            reset_timeout: Duration::from_millis(10),
            half_open_max_requests: 1,
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    // First 3 emissions fail (each tries 2x), opening circuit
    // Total inner calls: 6, all fail (inner has 8 failures)
    for i in 0..3 {
        let _ = resilient
            .emit(&[make_test_event(&format!("phase1-{i}"))])
            .await;
    }

    // Circuit should be open, requests fail fast
    let fast_fail = resilient.emit(&[make_test_event("fast-fail")]).await;
    assert!(
        matches!(fast_fail, Err(PluginError::NotReady)),
        "Circuit should be open, got {:?}",
        fast_fail
    );

    // Wait for reset timeout
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Half-open probe: inner will fail (calls 7,8), back to open
    let _ = resilient.emit(&[make_test_event("probe-1")]).await;

    // Wait again
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Now inner has exhausted 8 failures, will succeed
    let recovery = resilient.emit(&[make_test_event("recovery")]).await;
    assert!(recovery.is_ok(), "Should recover after backend comes up");

    // Subsequent requests should succeed
    let after_recovery = resilient.emit(&[make_test_event("after-recovery")]).await;
    assert!(after_recovery.is_ok());
}

#[tokio::test]
async fn test_concurrent_emissions_with_resilience() {
    let inner = Arc::new(FailNTimesEmitter::new(10));
    let buffer = Arc::new(FailureBuffer::new(1000));

    let resilient = Arc::new(
        ResilientEmitter::wrap_arc(inner.clone())
            .with_retry(BackoffConfig {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .with_circuit_breaker(CircuitBreakerConfig {
                failure_threshold: 20, // High threshold to not trigger
                ..Default::default()
            })
            .with_default_failure_capture(buffer.clone())
            .build(),
    );

    // Spawn 20 concurrent emissions
    let mut handles = vec![];
    for i in 0..20 {
        let emitter = Arc::clone(&resilient);
        handles.push(tokio::spawn(async move {
            emitter
                .emit(&[make_test_event(&format!("concurrent-{i}"))])
                .await
        }));
    }

    // Wait for all
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Some should succeed, some might fail (depending on timing)
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    // We expect most to eventually succeed due to retries
    assert!(successes > 0, "Some requests should succeed");

    // Any failures should be captured in buffer
    if failures > 0 {
        assert!(!buffer.is_empty(), "Failures should be captured in buffer");
    }
}

// ============================================================================
// System Tests: Hub Integration
// ============================================================================

#[tokio::test]
async fn test_hub_with_resilient_emitter() {
    let inner = Arc::new(TrackingEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_default_retry()
        .with_default_failure_capture(buffer.clone())
        .build();

    let hub = Hub::new().buffer_capacity(100).emitter_arc(resilient);

    let (_, sender, runner) = hub.build();

    // Run hub in background
    let runner_handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(200), runner.run())
            .await
            .ok();
    });

    // Send messages
    for i in 0..10 {
        let msg = Message::new("test-source", format!("evt-{i}"), Bytes::from("payload"));
        sender.send(msg).await.ok();
    }

    // Give time for flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drop sender to close channel
    drop(sender);
    runner_handle.await.ok();

    // Verify events were emitted
    assert!(inner.event_count() > 0, "Events should have been emitted");
    assert!(buffer.is_empty(), "Buffer should be empty (no failures)");
}

#[tokio::test]
async fn test_hub_with_failing_resilient_emitter() {
    let inner = Arc::new(AlwaysFailEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 1,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(60),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    let hub = Hub::new().buffer_capacity(100).emitter_arc(resilient);

    let (_, sender, runner) = hub.build();

    let runner_handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(200), runner.run())
            .await
            .ok();
    });

    // Send messages
    for i in 0..10 {
        let msg = Message::new("test-source", format!("evt-{i}"), Bytes::from("payload"));
        sender.send(msg).await.ok();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(sender);
    runner_handle.await.ok();

    // Events should be captured due to failures
    // Note: exact count depends on batching behavior
    assert!(
        buffer.total_captured() > 0,
        "Buffer should have captured failures"
    );
}

#[tokio::test]
async fn test_hub_graceful_degradation() {
    // Two emitters: one always fails, one always succeeds
    let failing = Arc::new(AlwaysFailEmitter::new());
    let tracking = Arc::new(TrackingEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(100));

    let resilient_failing = ResilientEmitter::wrap_arc(failing.clone())
        .with_retry(BackoffConfig {
            max_attempts: 1,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    let hub = Hub::new()
        .buffer_capacity(100)
        .emitter_arc(resilient_failing)
        .emitter_arc(tracking.clone());

    let (_, sender, runner) = hub.build();

    let runner_handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(200), runner.run())
            .await
            .ok();
    });

    // Send messages
    for i in 0..5 {
        let msg = Message::new("test", format!("evt-{i}"), Bytes::from("data"));
        sender.send(msg).await.ok();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(sender);
    runner_handle.await.ok();

    // Tracking emitter should have received events (graceful degradation)
    assert!(
        tracking.event_count() > 0,
        "Working emitter should receive events even when other fails"
    );

    // Failures should be captured
    assert!(buffer.total_captured() > 0, "Failures should be captured");
}

// ============================================================================
// System Tests: Load and Stress
// ============================================================================

#[tokio::test]
async fn test_high_load_with_intermittent_failures() {
    // Emitter that fails every 5th request
    struct IntermittentEmitter {
        counter: AtomicU64,
        success_count: AtomicU64,
    }

    impl IntermittentEmitter {
        fn new() -> Self {
            Self {
                counter: AtomicU64::new(0),
                success_count: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl Emitter for IntermittentEmitter {
        fn name(&self) -> &'static str {
            "intermittent"
        }

        async fn emit(&self, events: &[polku_gateway::Event]) -> Result<(), PluginError> {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            if count % 5 == 4 {
                // Fail every 5th call
                Err(PluginError::Connection("intermittent failure".into()))
            } else {
                self.success_count
                    .fetch_add(events.len() as u64, Ordering::SeqCst);
                Ok(())
            }
        }

        async fn health(&self) -> bool {
            true
        }
    }

    let inner = Arc::new(IntermittentEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(10000));

    let resilient = ResilientEmitter::wrap_arc(inner.clone())
        .with_retry(BackoffConfig {
            max_attempts: 2,
            initial_delay: Duration::from_micros(100),
            max_delay: Duration::from_millis(1),
            ..Default::default()
        })
        .with_default_failure_capture(buffer.clone())
        .build();

    // Send 100 emissions
    let mut success_count = 0u64;
    let mut fail_count = 0u64;

    for i in 0..100 {
        match resilient
            .emit(&[make_test_event(&format!("load-{i}"))])
            .await
        {
            Ok(()) => success_count += 1,
            Err(_) => fail_count += 1,
        }
    }

    // Most should succeed due to retries
    assert!(
        success_count > fail_count,
        "With retries, successes ({success_count}) should exceed failures ({fail_count})"
    );

    // Any failures should be in buffer
    if fail_count > 0 {
        assert!(buffer.len() > 0);
    }
}

#[tokio::test]
async fn test_buffer_capacity_under_sustained_failure() {
    let inner = Arc::new(AlwaysFailEmitter::new());
    let buffer = Arc::new(FailureBuffer::new(50)); // Small capacity

    let resilient = ResilientEmitter::wrap_arc(inner)
        .with_retry(BackoffConfig {
            max_attempts: 0, // No retries, fail immediately
            ..Default::default()
        })
        .with_failure_capture(
            buffer.clone(),
            FailureCaptureConfig {
                capacity: 50,
                store_full_batch: true,
            },
        )
        .build();

    // Send 100 events that will all fail
    for i in 0..100 {
        let _ = resilient
            .emit(&[make_test_event(&format!("overflow-{i}"))])
            .await;
    }

    // Buffer should be at capacity
    assert_eq!(buffer.len(), 50, "Buffer should be at capacity");

    // Oldest should have been dropped
    assert_eq!(
        buffer.total_dropped(),
        50,
        "50 events should have been dropped"
    );

    // Verify we have the newest events
    let events = buffer.drain(100);
    assert!(events[0].event.id.starts_with("overflow-5")); // 50+ onwards
}

// ============================================================================
// Helper Functions
// ============================================================================

fn make_test_event(id: &str) -> polku_gateway::Event {
    polku_gateway::Event {
        id: id.to_string(),
        timestamp_unix_ns: 0,
        source: "test".to_string(),
        event_type: "test".to_string(),
        metadata: std::collections::HashMap::new(),
        payload: vec![],
        route_to: vec![],
        severity: 0,
        outcome: 0,
        data: None,
    }
}

/// Create a test event with typed network data
fn make_typed_network_event(id: &str) -> polku_gateway::Event {
    use polku_core::NetworkEventData;
    use polku_core::proto::event::Data as EventData;

    polku_gateway::Event {
        id: id.to_string(),
        timestamp_unix_ns: 1704067200_000_000_000, // 2024-01-01T00:00:00Z
        source: "tapio".to_string(),
        event_type: "network.connection".to_string(),
        metadata: [
            ("cluster".to_string(), "prod".to_string()),
            ("namespace".to_string(), "default".to_string()),
        ]
        .into_iter()
        .collect(),
        payload: vec![],
        route_to: vec![],
        severity: 2, // Info
        outcome: 1,  // Success
        data: Some(EventData::Network(NetworkEventData {
            protocol: "TCP".to_string(),
            src_ip: "10.0.0.1".to_string(),
            dst_ip: "10.0.0.2".to_string(),
            src_port: 54321,
            dst_port: 443,
            direction: "outbound".to_string(),
            latency_ms: 15.5,
            bytes_sent: 1024,
            bytes_received: 2048,
            ..Default::default()
        })),
    }
}
