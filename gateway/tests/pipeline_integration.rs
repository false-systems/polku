//! Pipeline integration tests for the Message-first refactor
//!
//! Validates key invariants:
//! - Zero-copy: `Bytes` payload survives pipeline without reallocation
//! - DST: deterministic flush timing with `tokio::time::pause()`
//! - Processing trace: middleware audit trail flows through Hub

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use polku_gateway::{Emitter, Hub, Message, PluginError, Transform};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ============================================================================
// Shared test emitters
// ============================================================================

/// Emitter that captures emitted messages for later inspection
struct CaptureEmitter {
    captured: Mutex<Vec<Vec<Message>>>,
    count: AtomicU64,
}

impl CaptureEmitter {
    fn new() -> Self {
        Self {
            captured: Mutex::new(Vec::new()),
            count: AtomicU64::new(0),
        }
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }

    fn take_all(&self) -> Vec<Message> {
        let batches = self.captured.lock().unwrap();
        batches.iter().flat_map(|b| b.iter().cloned()).collect()
    }
}

#[async_trait::async_trait]
impl Emitter for CaptureEmitter {
    fn name(&self) -> &'static str {
        "capture"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        self.count
            .fetch_add(messages.len() as u64, Ordering::SeqCst);
        self.captured.lock().unwrap().push(messages.to_vec());
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

// ============================================================================
// Zero-copy verification
// ============================================================================

/// Verify that Bytes payload is NOT reallocated through the pipeline.
///
/// Before the Message-first refactor, Event used Vec<u8> which caused
/// a full copy on every Message->Event conversion. Now Bytes (Arc-based)
/// should flow end-to-end without any data copy.
#[tokio::test]
async fn payload_zero_copy_through_pipeline() {
    let payload = Bytes::from(vec![42u8; 10_000]);
    let original_ptr = payload.as_ptr();

    let emitter = Arc::new(CaptureEmitter::new());
    let (_, sender, runner) = Hub::new()
        .buffer_capacity(100)
        .batch_size(10)
        .flush_interval_ms(1)
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send message with known payload
    let msg = Message::new("test", "zero-copy", payload);
    sender.send(msg).await.unwrap();

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shutdown
    drop(sender);
    let _ = handle.await;

    // Verify emitter received the message
    assert_eq!(emitter.count(), 1);

    // Verify zero-copy: payload pointer is the same
    let captured = emitter.take_all();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].payload.len(), 10_000);
    assert_eq!(
        captured[0].payload.as_ptr(),
        original_ptr,
        "Payload should share the same underlying allocation (zero-copy)"
    );
}

/// Verify that metadata is preserved through the pipeline
#[tokio::test]
async fn metadata_preserved_through_pipeline() {
    let emitter = Arc::new(CaptureEmitter::new());
    let (_, sender, runner) = Hub::new()
        .buffer_capacity(100)
        .batch_size(10)
        .flush_interval_ms(1)
        .middleware(Transform::new(|mut msg: Message| {
            msg.metadata_mut()
                .insert("pipeline".into(), "visited".into());
            msg
        }))
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    let msg = Message::new("my-service", "user.created", Bytes::from(r#"{"id":1}"#))
        .with_metadata("custom-header", "my-value");

    let original_id = msg.id;
    sender.send(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(sender);
    let _ = handle.await;

    let captured = emitter.take_all();
    assert_eq!(captured.len(), 1);

    let msg = &captured[0];
    assert_eq!(msg.id, original_id);
    assert_eq!(msg.source, "my-service");
    assert_eq!(msg.message_type, "user.created");
    assert_eq!(
        msg.metadata().get("custom-header"),
        Some(&"my-value".to_string())
    );
    assert_eq!(msg.metadata().get("pipeline"), Some(&"visited".to_string()));
}

// ============================================================================
// Deterministic Simulation Testing (DST)
// ============================================================================

/// Verify that flush fires at exact interval using tokio::time::pause()
///
/// With paused time, we can advance the clock by exactly the flush interval
/// and assert that messages are emitted deterministically.
#[tokio::test(start_paused = true)]
async fn dst_flush_fires_at_exact_interval() {
    let flush_interval_ms = 100;
    let emitter = Arc::new(CaptureEmitter::new());

    let (_, sender, runner) = Hub::new()
        .buffer_capacity(1000)
        .batch_size(1000) // Large batch so we only flush on timer
        .flush_interval_ms(flush_interval_ms)
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send 10 messages (well below batch_size of 1000)
    for i in 0..10 {
        sender
            .send(Message::new("test", format!("evt-{i}"), Bytes::new()))
            .await
            .unwrap();
    }

    // Yield to let the runner receive messages
    tokio::task::yield_now().await;

    // Nothing should be emitted yet (below batch_size, timer hasn't fired)
    assert_eq!(
        emitter.count(),
        0,
        "Should not have flushed before interval"
    );

    // Advance time past the flush interval
    tokio::time::advance(Duration::from_millis(flush_interval_ms + 10)).await;
    // Yield to let the flush loop run
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Now messages should be flushed
    assert_eq!(
        emitter.count(),
        10,
        "All 10 messages should be flushed after interval"
    );

    drop(sender);
    let _ = handle.await;
}

/// Verify that inline flush triggers when batch_size is reached
#[tokio::test(start_paused = true)]
async fn dst_inline_flush_at_batch_size() {
    let emitter = Arc::new(CaptureEmitter::new());

    let (_, sender, runner) = Hub::new()
        .buffer_capacity(1000)
        .batch_size(5) // Small batch — triggers inline flush
        .flush_interval_ms(10_000) // Long interval — should NOT trigger
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send exactly batch_size messages
    for i in 0..5 {
        sender
            .send(Message::new("test", format!("evt-{i}"), Bytes::new()))
            .await
            .unwrap();
    }

    // Yield to let inline flush happen
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Should have flushed via inline path (no timer needed)
    assert_eq!(
        emitter.count(),
        5,
        "Inline flush should trigger at batch_size"
    );

    drop(sender);
    let _ = handle.await;
}

// ============================================================================
// Processing trace integration
// ============================================================================

/// Verify that processing trace metadata flows through the full Hub pipeline
#[tokio::test]
async fn processing_trace_flows_through_hub() {
    let emitter = Arc::new(CaptureEmitter::new());

    let (_, sender, runner) = Hub::new()
        .buffer_capacity(100)
        .batch_size(10)
        .flush_interval_ms(1)
        .middleware(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("step".into(), "1".into());
            msg
        }))
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send message with tracing enabled
    let msg = Message::new("test", "traced-event", Bytes::from("hello"))
        .with_metadata(polku_core::metadata_keys::TRACE_ENABLED, "true");

    sender.send(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(sender);
    let _ = handle.await;

    let captured = emitter.take_all();
    assert_eq!(captured.len(), 1);

    let msg = &captured[0];

    // Verify trace was recorded
    let trace = msg
        .metadata()
        .get(polku_core::metadata_keys::TRACE_LOG)
        .expect("Trace should be present when polku.trace=true");

    // Parse and verify structure
    let entries: Vec<serde_json::Value> = serde_json::from_str(trace).unwrap();
    assert_eq!(entries.len(), 1, "Should have 1 middleware trace entry");
    assert_eq!(entries[0]["mw"], "transform");
    assert_eq!(entries[0]["action"], "passed");
    assert!(entries[0]["us"].is_number());

    // Verify middleware also ran
    assert_eq!(msg.metadata().get("step"), Some(&"1".to_string()));
}

/// Verify that messages without trace enabled don't get trace metadata
#[tokio::test]
async fn no_trace_without_opt_in() {
    let emitter = Arc::new(CaptureEmitter::new());

    let (_, sender, runner) = Hub::new()
        .buffer_capacity(100)
        .batch_size(10)
        .flush_interval_ms(1)
        .middleware(Transform::new(|msg: Message| msg))
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send message WITHOUT trace enabled
    let msg = Message::new("test", "untraced", Bytes::from("hello"));
    sender.send(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(sender);
    let _ = handle.await;

    let captured = emitter.take_all();
    assert_eq!(captured.len(), 1);

    assert!(
        captured[0]
            .metadata()
            .get(polku_core::metadata_keys::TRACE_LOG)
            .is_none(),
        "Untraced messages should not have trace metadata"
    );
}

// ============================================================================
// Batch ordering invariant
// ============================================================================

/// Verify that message ordering is preserved through the pipeline
#[tokio::test]
async fn message_ordering_preserved() {
    let emitter = Arc::new(CaptureEmitter::new());

    let (_, sender, runner) = Hub::new()
        .buffer_capacity(1000)
        .batch_size(100)
        .flush_interval_ms(1)
        .emitter_arc(emitter.clone())
        .build();

    let handle = tokio::spawn(async move { runner.run().await });

    // Send messages with sequential types
    let mut sent_ids = Vec::new();
    for i in 0..50 {
        let msg = Message::new("test", format!("evt-{i}"), Bytes::new());
        sent_ids.push(msg.id);
        sender.send(msg).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(sender);
    let _ = handle.await;

    let captured = emitter.take_all();
    assert_eq!(captured.len(), 50);

    let received_ids: Vec<_> = captured.iter().map(|m| m.id).collect();
    assert_eq!(sent_ids, received_ids, "Message ordering must be preserved");
}
