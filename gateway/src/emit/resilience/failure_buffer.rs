//! Failure buffer for capturing failed emissions
//!
//! In-memory buffer for inspecting failed messages. NOT a persistent dead letter queue.
//! Messages are lost on process restart - this is for debugging/inspection only.
//!
//! For true dead letter queue semantics, use external infrastructure (Kafka, SQS, etc.)
//!
//! # Performance Note
//!
//! The buffer must clone messages on failure because it stores them for inspection.
//! `Message` uses `Bytes` for payload, which is Arc-based and cheap to clone
//! (just a reference count increment). For high-throughput scenarios, consider:
//!
//! - Using `store_full_batch: false` to only sample the first message
//! - Setting a conservative capacity limit to bound memory usage
//! - Monitoring `total_dropped` to detect capacity pressure

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A failed event with metadata about the failure
///
/// Note: The `message` field is a clone of the original. See module docs for
/// performance implications when storing large payloads.
#[derive(Debug, Clone)]
pub struct FailedEvent {
    /// The original message that failed (cloned from the emission batch)
    pub message: Message,
    /// Error message from the failure
    pub error: String,
    /// Which emitter failed
    pub emitter_name: String,
    /// When the failure occurred
    pub failed_at: Instant,
    /// Number of delivery attempts
    pub attempts: u32,
}

/// Configuration for failure capture behavior
#[derive(Debug, Clone)]
pub struct FailureCaptureConfig {
    /// Maximum number of failed events to retain
    pub capacity: usize,
    /// Whether to store all events from batch or just first as sample
    pub store_full_batch: bool,
}

impl Default for FailureCaptureConfig {
    fn default() -> Self {
        Self {
            capacity: 1000,
            store_full_batch: true,
        }
    }
}

/// In-memory buffer for capturing failed events (for inspection, not persistence)
pub struct FailureBuffer {
    events: Mutex<VecDeque<FailedEvent>>,
    capacity: usize,
    /// Metrics: total events ever captured
    total_captured: AtomicU64,
    /// Metrics: events dropped due to capacity
    total_dropped: AtomicU64,
}

impl FailureBuffer {
    /// Create a new failure buffer with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity,
            total_captured: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
        }
    }

    /// Add failed events to the buffer
    pub fn push(&self, events: Vec<FailedEvent>) {
        let mut queue = self.events.lock();
        let count = events.len() as u64;
        let mut dropped = 0u64;

        for event in events {
            if queue.len() >= self.capacity {
                queue.pop_front();
                dropped += 1;
            }
            queue.push_back(event);
        }

        self.total_captured.fetch_add(count, Ordering::Relaxed);
        if dropped > 0 {
            self.total_dropped.fetch_add(dropped, Ordering::Relaxed);
        }
    }

    /// Drain up to n events from the buffer for reprocessing
    pub fn drain(&self, n: usize) -> Vec<FailedEvent> {
        let mut queue = self.events.lock();
        let drain_count = n.min(queue.len());
        queue.drain(..drain_count).collect()
    }

    /// Peek at events without removing them
    pub fn peek(&self, n: usize) -> Vec<FailedEvent> {
        let queue = self.events.lock();
        queue.iter().take(n).cloned().collect()
    }

    /// Current number of events in buffer
    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.events.lock().is_empty()
    }

    /// Total events ever captured
    pub fn total_captured(&self) -> u64 {
        self.total_captured.load(Ordering::Relaxed)
    }

    /// Total events dropped due to capacity
    pub fn total_dropped(&self) -> u64 {
        self.total_dropped.load(Ordering::Relaxed)
    }

    /// Clear all events from buffer
    pub fn clear(&self) {
        self.events.lock().clear();
    }
}

/// Emitter wrapper that captures failed events to a failure buffer
pub struct FailureCaptureEmitter {
    inner: Arc<dyn Emitter>,
    buffer: Arc<FailureBuffer>,
    config: FailureCaptureConfig,
}

impl FailureCaptureEmitter {
    /// Create a new FailureCaptureEmitter
    pub fn new(
        inner: Arc<dyn Emitter>,
        buffer: Arc<FailureBuffer>,
        config: FailureCaptureConfig,
    ) -> Self {
        Self {
            inner,
            buffer,
            config,
        }
    }

    /// Create a FailureCaptureEmitter with default configuration
    pub fn with_defaults(inner: Arc<dyn Emitter>, buffer: Arc<FailureBuffer>) -> Self {
        Self::new(inner, buffer, FailureCaptureConfig::default())
    }

    /// Get reference to the buffer for inspection/replay
    pub fn buffer(&self) -> &Arc<FailureBuffer> {
        &self.buffer
    }
}

#[async_trait]
impl Emitter for FailureCaptureEmitter {
    fn name(&self) -> &str {
        "failure_capture"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        match self.inner.emit(messages).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Capture failed messages to buffer
                let failed_events: Vec<FailedEvent> = if self.config.store_full_batch {
                    messages
                        .iter()
                        .map(|msg| FailedEvent {
                            message: msg.clone(),
                            error: e.to_string(),
                            emitter_name: self.inner.name().to_string(),
                            failed_at: Instant::now(),
                            attempts: 1,
                        })
                        .collect()
                } else {
                    // Store only first message as representative sample
                    messages
                        .first()
                        .map(|msg| FailedEvent {
                            message: msg.clone(),
                            error: format!("{} (batch of {})", e, messages.len()),
                            emitter_name: self.inner.name().to_string(),
                            failed_at: Instant::now(),
                            attempts: 1,
                        })
                        .into_iter()
                        .collect()
                };

                let count = failed_events.len();
                self.buffer.push(failed_events);

                tracing::warn!(
                    emitter = self.inner.name(),
                    error = %e,
                    messages_captured = count,
                    buffer_size = self.buffer.len(),
                    "messages captured to failure buffer"
                );

                // Return the original error (capture is transparent)
                Err(e)
            }
        }
    }

    async fn health(&self) -> bool {
        self.inner.health().await
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        let pending = self.buffer.len();
        if pending > 0 {
            tracing::warn!(
                emitter = self.inner.name(),
                pending_events = pending,
                total_captured = self.buffer.total_captured(),
                total_dropped = self.buffer.total_dropped(),
                "failure buffer has unprocessed events - consider draining before shutdown"
            );
        }
        self.inner.shutdown().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Always failing emitter
    struct AlwaysFailingEmitter;

    #[async_trait]
    impl Emitter for AlwaysFailingEmitter {
        fn name(&self) -> &str {
            "always_failing"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            Err(PluginError::Connection("always fails".into()))
        }
        async fn health(&self) -> bool {
            false
        }
    }

    /// Always succeeding emitter
    struct SuccessEmitter;

    #[async_trait]
    impl Emitter for SuccessEmitter {
        fn name(&self) -> &str {
            "success"
        }
        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
            Ok(())
        }
        async fn health(&self) -> bool {
            true
        }
    }

    fn make_test_message(id: &str) -> Message {
        Message::with_id(id, 0, "test", "test", bytes::Bytes::new())
    }

    #[test]
    fn test_buffer_push_and_len() {
        let buffer = FailureBuffer::new(100);

        buffer.push(vec![FailedEvent {
            message: make_test_message("e1"),
            error: "test error".into(),
            emitter_name: "test".into(),
            failed_at: Instant::now(),
            attempts: 1,
        }]);

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_buffer_capacity_limit() {
        let buffer = FailureBuffer::new(3);

        // Push 5 events
        for i in 0..5 {
            buffer.push(vec![FailedEvent {
                message: make_test_message(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Should only have last 3
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.total_captured(), 5);
        assert_eq!(buffer.total_dropped(), 2);

        let events = buffer.drain(10);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message.id, "evt-2");
        assert_eq!(events[1].message.id, "evt-3");
        assert_eq!(events[2].message.id, "evt-4");
    }

    #[test]
    fn test_buffer_drain() {
        let buffer = FailureBuffer::new(100);

        for i in 0..5 {
            buffer.push(vec![FailedEvent {
                message: make_test_message(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Drain 3
        let drained = buffer.drain(3);
        assert_eq!(drained.len(), 3);
        assert_eq!(buffer.len(), 2);

        // Drain remaining
        let drained = buffer.drain(10);
        assert_eq!(drained.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_peek() {
        let buffer = FailureBuffer::new(100);

        for i in 0..3 {
            buffer.push(vec![FailedEvent {
                message: make_test_message(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Peek doesn't remove
        let peeked = buffer.peek(2);
        assert_eq!(peeked.len(), 2);
        assert_eq!(buffer.len(), 3); // Still 3 in queue
    }

    #[test]
    fn test_buffer_clear() {
        let buffer = FailureBuffer::new(100);

        buffer.push(vec![FailedEvent {
            message: make_test_message("e1"),
            error: "test".into(),
            emitter_name: "test".into(),
            failed_at: Instant::now(),
            attempts: 1,
        }]);

        assert_eq!(buffer.len(), 1);
        buffer.clear();
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_buffer_emitter_captures_failures() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::with_defaults(inner, buffer.clone());

        let events = vec![make_test_message("e1"), make_test_message("e2")];
        let result = emitter.emit(&events).await;

        assert!(result.is_err());
        assert_eq!(buffer.len(), 2);

        let failed = buffer.drain(10);
        assert_eq!(failed.len(), 2);
        assert_eq!(failed[0].emitter_name, "always_failing");
        assert_eq!(failed[0].message.id, "e1");
        assert_eq!(failed[1].message.id, "e2");
    }

    #[tokio::test]
    async fn test_buffer_emitter_passthrough_success() {
        let inner = Arc::new(SuccessEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::with_defaults(inner, buffer.clone());

        let result = emitter.emit(&[make_test_message("e1")]).await;

        assert!(result.is_ok());
        assert!(buffer.is_empty()); // Nothing captured on success
    }

    #[tokio::test]
    async fn test_buffer_emitter_sample_mode() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::new(
            inner,
            buffer.clone(),
            FailureCaptureConfig {
                capacity: 100,
                store_full_batch: false, // Only store first event
            },
        );

        let events = vec![
            make_test_message("e1"),
            make_test_message("e2"),
            make_test_message("e3"),
        ];
        let _ = emitter.emit(&events).await;

        // Only first event stored
        assert_eq!(buffer.len(), 1);
        let failed = buffer.drain(10);
        assert_eq!(failed[0].message.id, "e1");
        assert!(failed[0].error.contains("batch of 3"));
    }

    #[tokio::test]
    async fn test_buffer_emitter_health_passthrough() {
        let inner = Arc::new(SuccessEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::with_defaults(inner, buffer.clone());

        assert!(emitter.health().await);
    }

    #[tokio::test]
    async fn test_buffer_emitter_shutdown_with_events_logs_warning() {
        // This test verifies the shutdown behavior when buffer has events.
        // We can't easily test tracing output, but we verify shutdown succeeds
        // and buffer state is preserved (not cleared).
        let inner = Arc::new(AlwaysFailingEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::with_defaults(inner, buffer.clone());

        // Cause some events to be captured
        let _ = emitter.emit(&[make_test_message("e1")]).await;
        assert_eq!(buffer.len(), 1);

        // Shutdown should succeed
        let result = emitter.shutdown().await;
        assert!(result.is_ok());

        // buffer should still have the events (not cleared on shutdown)
        assert_eq!(buffer.len(), 1);
    }

    #[tokio::test]
    async fn test_buffer_emitter_shutdown_empty_no_warning() {
        let inner = Arc::new(SuccessEmitter);
        let buffer = Arc::new(FailureBuffer::new(100));
        let emitter = FailureCaptureEmitter::with_defaults(inner, buffer.clone());

        // No failures, buffer is empty
        let _ = emitter.emit(&[make_test_message("e1")]).await;
        assert!(buffer.is_empty());

        // Shutdown should succeed without warning
        let result = emitter.shutdown().await;
        assert!(result.is_ok());
    }
}
