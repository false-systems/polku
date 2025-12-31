//! Dead Letter Queue emitter
//!
//! Captures failed events for later inspection or replay.
//!
//! # Performance Note
//!
//! The DLQ must clone events on failure because it stores them for later replay.
//! The proto-generated `Event` type uses `Vec<u8>` for payload (not `Bytes`),
//! which means each failed event incurs a full payload copy. For high-throughput
//! scenarios with large payloads, consider:
//!
//! - Using `store_full_batch: false` to only sample the first event
//! - Setting a conservative capacity limit to bound memory usage
//! - Monitoring `total_dropped` to detect capacity pressure
//!
//! This is a trade-off inherent to the protobuf-generated types. The internal
//! `Message` type uses zero-copy `Bytes`, but the gRPC boundary requires `Vec<u8>`.

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::proto::Event;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A failed event with metadata about the failure
///
/// Note: The `event` field is a clone of the original. See module docs for
/// performance implications when storing large payloads.
#[derive(Debug, Clone)]
pub struct FailedEvent {
    /// The original event that failed (cloned from the emission batch)
    pub event: Event,
    /// Error message from the failure
    pub error: String,
    /// Which emitter failed
    pub emitter_name: String,
    /// When the failure occurred
    pub failed_at: Instant,
    /// Number of delivery attempts
    pub attempts: u32,
}

/// Configuration for DLQ behavior
#[derive(Debug, Clone)]
pub struct DLQConfig {
    /// Maximum number of failed events to retain
    pub capacity: usize,
    /// Whether to store all events from batch or just first as sample
    pub store_full_batch: bool,
}

impl Default for DLQConfig {
    fn default() -> Self {
        Self {
            capacity: 1000,
            store_full_batch: true,
        }
    }
}

/// In-memory Dead Letter Queue for capturing failed events
pub struct DeadLetterQueue {
    events: Mutex<VecDeque<FailedEvent>>,
    capacity: usize,
    /// Metrics: total events ever captured
    total_captured: AtomicU64,
    /// Metrics: events dropped due to capacity
    total_dropped: AtomicU64,
}

impl DeadLetterQueue {
    /// Create a new DLQ with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity,
            total_captured: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
        }
    }

    /// Add failed events to the DLQ
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

    /// Drain up to n events from the DLQ for reprocessing
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

    /// Current number of events in DLQ
    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    /// Check if DLQ is empty
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

    /// Clear all events from DLQ
    pub fn clear(&self) {
        self.events.lock().clear();
    }
}

/// Emitter wrapper that captures failed events into a DLQ
pub struct DLQEmitter {
    inner: Arc<dyn Emitter>,
    dlq: Arc<DeadLetterQueue>,
    config: DLQConfig,
}

impl DLQEmitter {
    /// Create a new DLQEmitter
    pub fn new(inner: Arc<dyn Emitter>, dlq: Arc<DeadLetterQueue>, config: DLQConfig) -> Self {
        Self { inner, dlq, config }
    }

    /// Create a DLQEmitter with default configuration
    pub fn with_defaults(inner: Arc<dyn Emitter>, dlq: Arc<DeadLetterQueue>) -> Self {
        Self::new(inner, dlq, DLQConfig::default())
    }

    /// Get reference to the DLQ for inspection/replay
    pub fn dlq(&self) -> &Arc<DeadLetterQueue> {
        &self.dlq
    }
}

#[async_trait]
impl Emitter for DLQEmitter {
    fn name(&self) -> &'static str {
        "dlq"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        match self.inner.emit(events).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Capture failed events to DLQ
                let failed_events: Vec<FailedEvent> = if self.config.store_full_batch {
                    events
                        .iter()
                        .map(|event| FailedEvent {
                            event: event.clone(),
                            error: e.to_string(),
                            emitter_name: self.inner.name().to_string(),
                            failed_at: Instant::now(),
                            attempts: 1,
                        })
                        .collect()
                } else {
                    // Store only first event as representative sample
                    events
                        .first()
                        .map(|event| FailedEvent {
                            event: event.clone(),
                            error: format!("{} (batch of {})", e, events.len()),
                            emitter_name: self.inner.name().to_string(),
                            failed_at: Instant::now(),
                            attempts: 1,
                        })
                        .into_iter()
                        .collect()
                };

                let count = failed_events.len();
                self.dlq.push(failed_events);

                tracing::warn!(
                    emitter = self.inner.name(),
                    error = %e,
                    events_captured = count,
                    dlq_size = self.dlq.len(),
                    "events captured to DLQ"
                );

                // Return the original error (DLQ capture is transparent)
                Err(e)
            }
        }
    }

    async fn health(&self) -> bool {
        self.inner.health().await
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        let pending = self.dlq.len();
        if pending > 0 {
            tracing::warn!(
                emitter = self.inner.name(),
                pending_events = pending,
                total_captured = self.dlq.total_captured(),
                total_dropped = self.dlq.total_dropped(),
                "DLQ shutdown with unprocessed events - consider draining before shutdown"
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
        fn name(&self) -> &'static str {
            "always_failing"
        }
        async fn emit(&self, _: &[Event]) -> Result<(), PluginError> {
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
        fn name(&self) -> &'static str {
            "success"
        }
        async fn emit(&self, _: &[Event]) -> Result<(), PluginError> {
            Ok(())
        }
        async fn health(&self) -> bool {
            true
        }
    }

    fn make_test_event(id: &str) -> Event {
        Event {
            id: id.to_string(),
            timestamp_unix_ns: 0,
            source: "test".to_string(),
            event_type: "test".to_string(),
            metadata: std::collections::HashMap::new(),
            payload: vec![],
            route_to: vec![],
        }
    }

    #[test]
    fn test_dlq_push_and_len() {
        let dlq = DeadLetterQueue::new(100);

        dlq.push(vec![FailedEvent {
            event: make_test_event("e1"),
            error: "test error".into(),
            emitter_name: "test".into(),
            failed_at: Instant::now(),
            attempts: 1,
        }]);

        assert_eq!(dlq.len(), 1);
        assert!(!dlq.is_empty());
    }

    #[test]
    fn test_dlq_capacity_limit() {
        let dlq = DeadLetterQueue::new(3);

        // Push 5 events
        for i in 0..5 {
            dlq.push(vec![FailedEvent {
                event: make_test_event(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Should only have last 3
        assert_eq!(dlq.len(), 3);
        assert_eq!(dlq.total_captured(), 5);
        assert_eq!(dlq.total_dropped(), 2);

        let events = dlq.drain(10);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.id, "evt-2");
        assert_eq!(events[1].event.id, "evt-3");
        assert_eq!(events[2].event.id, "evt-4");
    }

    #[test]
    fn test_dlq_drain() {
        let dlq = DeadLetterQueue::new(100);

        for i in 0..5 {
            dlq.push(vec![FailedEvent {
                event: make_test_event(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Drain 3
        let drained = dlq.drain(3);
        assert_eq!(drained.len(), 3);
        assert_eq!(dlq.len(), 2);

        // Drain remaining
        let drained = dlq.drain(10);
        assert_eq!(drained.len(), 2);
        assert!(dlq.is_empty());
    }

    #[test]
    fn test_dlq_peek() {
        let dlq = DeadLetterQueue::new(100);

        for i in 0..3 {
            dlq.push(vec![FailedEvent {
                event: make_test_event(&format!("evt-{i}")),
                error: "test".into(),
                emitter_name: "test".into(),
                failed_at: Instant::now(),
                attempts: 1,
            }]);
        }

        // Peek doesn't remove
        let peeked = dlq.peek(2);
        assert_eq!(peeked.len(), 2);
        assert_eq!(dlq.len(), 3); // Still 3 in queue
    }

    #[test]
    fn test_dlq_clear() {
        let dlq = DeadLetterQueue::new(100);

        dlq.push(vec![FailedEvent {
            event: make_test_event("e1"),
            error: "test".into(),
            emitter_name: "test".into(),
            failed_at: Instant::now(),
            attempts: 1,
        }]);

        assert_eq!(dlq.len(), 1);
        dlq.clear();
        assert!(dlq.is_empty());
    }

    #[tokio::test]
    async fn test_dlq_emitter_captures_failures() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::with_defaults(inner, dlq.clone());

        let events = vec![make_test_event("e1"), make_test_event("e2")];
        let result = emitter.emit(&events).await;

        assert!(result.is_err());
        assert_eq!(dlq.len(), 2);

        let failed = dlq.drain(10);
        assert_eq!(failed.len(), 2);
        assert_eq!(failed[0].emitter_name, "always_failing");
        assert_eq!(failed[0].event.id, "e1");
        assert_eq!(failed[1].event.id, "e2");
    }

    #[tokio::test]
    async fn test_dlq_emitter_passthrough_success() {
        let inner = Arc::new(SuccessEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::with_defaults(inner, dlq.clone());

        let result = emitter.emit(&[make_test_event("e1")]).await;

        assert!(result.is_ok());
        assert!(dlq.is_empty()); // Nothing captured on success
    }

    #[tokio::test]
    async fn test_dlq_emitter_sample_mode() {
        let inner = Arc::new(AlwaysFailingEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::new(
            inner,
            dlq.clone(),
            DLQConfig {
                capacity: 100,
                store_full_batch: false, // Only store first event
            },
        );

        let events = vec![
            make_test_event("e1"),
            make_test_event("e2"),
            make_test_event("e3"),
        ];
        let _ = emitter.emit(&events).await;

        // Only first event stored
        assert_eq!(dlq.len(), 1);
        let failed = dlq.drain(10);
        assert_eq!(failed[0].event.id, "e1");
        assert!(failed[0].error.contains("batch of 3"));
    }

    #[tokio::test]
    async fn test_dlq_emitter_health_passthrough() {
        let inner = Arc::new(SuccessEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::with_defaults(inner, dlq);

        assert!(emitter.health().await);
    }

    #[tokio::test]
    async fn test_dlq_emitter_shutdown_with_events_logs_warning() {
        // This test verifies the shutdown behavior when DLQ has events.
        // We can't easily test tracing output, but we verify shutdown succeeds
        // and DLQ state is preserved (not cleared).
        let inner = Arc::new(AlwaysFailingEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::with_defaults(inner, dlq.clone());

        // Cause some events to be captured
        let _ = emitter.emit(&[make_test_event("e1")]).await;
        assert_eq!(dlq.len(), 1);

        // Shutdown should succeed
        let result = emitter.shutdown().await;
        assert!(result.is_ok());

        // DLQ should still have the events (not cleared on shutdown)
        assert_eq!(dlq.len(), 1);
    }

    #[tokio::test]
    async fn test_dlq_emitter_shutdown_empty_no_warning() {
        let inner = Arc::new(SuccessEmitter);
        let dlq = Arc::new(DeadLetterQueue::new(100));
        let emitter = DLQEmitter::with_defaults(inner, dlq.clone());

        // No failures, DLQ is empty
        let _ = emitter.emit(&[make_test_event("e1")]).await;
        assert!(dlq.is_empty());

        // Shutdown should succeed without warning
        let result = emitter.shutdown().await;
        assert!(result.is_ok());
    }
}
