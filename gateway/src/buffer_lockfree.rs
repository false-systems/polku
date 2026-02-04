//! Lock-free ring buffer for high-performance message buffering
//!
//! This replaces the Mutex-based RingBuffer with a lock-free MPSC ring buffer
//! using crossbeam-queue. For POLKU's "Monster" architecture, this eliminates
//! the primary contention point in the pipeline.
//!
//! # Performance Target
//!
//! - Current: ~3M msgs/sec (Mutex-based)
//! - Target:  >10M msgs/sec (lock-free)
//!
//! # Design
//!
//! Uses ArrayQueue from crossbeam for bounded MPSC. When full, new incoming
//! messages are dropped and existing (older) messages are retained. This
//! avoids blocking the pipeline while preserving buffered data.
//!
//! # Variants
//!
//! - `LockFreeBuffer`: Stores owned `Message` (moves data into buffer)
//! - `SharedBuffer`: Stores `SharedMessage` (Arc pointer, zero-copy fan-out)

use crate::message::Message;
use crate::metrics::Metrics;
use crate::shared_message::SharedMessage;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free ring buffer for messages
///
/// Uses crossbeam's ArrayQueue internally, which provides:
/// - Lock-free push/pop operations
/// - Bounded capacity (no unbounded growth)
/// - MPSC semantics (multiple producers, single consumer)
///
/// Cloning a buffer creates a new handle to the same underlying queue
/// and metrics - all clones share the same counters for accurate totals.
pub struct LockFreeBuffer {
    /// The underlying lock-free queue
    queue: Arc<ArrayQueue<Message>>,
    /// Capacity
    capacity: usize,
    /// Metrics (shared across clones)
    metrics: Arc<BufferMetrics>,
}

/// Metrics for buffer monitoring
///
/// Wrapped in Arc and shared across all cloned buffer handles,
/// so metrics reflect the global state across all producers/consumers.
pub struct BufferMetrics {
    /// Total messages pushed successfully
    pub pushed: AtomicU64,
    /// Total messages dropped due to full buffer
    pub dropped: AtomicU64,
    /// Total messages drained
    pub drained: AtomicU64,
}

impl Default for BufferMetrics {
    fn default() -> Self {
        Self {
            pushed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
        }
    }
}

impl LockFreeBuffer {
    /// Create a new lock-free buffer with the given capacity
    ///
    /// Capacity is used as provided; no rounding is performed.
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
            capacity,
            metrics: Arc::new(BufferMetrics::default()),
        }
    }

    /// Push a single message into the buffer
    ///
    /// Returns true if pushed successfully, false if dropped due to overflow.
    pub fn push(&self, msg: Message) -> bool {
        self.try_push(msg).is_ok()
    }

    /// Try to push a message, returning the message back on failure
    ///
    /// Returns Ok(()) if pushed successfully, Err(msg) if buffer is full.
    /// This allows callers to handle overflow without cloning upfront.
    #[allow(clippy::result_large_err)]
    pub fn try_push(&self, msg: Message) -> Result<(), Message> {
        match self.queue.push(msg) {
            Ok(()) => {
                self.metrics.pushed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(rejected) => {
                // Buffer full - return message to caller
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                // Export overflow to Prometheus
                if let Some(m) = Metrics::get() {
                    m.record_buffer_overflow(1);
                }
                Err(rejected)
            }
        }
    }

    /// Push multiple messages, returning count of dropped messages
    pub fn push_batch(&self, messages: Vec<Message>) -> usize {
        let mut dropped = 0;
        for msg in messages {
            if !self.push(msg) {
                dropped += 1;
            }
        }
        dropped
    }

    /// Drain up to `n` messages from the buffer
    ///
    /// Returns messages in FIFO order. This is the hot path -
    /// it's called by the flush loop.
    pub fn drain(&self, n: usize) -> Vec<Message> {
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            match self.queue.pop() {
                Some(msg) => result.push(msg),
                None => break,
            }
        }
        self.metrics
            .drained
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        result
    }

    /// Get approximate current length
    ///
    /// Note: This is racy in concurrent scenarios but useful for metrics.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get total messages pushed
    pub fn total_pushed(&self) -> u64 {
        self.metrics.pushed.load(Ordering::Relaxed)
    }

    /// Get total messages dropped
    pub fn total_dropped(&self) -> u64 {
        self.metrics.dropped.load(Ordering::Relaxed)
    }

    /// Get total messages drained
    pub fn total_drained(&self) -> u64 {
        self.metrics.drained.load(Ordering::Relaxed)
    }
}

// Allow cloning the buffer handle (shares queue AND metrics)
impl Clone for LockFreeBuffer {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            capacity: self.capacity,
            metrics: Arc::clone(&self.metrics), // Share metrics across clones
        }
    }
}

// ============================================================================
// SharedBuffer: Zero-copy fan-out via Arc<Message>
// ============================================================================

/// Zero-copy buffer for fan-out scenarios
///
/// Uses `SharedMessage` (Arc<Message>) internally, so messages can be
/// sent to multiple consumers without copying. Each consumer gets a
/// reference-counted pointer to the same underlying data.
///
/// # When to Use
///
/// - Fan-out to multiple emitters
/// - Retry/redelivery scenarios
/// - When the same message goes to 3+ destinations
///
/// # Performance
///
/// Push/pop are still lock-free. The only overhead vs `LockFreeBuffer`
/// is the Arc refcount operations (a few nanoseconds per operation).
///
/// Cloning shares both the queue and metrics across all handles.
pub struct SharedBuffer {
    queue: Arc<ArrayQueue<SharedMessage>>,
    capacity: usize,
    metrics: Arc<BufferMetrics>,
}

impl SharedBuffer {
    /// Create a new shared buffer with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
            capacity,
            metrics: Arc::new(BufferMetrics::default()),
        }
    }

    /// Push a message (wraps in Arc automatically)
    #[inline]
    pub fn push(&self, msg: Message) -> bool {
        self.push_shared(SharedMessage::new(msg))
    }

    /// Push an already-shared message
    #[inline]
    pub fn push_shared(&self, msg: SharedMessage) -> bool {
        match self.queue.push(msg) {
            Ok(()) => {
                self.metrics.pushed.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Drain up to `n` shared messages
    ///
    /// Returns `SharedMessage` which can be cloned for fan-out without
    /// copying the underlying data.
    pub fn drain(&self, n: usize) -> Vec<SharedMessage> {
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            match self.queue.pop() {
                Some(msg) => result.push(msg),
                None => break,
            }
        }
        self.metrics
            .drained
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        result
    }

    /// Get approximate current length
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Get buffer capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get total messages pushed
    pub fn total_pushed(&self) -> u64 {
        self.metrics.pushed.load(Ordering::Relaxed)
    }

    /// Get total messages dropped
    pub fn total_dropped(&self) -> u64 {
        self.metrics.dropped.load(Ordering::Relaxed)
    }

    /// Get total messages drained
    pub fn total_drained(&self) -> u64 {
        self.metrics.drained.load(Ordering::Relaxed)
    }
}

impl Clone for SharedBuffer {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            capacity: self.capacity,
            metrics: Arc::clone(&self.metrics), // Share metrics across clones
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;

    fn make_message(id: &str) -> Message {
        Message::with_id(id, 0, "test", "test", Bytes::new())
    }

    #[test]
    fn test_push_and_drain() {
        let buffer = LockFreeBuffer::new(10);

        // Push 5 messages
        for i in 0..5 {
            assert!(buffer.push(make_message(&format!("msg-{i}"))));
        }

        assert_eq!(buffer.len(), 5);

        // Drain 3 messages
        let drained = buffer.drain(3);
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].id, "msg-0");
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_overflow_drops_new() {
        let buffer = LockFreeBuffer::new(3);

        // Push 5 messages into buffer of size 3
        // Unlike Mutex-based version, this drops NEW messages (not oldest)
        for i in 0..5 {
            buffer.push(make_message(&format!("msg-{i}")));
        }

        assert_eq!(buffer.total_dropped(), 2);
        assert_eq!(buffer.len(), 3);

        // Should have messages 0, 1, 2 (oldest kept)
        let drained = buffer.drain(3);
        assert_eq!(drained[0].id, "msg-0");
        assert_eq!(drained[1].id, "msg-1");
        assert_eq!(drained[2].id, "msg-2");
    }

    #[test]
    fn test_concurrent_access() {
        let buffer = LockFreeBuffer::new(10_000);
        let mut handles = vec![];

        // 4 producer threads
        for t in 0..4 {
            let buf = buffer.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    buf.push(make_message(&format!("t{t}-{i}")));
                }
            }));
        }

        // Wait for producers
        for h in handles {
            h.join().expect("thread should complete");
        }

        // Should have all 4000 messages (buffer is large enough)
        assert_eq!(buffer.len(), 4000);
        assert_eq!(buffer.total_dropped(), 0);
    }

    #[test]
    fn test_mpsc_pattern() {
        // This tests the actual MPSC pattern: multiple producers, one consumer
        let buffer = LockFreeBuffer::new(10_000);
        let buf_clone = buffer.clone();

        // Consumer thread
        let consumer = thread::spawn(move || {
            let mut total = 0;
            while total < 4000 {
                let drained = buf_clone.drain(100);
                total += drained.len();
                if drained.is_empty() {
                    thread::yield_now();
                }
            }
            total
        });

        // Producer threads
        let mut producers = vec![];
        for t in 0..4 {
            let buf = buffer.clone();
            producers.push(thread::spawn(move || {
                for i in 0..1000 {
                    buf.push(make_message(&format!("t{t}-{i}")));
                }
            }));
        }

        // Wait for producers
        for p in producers {
            p.join().expect("producer should complete");
        }

        // Wait for consumer
        let consumed = consumer.join().expect("consumer should complete");
        assert_eq!(consumed, 4000);
    }

    // ========================================================================
    // SharedBuffer tests
    // ========================================================================

    #[test]
    fn test_shared_buffer_zero_copy_fanout() {
        let buffer = SharedBuffer::new(100);

        // Push a message with a large payload
        let payload = Bytes::from(vec![42u8; 10_000]);
        let payload_ptr = payload.as_ptr();
        let msg = Message::new("svc", "evt", payload);
        buffer.push(msg);

        // Drain and fan-out to 5 "consumers"
        let drained = buffer.drain(1);
        assert_eq!(drained.len(), 1);

        let shared = &drained[0];
        let clones: Vec<_> = (0..5).map(|_| shared.clone()).collect();

        // All clones point to the same payload data
        for clone in &clones {
            assert_eq!(clone.payload.as_ptr(), payload_ptr);
        }

        // Ref count: original + 5 clones
        assert_eq!(shared.ref_count(), 6);
    }

    #[test]
    fn test_shared_buffer_concurrent() {
        let buffer = SharedBuffer::new(10_000);
        let mut handles = vec![];

        // 4 producer threads
        for t in 0..4 {
            let buf = buffer.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    buf.push(make_message(&format!("t{t}-{i}")));
                }
            }));
        }

        for h in handles {
            h.join().expect("thread should complete");
        }

        assert_eq!(buffer.len(), 4000);
        assert_eq!(buffer.total_dropped(), 0);
    }
}
