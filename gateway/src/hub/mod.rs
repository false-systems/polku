//! Hub - the central pipeline builder for POLKU
//!
//! The Hub provides a builder pattern for configuring and running
//! the message pipeline. No YAML, just code.
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::{Hub, Message, Transform, StdoutOutput};
//!
//! Hub::new()
//!     .middleware(Transform::new(|mut msg| {
//!         msg.metadata.insert("processed".into(), "true".into());
//!         msg
//!     }))
//!     .output(StdoutOutput::new())
//!     .run()
//!     .await?;
//! ```
//!
//! # Buffer Strategies
//!
//! The Hub supports different buffer strategies for different use cases:
//!
//! - **Standard** (default): Fast lock-free buffer, drops new messages when full
//! - **Tiered**: Primary buffer + compressed overflow for graceful degradation
//!
//! ```ignore
//! // Enable tiered buffering for traffic spike handling
//! Hub::new()
//!     .buffer_strategy(BufferStrategy::tiered(10_000, 5_000))
//!     .build();
//! ```

mod buffer;
mod runner;

pub use buffer::{BufferStrategy, HubBuffer};
pub use runner::HubRunner;

use crate::checkpoint::CheckpointStore;
use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use crate::middleware::{Middleware, MiddlewareChain};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;

/// The Hub - central message pipeline
///
/// Connects inputs → middleware → buffer → outputs.
///
/// # Architecture
///
/// ```text
/// Input Channels ──► MiddlewareChain ──► Buffer ──► Outputs (fan-out)
/// ```
///
/// # Buffer Strategies
///
/// - `BufferStrategy::Standard`: Lock-free buffer, drops on overflow (default)
/// - `BufferStrategy::Tiered`: Compressed overflow for graceful degradation
pub struct Hub {
    /// Buffer strategy (standard or tiered)
    buffer_strategy: BufferStrategy,
    /// Input channel capacity (mpsc buffer before middleware)
    channel_capacity: usize,
    /// Flush batch size (messages per flush)
    batch_size: usize,
    /// Flush interval in milliseconds
    flush_interval_ms: u64,
    /// Middleware chain (applied before buffering)
    middleware: MiddlewareChain,
    /// Registered emitters
    emitters: Vec<Arc<dyn Emitter>>,
    /// Optional checkpoint store for reliable delivery tracking
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
}

impl Hub {
    /// Create a new Hub with default settings
    pub fn new() -> Self {
        Self {
            buffer_strategy: BufferStrategy::default(),
            channel_capacity: 8192, // Higher default for better throughput
            batch_size: 100,
            flush_interval_ms: 10,
            middleware: MiddlewareChain::new(),
            emitters: Vec::new(),
            checkpoint_store: None,
        }
    }

    /// Set the flush batch size
    ///
    /// Default is 100 messages per flush.
    /// Larger batches = higher throughput, higher latency.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the flush interval in milliseconds
    ///
    /// Default is 10ms.
    /// Lower = lower latency, higher CPU.
    /// Higher = higher throughput, higher latency.
    pub fn flush_interval_ms(mut self, ms: u64) -> Self {
        self.flush_interval_ms = ms;
        self
    }

    /// Set the buffer capacity (standard strategy)
    ///
    /// Default is 10,000 messages.
    /// This is a convenience method that sets a standard buffer strategy.
    /// For tiered buffering, use `buffer_strategy()` instead.
    pub fn buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_strategy = BufferStrategy::standard(capacity);
        self
    }

    /// Set the buffer strategy
    ///
    /// Controls how messages are buffered and how overflow is handled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Standard buffer (default) - drops on overflow
    /// Hub::new().buffer_strategy(BufferStrategy::standard(10_000))
    ///
    /// // Tiered buffer - compressed overflow for graceful degradation
    /// Hub::new().buffer_strategy(BufferStrategy::tiered(10_000, 5_000))
    /// ```
    pub fn buffer_strategy(mut self, strategy: BufferStrategy) -> Self {
        self.buffer_strategy = strategy;
        self
    }

    /// Set the input channel capacity
    ///
    /// This controls the mpsc channel buffer between send() and the
    /// middleware/buffer. Higher values allow more messages to queue
    /// before backpressure kicks in, improving burst handling.
    ///
    /// Default is 8,192 messages.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Add a middleware to the processing chain
    ///
    /// Middleware is applied in order before messages enter the buffer.
    pub fn middleware<M: Middleware + 'static>(mut self, mw: M) -> Self {
        self.middleware.add(mw);
        self
    }

    /// Add an emitter destination
    ///
    /// All messages are sent to all emitters (fan-out).
    /// Use `route_to` field in Message to control routing.
    pub fn emitter<E: Emitter + 'static>(mut self, emitter: E) -> Self {
        self.emitters.push(Arc::new(emitter));
        self
    }

    /// Add an emitter destination (Arc version)
    pub fn emitter_arc(mut self, emitter: Arc<dyn Emitter>) -> Self {
        self.emitters.push(emitter);
        self
    }

    // Keep old name for compatibility
    pub fn output<E: Emitter + 'static>(self, emitter: E) -> Self {
        self.emitter(emitter)
    }

    /// Enable checkpoint-based acknowledgment for reliable delivery
    ///
    /// When enabled, the Hub tracks which messages each emitter has successfully
    /// processed. This allows:
    /// - Resume from last known position after restarts
    /// - Safe buffer retention based on minimum checkpoint
    ///
    /// # Example
    ///
    /// ```ignore
    /// use polku_gateway::{Hub, MemoryCheckpointStore};
    /// use std::sync::Arc;
    ///
    /// let checkpoints = Arc::new(MemoryCheckpointStore::new());
    /// Hub::new()
    ///     .checkpoint_store(checkpoints)
    ///     .build();
    /// ```
    pub fn checkpoint_store<S: CheckpointStore + 'static>(mut self, store: Arc<S>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Build a message sender for this hub
    ///
    /// Returns a sender that can be used to inject messages into the pipeline.
    /// This is useful for custom inputs or testing.
    pub fn build(self) -> (MessageSender, HubRunner) {
        let (tx, rx) = mpsc::channel(self.channel_capacity);

        let sender = MessageSender { tx };

        // Initialize sequence from checkpoint store to resume after restart.
        // If checkpoints exist, start from max checkpoint + 1 to avoid conflicts.
        // This ensures sequence numbers are always monotonically increasing across restarts.
        let initial_sequence = self
            .checkpoint_store
            .as_ref()
            .and_then(|store| store.all().values().max().map(|max| max + 1))
            .unwrap_or(0);

        let runner = HubRunner {
            rx,
            buffer: self.buffer_strategy.build(),
            batch_size: self.batch_size,
            flush_interval_ms: self.flush_interval_ms,
            middleware: self.middleware,
            emitters: self.emitters,
            checkpoint_store: self.checkpoint_store,
            sequence: Arc::new(AtomicU64::new(initial_sequence)),
        };

        (sender, runner)
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

/// Message sender for injecting messages into the pipeline
#[derive(Clone)]
pub struct MessageSender {
    tx: mpsc::Sender<Message>,
}

impl MessageSender {
    /// Send a message into the pipeline
    pub async fn send(&self, msg: Message) -> Result<(), PluginError> {
        self.tx
            .send(msg)
            .await
            .map_err(|e| PluginError::Send(e.to_string()))
    }

    /// Try to send a message without blocking
    pub fn try_send(&self, msg: Message) -> Result<(), PluginError> {
        self.tx
            .try_send(msg)
            .map_err(|e| PluginError::Send(e.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::emit::{Event, StdoutEmitter};
    use crate::middleware::{Filter, Transform};
    use bytes::Bytes;
    use runner::partition_by_destination_owned;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_hub_builder() {
        let hub = Hub::new()
            .buffer_capacity(1000)
            .middleware(Transform::new(|msg| msg))
            .emitter(StdoutEmitter::new());

        // Verify strategy was set (can't inspect capacity directly, but emitters count is visible)
        assert_eq!(hub.emitters.len(), 1);
    }

    #[test]
    fn test_hub_build() {
        let hub = Hub::new().emitter(StdoutEmitter::new());

        let (sender, runner) = hub.build();

        // Sender should be cloneable
        let _sender2 = sender.clone();

        // Runner should have buffer
        assert_eq!(runner.buffer.capacity(), 10_000);
    }

    #[tokio::test]
    async fn test_message_sender() {
        let hub = Hub::new();
        let (sender, _runner) = hub.build();

        let msg = Message::new("test", "evt", Bytes::from("payload"));
        sender.send(msg).await.expect("should send");
    }

    #[tokio::test]
    async fn test_hub_with_middleware() {
        // Track how many messages pass through
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        struct CountingMiddleware;

        #[async_trait::async_trait]
        impl Middleware for CountingMiddleware {
            fn name(&self) -> &'static str {
                "counter"
            }

            async fn process(&self, msg: Message) -> Option<Message> {
                COUNTER.fetch_add(1, Ordering::Relaxed);
                Some(msg)
            }
        }

        let hub = Hub::new().middleware(CountingMiddleware);

        let (sender, runner) = hub.build();

        // Send messages in background
        let sender_handle = tokio::spawn(async move {
            for i in 0..5 {
                let msg = Message::new("test", format!("evt-{i}"), Bytes::new());
                sender.send(msg).await.ok();
            }
            // Drop sender to close channel
        });

        // Run hub briefly
        let runner_handle = tokio::spawn(async move {
            tokio::time::timeout(tokio::time::Duration::from_millis(100), runner.run())
                .await
                .ok();
        });

        sender_handle.await.ok();
        runner_handle.await.ok();

        // All 5 messages should have been processed
        assert_eq!(COUNTER.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn test_hub_filter() {
        use std::sync::atomic::AtomicU64;

        // Emitter that counts events
        struct CountingEmitter(AtomicU64);

        #[async_trait::async_trait]
        impl crate::emit::Emitter for CountingEmitter {
            fn name(&self) -> &'static str {
                "counter"
            }
            async fn emit(&self, events: &[Event]) -> Result<(), crate::error::PluginError> {
                self.0.fetch_add(events.len() as u64, Ordering::SeqCst);
                Ok(())
            }
            async fn health(&self) -> bool {
                true
            }
        }

        let counter = Arc::new(CountingEmitter(AtomicU64::new(0)));
        let hub = Hub::new()
            // Only allow messages with type "keep"
            .middleware(Filter::new(|msg: &Message| msg.message_type == "keep"))
            .emitter_arc(counter.clone());

        let (sender, runner) = hub.build();

        // Send messages: 2 "keep", 1 "drop"
        sender
            .send(Message::new("test", "keep", Bytes::new()))
            .await
            .ok();
        sender
            .send(Message::new("test", "drop", Bytes::new()))
            .await
            .ok();
        sender
            .send(Message::new("test", "keep", Bytes::new()))
            .await
            .ok();

        // Drop sender to trigger shutdown
        drop(sender);

        // Run to completion
        runner.run().await.ok();

        // Only 2 messages should have been emitted (the "keep" ones)
        assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_drains_buffer() {
        use std::sync::atomic::AtomicU64;

        // Emitter that counts how many events it receives
        struct CountingEmitter {
            count: AtomicU64,
        }

        impl CountingEmitter {
            fn new() -> Self {
                Self {
                    count: AtomicU64::new(0),
                }
            }

            fn count(&self) -> u64 {
                self.count.load(Ordering::SeqCst)
            }
        }

        #[async_trait::async_trait]
        impl crate::emit::Emitter for CountingEmitter {
            fn name(&self) -> &'static str {
                "counter"
            }

            async fn emit(&self, events: &[Event]) -> Result<(), crate::error::PluginError> {
                self.count.fetch_add(events.len() as u64, Ordering::SeqCst);
                Ok(())
            }

            async fn health(&self) -> bool {
                true
            }
        }

        let counter = Arc::new(CountingEmitter::new());
        let hub = Hub::new()
            .buffer_capacity(1000)
            .emitter_arc(counter.clone());

        let (sender, runner) = hub.build();

        // Send 100 messages as fast as possible (will pile up in buffer)
        for i in 0..100 {
            let msg = Message::new("test", format!("evt-{i}"), Bytes::new());
            sender.send(msg).await.expect("send should work");
        }

        // Drop sender to trigger shutdown
        drop(sender);

        // Run hub to completion (NOT a timeout - we want graceful shutdown)
        runner.run().await.expect("hub should shutdown gracefully");

        // ALL 100 messages should have been emitted
        assert_eq!(
            counter.count(),
            100,
            "Graceful shutdown should drain all buffered messages"
        );
    }

    // ========================================================================
    // Partition by destination tests
    // ========================================================================

    /// Named emitter for testing routing
    struct NamedEmitter {
        name: &'static str,
    }

    #[async_trait::async_trait]
    impl crate::emit::Emitter for NamedEmitter {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn emit(&self, _: &[Event]) -> Result<(), crate::error::PluginError> {
            Ok(())
        }
        async fn health(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_partition_broadcast_messages() {
        // 3 emitters
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> = vec![
            Arc::new(NamedEmitter { name: "kafka" }),
            Arc::new(NamedEmitter { name: "stdout" }),
            Arc::new(NamedEmitter { name: "webhook" }),
        ];

        // 5 broadcast messages (empty route_to = goes everywhere)
        let events: Vec<Event> = (0..5)
            .map(|i| Event {
                id: format!("msg-{i}"),
                route_to: vec![], // broadcast
                ..Default::default()
            })
            .collect();

        let batches = partition_by_destination_owned(events, &emitters);

        // Each emitter should get all 5 messages
        assert_eq!(batches.get("kafka").map(|v| v.len()), Some(5));
        assert_eq!(batches.get("stdout").map(|v| v.len()), Some(5));
        assert_eq!(batches.get("webhook").map(|v| v.len()), Some(5));
    }

    #[test]
    fn test_partition_targeted_messages() {
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> = vec![
            Arc::new(NamedEmitter { name: "kafka" }),
            Arc::new(NamedEmitter { name: "stdout" }),
            Arc::new(NamedEmitter { name: "webhook" }),
        ];

        // Mixed routing:
        // - msg-0: kafka only
        // - msg-1: stdout only
        // - msg-2: kafka + webhook
        // - msg-3: broadcast (empty)
        let events = vec![
            Event {
                id: "msg-0".into(),
                route_to: vec!["kafka".into()],
                ..Default::default()
            },
            Event {
                id: "msg-1".into(),
                route_to: vec!["stdout".into()],
                ..Default::default()
            },
            Event {
                id: "msg-2".into(),
                route_to: vec!["kafka".into(), "webhook".into()],
                ..Default::default()
            },
            Event {
                id: "msg-3".into(),
                route_to: vec![], // broadcast
                ..Default::default()
            },
        ];

        let batches = partition_by_destination_owned(events, &emitters);

        // kafka: msg-0, msg-2, msg-3
        let kafka_events = batches.get("kafka").unwrap();
        let kafka_ids: Vec<_> = kafka_events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(kafka_ids, &["msg-0", "msg-2", "msg-3"]);

        // stdout: msg-1, msg-3
        let stdout_events = batches.get("stdout").unwrap();
        let stdout_ids: Vec<_> = stdout_events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(stdout_ids, &["msg-1", "msg-3"]);

        // webhook: msg-2, msg-3
        let webhook_events = batches.get("webhook").unwrap();
        let webhook_ids: Vec<_> = webhook_events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(webhook_ids, &["msg-2", "msg-3"]);
    }

    #[test]
    fn test_partition_no_matching_route() {
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> =
            vec![Arc::new(NamedEmitter { name: "kafka" })];

        // Message routed to non-existent emitter
        let events = vec![Event {
            id: "msg-0".into(),
            route_to: vec!["nonexistent".into()],
            ..Default::default()
        }];

        let batches = partition_by_destination_owned(events, &emitters);

        // kafka should get nothing (route_to doesn't match)
        assert_eq!(batches.get("kafka").map(|v| v.len()), Some(0));
    }

    #[test]
    fn test_partition_single_destination_no_clone() {
        // Test that single-destination events are moved, not cloned
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> = vec![
            Arc::new(NamedEmitter { name: "kafka" }),
            Arc::new(NamedEmitter { name: "stdout" }),
        ];

        // Create events with large payloads to make clone cost visible
        let events: Vec<Event> = (0..3)
            .map(|i| Event {
                id: format!("msg-{i}"),
                // Each goes to only one destination
                route_to: if i % 2 == 0 {
                    vec!["kafka".into()]
                } else {
                    vec!["stdout".into()]
                },
                payload: vec![0u8; 10_000], // 10KB payload
                ..Default::default()
            })
            .collect();

        let batches = partition_by_destination_owned(events, &emitters);

        // kafka: msg-0, msg-2
        assert_eq!(batches.get("kafka").map(|v| v.len()), Some(2));
        // stdout: msg-1
        assert_eq!(batches.get("stdout").map(|v| v.len()), Some(1));

        // Verify the payloads are intact (moved, not corrupted)
        let kafka_events = batches.get("kafka").unwrap();
        assert_eq!(kafka_events[0].payload.len(), 10_000);
        assert_eq!(kafka_events[1].payload.len(), 10_000);
    }

    #[test]
    fn test_partition_multi_destination_clones_correctly() {
        // Test that multi-destination events are cloned to each destination
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> = vec![
            Arc::new(NamedEmitter { name: "kafka" }),
            Arc::new(NamedEmitter { name: "stdout" }),
            Arc::new(NamedEmitter { name: "webhook" }),
        ];

        // One event going to all 3 destinations
        let events = vec![Event {
            id: "broadcast-msg".into(),
            route_to: vec![], // broadcast = all destinations
            payload: vec![42u8; 100],
            ..Default::default()
        }];

        let batches = partition_by_destination_owned(events, &emitters);

        // Each emitter should have the event
        for name in ["kafka", "stdout", "webhook"] {
            let batch = batches.get(name).unwrap();
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].id, "broadcast-msg");
            assert_eq!(batch[0].payload, vec![42u8; 100]);
        }
    }

    // ========================================================================
    // Checkpoint tests
    // ========================================================================

    #[tokio::test]
    async fn test_checkpoint_updates_on_successful_emit() {
        use crate::checkpoint::MemoryCheckpointStore;
        use std::sync::atomic::AtomicUsize;

        struct CountingEmitter {
            count: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl crate::emit::Emitter for CountingEmitter {
            fn name(&self) -> &'static str {
                "counter"
            }
            async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
                self.count
                    .fetch_add(events.len(), std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            async fn health(&self) -> bool {
                true
            }
        }

        let checkpoint_store = Arc::new(MemoryCheckpointStore::new());

        let hub = Hub::new()
            .batch_size(5)
            .flush_interval_ms(1000) // Long interval to force inline flush
            .checkpoint_store(checkpoint_store.clone())
            .emitter(CountingEmitter {
                count: AtomicUsize::new(0),
            });

        let (sender, runner) = hub.build();

        // Spawn the runner
        let handle = tokio::spawn(async move { runner.run().await });

        // Send 5 messages (triggers inline flush)
        for _ in 0..5 {
            sender
                .send(Message::new("test", "test.event", bytes::Bytes::new()))
                .await
                .unwrap();
        }

        // Give time for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Checkpoint should be updated (seq 0-4, so checkpoint = 4)
        assert_eq!(checkpoint_store.get("counter"), Some(4));

        // Send another batch
        for _ in 0..5 {
            sender
                .send(Message::new("test", "test.event", bytes::Bytes::new()))
                .await
                .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Checkpoint should be updated (seq 5-9, so checkpoint = 9)
        assert_eq!(checkpoint_store.get("counter"), Some(9));

        // Shutdown
        drop(sender);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_checkpoint_not_updated_on_emit_failure() {
        use crate::checkpoint::MemoryCheckpointStore;

        struct FailingEmitter;

        #[async_trait::async_trait]
        impl crate::emit::Emitter for FailingEmitter {
            fn name(&self) -> &'static str {
                "failing"
            }
            async fn emit(&self, _events: &[Event]) -> Result<(), PluginError> {
                Err(PluginError::Send("intentional failure".into()))
            }
            async fn health(&self) -> bool {
                false
            }
        }

        let checkpoint_store = Arc::new(MemoryCheckpointStore::new());

        let hub = Hub::new()
            .batch_size(5)
            .flush_interval_ms(1000)
            .checkpoint_store(checkpoint_store.clone())
            .emitter(FailingEmitter);

        let (sender, runner) = hub.build();

        let handle = tokio::spawn(async move { runner.run().await });

        // Send 5 messages
        for _ in 0..5 {
            sender
                .send(Message::new("test", "test.event", bytes::Bytes::new()))
                .await
                .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Checkpoint should NOT be updated due to failure
        assert_eq!(checkpoint_store.get("failing"), None);

        drop(sender);
        let _ = handle.await;
    }

    // ========================================================================
    // Inline flush tests
    // ========================================================================

    #[tokio::test]
    async fn test_inline_flush_triggers_at_threshold() {
        use std::sync::atomic::AtomicU64;
        use std::time::Instant;

        // Emitter that records when it receives events
        struct TimingEmitter {
            count: AtomicU64,
            first_emit_time: parking_lot::Mutex<Option<Instant>>,
        }

        impl TimingEmitter {
            fn new() -> Self {
                Self {
                    count: AtomicU64::new(0),
                    first_emit_time: parking_lot::Mutex::new(None),
                }
            }
        }

        #[async_trait::async_trait]
        impl crate::emit::Emitter for TimingEmitter {
            fn name(&self) -> &'static str {
                "timing"
            }

            async fn emit(&self, events: &[Event]) -> Result<(), crate::error::PluginError> {
                self.count.fetch_add(events.len() as u64, Ordering::SeqCst);
                let mut first = self.first_emit_time.lock();
                if first.is_none() {
                    *first = Some(Instant::now());
                }
                Ok(())
            }

            async fn health(&self) -> bool {
                true
            }
        }

        let emitter = Arc::new(TimingEmitter::new());

        // Long flush interval (1 second) - inline flush should beat this
        // Small batch size (10) - should trigger inline flush quickly
        let hub = Hub::new()
            .batch_size(10)
            .flush_interval_ms(1000) // 1 second - way too slow if we wait for timer
            .emitter_arc(emitter.clone());

        let (sender, runner) = hub.build();
        let start = Instant::now();

        // Spawn runner
        let runner_handle = tokio::spawn(async move { runner.run().await });

        // Send exactly batch_size messages
        for i in 0..10 {
            let msg = Message::new("test", format!("evt-{i}"), Bytes::new());
            sender.send(msg).await.expect("send should work");
        }

        // Wait a bit for inline flush to trigger (should be < 100ms)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check if messages were emitted
        let emitted = emitter.count.load(Ordering::SeqCst);

        // Drop sender to shutdown
        drop(sender);
        let _ = runner_handle.await;

        // With inline flush: messages should be emitted within ~100ms
        // Without inline flush: would take 1000ms (the flush interval)
        let first_emit = emitter.first_emit_time.lock();
        let emit_latency = first_emit.map(|t| t.duration_since(start));

        assert!(
            emitted >= 10,
            "Expected at least 10 messages emitted, got {}",
            emitted
        );

        assert!(
            emit_latency.map(|d| d.as_millis() < 500).unwrap_or(false),
            "Expected emit within 500ms, but took {:?} (inline flush not working)",
            emit_latency
        );
    }

    // ========================================================================
    // Buffer strategy tests
    // ========================================================================

    #[test]
    fn test_buffer_strategy_standard() {
        let hub = Hub::new().buffer_strategy(BufferStrategy::standard(5000));

        let (_sender, runner) = hub.build();

        // Should have standard strategy
        assert_eq!(runner.buffer.strategy_name(), "standard");
        assert_eq!(runner.buffer.capacity(), 5000);
    }

    #[test]
    fn test_buffer_strategy_tiered() {
        let hub = Hub::new().buffer_strategy(BufferStrategy::tiered(3000, 2000));

        let (_sender, runner) = hub.build();

        // Should have tiered strategy
        assert_eq!(runner.buffer.strategy_name(), "tiered");
        // Total capacity = primary + secondary
        assert_eq!(runner.buffer.capacity(), 5000);
    }

    #[tokio::test]
    async fn test_tiered_buffer_handles_overflow() {
        use std::sync::atomic::AtomicU64;

        struct CountingEmitter {
            count: AtomicU64,
        }

        impl CountingEmitter {
            fn new() -> Self {
                Self {
                    count: AtomicU64::new(0),
                }
            }
        }

        #[async_trait::async_trait]
        impl crate::emit::Emitter for CountingEmitter {
            fn name(&self) -> &'static str {
                "counter"
            }

            async fn emit(&self, events: &[Event]) -> Result<(), crate::error::PluginError> {
                self.count.fetch_add(events.len() as u64, Ordering::SeqCst);
                Ok(())
            }

            async fn health(&self) -> bool {
                true
            }
        }

        let emitter = Arc::new(CountingEmitter::new());

        // Tiny primary buffer (5) + overflow buffer (10)
        // This forces overflow to secondary tier
        let hub = Hub::new()
            .buffer_strategy(BufferStrategy::tiered(5, 10))
            .batch_size(20) // Large batch to drain all at once
            .flush_interval_ms(1)
            .emitter_arc(emitter.clone());

        let (sender, runner) = hub.build();
        let runner_handle = tokio::spawn(async move { runner.run().await });

        // Send 12 messages - 5 go to primary, 7 overflow to secondary (compressed)
        for i in 0..12 {
            let msg = Message::new("test", format!("evt-{i}"), Bytes::from("test payload"));
            sender.send(msg).await.expect("send should work");
        }

        // Wait for flush
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Shutdown
        drop(sender);
        let _ = runner_handle.await;

        // All 12 messages should have been processed (none dropped)
        let emitted = emitter.count.load(Ordering::SeqCst);
        assert!(
            emitted >= 12,
            "Expected at least 12 messages emitted with tiered buffer, got {}",
            emitted
        );
    }

    #[test]
    fn test_buffer_capacity_sets_standard_strategy() {
        // buffer_capacity() should set a standard strategy
        let hub = Hub::new().buffer_capacity(7500);

        let (_sender, runner) = hub.build();

        assert_eq!(runner.buffer.strategy_name(), "standard");
        assert_eq!(runner.buffer.capacity(), 7500);
    }
}
