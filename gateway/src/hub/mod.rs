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
use crate::ingest::{IngestContext, Ingestor, IngestorRegistry};
use crate::manifest::{BufferDesc, ComponentDesc, PipelineManifest, TuningDesc};
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
    /// Ingestor registry (Ingest Context)
    ingestors: IngestorRegistry,
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
            ingestors: IngestorRegistry::new(),
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

    /// Add an ingestor for transforming raw bytes to Events
    ///
    /// Ingestors are automatically registered for all sources they declare
    /// via their `sources()` method.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use polku_gateway::{Hub, ingest::{JsonIngestor, PassthroughIngestor}};
    ///
    /// Hub::new()
    ///     .ingestor(JsonIngestor::new())          // handles "json", "json-lines", "ndjson"
    ///     .ingestor(PassthroughIngestor::new())   // handles "passthrough", "polku"
    ///     .build();
    /// ```
    pub fn ingestor<I: Ingestor + 'static>(mut self, ingestor: I) -> Self {
        self.ingestors.add(Arc::new(ingestor));
        self
    }

    /// Add an ingestor (Arc version)
    pub fn ingestor_arc(mut self, ingestor: Arc<dyn Ingestor>) -> Self {
        self.ingestors.add(ingestor);
        self
    }

    /// Set a default ingestor for unknown sources
    ///
    /// When raw data arrives from an unregistered source, the default
    /// ingestor will be used instead of returning an error.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use polku_gateway::{Hub, ingest::JsonIngestor};
    ///
    /// Hub::new()
    ///     .default_ingestor(JsonIngestor::new())  // Unknown sources parsed as JSON
    ///     .build();
    /// ```
    pub fn default_ingestor<I: Ingestor + 'static>(mut self, ingestor: I) -> Self {
        self.ingestors.set_default(Arc::new(ingestor));
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

    /// Build senders for this hub
    ///
    /// Returns:
    /// - `RawSender` - for injecting raw bytes that need ingestion
    /// - `MessageSender` - for injecting pre-built Messages
    /// - `HubRunner` - the pipeline runner
    ///
    /// # Example
    ///
    /// ```ignore
    /// use polku_gateway::{Hub, ingest::JsonIngestor};
    ///
    /// let (raw_sender, msg_sender, runner) = Hub::new()
    ///     .ingestor(JsonIngestor::new())
    ///     .emitter(StdoutEmitter::new())
    ///     .build();
    ///
    /// // Spawn the runner
    /// tokio::spawn(runner.run());
    ///
    /// // Send raw JSON - transformed by ingestor
    /// raw_sender.send("json", "prod", "json", br#"{"source": "svc"}"#).await?;
    ///
    /// // Or send a pre-built Message
    /// msg_sender.send(Message::new("svc", "evt", Bytes::new())).await?;
    /// ```
    pub fn build(self) -> (RawSender, MessageSender, HubRunner) {
        let (tx, rx) = mpsc::channel(self.channel_capacity);

        // Wrap ingestors in Arc for sharing with RawSender
        let ingestors = Arc::new(self.ingestors);

        let raw_sender = RawSender {
            tx: tx.clone(),
            ingestors,
        };

        let msg_sender = MessageSender { tx };

        // Initialize sequence from checkpoint store to resume after restart.
        // If checkpoints exist, start from max checkpoint + 1 to avoid conflicts.
        // This ensures sequence numbers are always monotonically increasing across restarts.
        let initial_sequence = self
            .checkpoint_store
            .as_ref()
            .and_then(|store| store.all().values().max().map(|max| max + 1))
            .unwrap_or(0);

        let buffer = self.buffer_strategy.build();

        // Generate pipeline manifest (self-describing topology)
        let manifest = Arc::new(PipelineManifest {
            version: "1".to_string(),
            middleware: self
                .middleware
                .names()
                .into_iter()
                .enumerate()
                .map(|(i, name)| ComponentDesc {
                    name: name.to_string(),
                    kind: "middleware".to_string(),
                    position: i,
                })
                .collect(),
            emitters: self
                .emitters
                .iter()
                .enumerate()
                .map(|(i, e)| ComponentDesc {
                    name: e.name().to_string(),
                    kind: "emitter".to_string(),
                    position: i,
                })
                .collect(),
            buffer: BufferDesc {
                strategy: buffer.strategy_name().to_string(),
                capacity: buffer.capacity(),
            },
            tuning: TuningDesc {
                batch_size: self.batch_size,
                flush_interval_ms: self.flush_interval_ms,
                channel_capacity: self.channel_capacity,
            },
        });

        let runner = HubRunner {
            rx,
            buffer,
            batch_size: self.batch_size,
            flush_interval_ms: self.flush_interval_ms,
            middleware: self.middleware,
            emitters: self.emitters,
            checkpoint_store: self.checkpoint_store,
            sequence: Arc::new(AtomicU64::new(initial_sequence)),
            manifest,
        };

        (raw_sender, msg_sender, runner)
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

/// Raw bytes sender for injecting data that needs ingestion
///
/// This sender accepts raw bytes along with context (source, cluster, format)
/// and uses the registered ingestors to transform them into Messages before
/// sending them into the pipeline.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::{Hub, ingest::JsonIngestor};
///
/// let hub = Hub::new()
///     .ingestor(JsonIngestor::new())
///     .build();
///
/// let (raw_sender, msg_sender, runner) = hub;
///
/// // Send raw JSON bytes - will be transformed by JsonIngestor
/// raw_sender.send("json", "prod", "json", br#"{"id": "evt-1", "source": "svc"}"#).await?;
/// ```
#[derive(Clone)]
pub struct RawSender {
    tx: mpsc::Sender<Message>,
    ingestors: Arc<IngestorRegistry>,
}

impl RawSender {
    /// Send raw bytes into the pipeline after ingestion
    ///
    /// The ingestor for the given source will transform the raw bytes into
    /// one or more Messages and send them into the pipeline.
    ///
    /// # Arguments
    /// * `source` - Source identifier (used to find the ingestor)
    /// * `cluster` - Cluster/environment identifier
    /// * `format` - Format hint (e.g., "protobuf", "json")
    /// * `data` - Raw bytes to ingest
    ///
    /// # Returns
    /// Number of events successfully sent, or an error
    pub async fn send(
        &self,
        source: &str,
        cluster: &str,
        format: &str,
        data: &[u8],
    ) -> Result<usize, PluginError> {
        let ctx = IngestContext {
            source,
            cluster,
            format,
        };

        // Transform raw bytes to Messages using the ingestor
        let messages = self.ingestors.ingest(&ctx, data)?;
        let count = messages.len();

        for msg in messages {
            self.tx
                .send(msg)
                .await
                .map_err(|e| PluginError::Send(e.to_string()))?;
        }

        Ok(count)
    }

    /// Try to send raw bytes without blocking
    pub fn try_send(
        &self,
        source: &str,
        cluster: &str,
        format: &str,
        data: &[u8],
    ) -> Result<usize, PluginError> {
        let ctx = IngestContext {
            source,
            cluster,
            format,
        };

        let messages = self.ingestors.ingest(&ctx, data)?;
        let count = messages.len();

        for msg in messages {
            self.tx
                .try_send(msg)
                .map_err(|e| PluginError::Send(e.to_string()))?;
        }

        Ok(count)
    }

    /// Check if an ingestor is registered for a source
    pub fn has_ingestor(&self, source: &str) -> bool {
        self.ingestors.has(source)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::emit::StdoutEmitter;
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

        let (_, sender, runner) = hub.build();

        // Sender should be cloneable
        let _sender2 = sender.clone();

        // Runner should have buffer
        assert_eq!(runner.buffer.capacity(), 10_000);
    }

    #[tokio::test]
    async fn test_message_sender() {
        let hub = Hub::new();
        let (_, sender, _runner) = hub.build();

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
            fn name(&self) -> &str {
                "counter"
            }

            async fn process(&self, msg: Message) -> Option<Message> {
                COUNTER.fetch_add(1, Ordering::Relaxed);
                Some(msg)
            }
        }

        let hub = Hub::new().middleware(CountingMiddleware);

        let (_, sender, runner) = hub.build();

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
            fn name(&self) -> &str {
                "counter"
            }
            async fn emit(&self, messages: &[Message]) -> Result<(), crate::error::PluginError> {
                self.0.fetch_add(messages.len() as u64, Ordering::SeqCst);
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

        let (_, sender, runner) = hub.build();

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
            fn name(&self) -> &str {
                "counter"
            }

            async fn emit(&self, messages: &[Message]) -> Result<(), crate::error::PluginError> {
                self.count
                    .fetch_add(messages.len() as u64, Ordering::SeqCst);
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

        let (_, sender, runner) = hub.build();

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
        fn name(&self) -> &str {
            self.name
        }
        async fn emit(&self, _: &[Message]) -> Result<(), crate::error::PluginError> {
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
        let messages: Vec<Message> = (0..5)
            .map(|i| Message::with_id(format!("msg-{i}"), 0, "test", "test", Bytes::new()))
            .collect();

        let batches = partition_by_destination_owned(messages, &emitters);

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
        let messages = vec![
            Message::with_id("msg-0", 0, "test", "test", Bytes::new())
                .with_routes(vec!["kafka".to_string()]),
            Message::with_id("msg-1", 0, "test", "test", Bytes::new())
                .with_routes(vec!["stdout".to_string()]),
            Message::with_id("msg-2", 0, "test", "test", Bytes::new())
                .with_routes(vec!["kafka".to_string(), "webhook".to_string()]),
            Message::with_id("msg-3", 0, "test", "test", Bytes::new()),
            // msg-3 has empty route_to = broadcast
        ];

        let batches = partition_by_destination_owned(messages, &emitters);

        // kafka: msg-0, msg-2, msg-3
        let kafka_batch = batches.get("kafka").unwrap();
        assert_eq!(kafka_batch.len(), 3);
        assert!(kafka_batch[0].id == "msg-0");
        assert!(kafka_batch[1].id == "msg-2");
        assert!(kafka_batch[2].id == "msg-3");

        // stdout: msg-1, msg-3
        let stdout_batch = batches.get("stdout").unwrap();
        assert_eq!(stdout_batch.len(), 2);
        assert!(stdout_batch[0].id == "msg-1");
        assert!(stdout_batch[1].id == "msg-3");

        // webhook: msg-2, msg-3
        let webhook_batch = batches.get("webhook").unwrap();
        assert_eq!(webhook_batch.len(), 2);
        assert!(webhook_batch[0].id == "msg-2");
        assert!(webhook_batch[1].id == "msg-3");
    }

    #[test]
    fn test_partition_no_matching_route() {
        let emitters: Vec<Arc<dyn crate::emit::Emitter>> =
            vec![Arc::new(NamedEmitter { name: "kafka" })];

        // Message routed to non-existent emitter
        let messages = vec![
            Message::with_id("msg-0", 0, "test", "test", Bytes::new())
                .with_routes(vec!["nonexistent".to_string()]),
        ];

        let batches = partition_by_destination_owned(messages, &emitters);

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

        // Create messages with large payloads to make clone cost visible
        let messages: Vec<Message> = (0..3)
            .map(|i| {
                let routes = if i % 2 == 0 {
                    vec!["kafka".to_string()]
                } else {
                    vec!["stdout".to_string()]
                };
                Message::with_id(
                    format!("msg-{i}"),
                    0,
                    "test",
                    "test",
                    Bytes::from(vec![0u8; 10_000]),
                )
                .with_routes(routes)
            })
            .collect();

        let batches = partition_by_destination_owned(messages, &emitters);

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

        // One message going to all 3 destinations
        let messages = vec![Message::with_id(
            "broadcast-msg",
            0,
            "test",
            "test",
            Bytes::from(vec![42u8; 100]),
        )];

        let batches = partition_by_destination_owned(messages, &emitters);

        // Each emitter should have the message
        for name in ["kafka", "stdout", "webhook"] {
            let batch = batches.get(name).unwrap();
            assert_eq!(batch.len(), 1);
            assert!(batch[0].id == "broadcast-msg");
            assert_eq!(&batch[0].payload[..], &vec![42u8; 100][..]);
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
            fn name(&self) -> &str {
                "counter"
            }
            async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
                self.count
                    .fetch_add(messages.len(), std::sync::atomic::Ordering::SeqCst);
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

        let (_, sender, runner) = hub.build();

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
            fn name(&self) -> &str {
                "failing"
            }
            async fn emit(&self, _messages: &[Message]) -> Result<(), PluginError> {
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

        let (_, sender, runner) = hub.build();

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
            fn name(&self) -> &str {
                "timing"
            }

            async fn emit(&self, messages: &[Message]) -> Result<(), crate::error::PluginError> {
                self.count
                    .fetch_add(messages.len() as u64, Ordering::SeqCst);
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

        let (_, sender, runner) = hub.build();
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

        let (_, _sender, runner) = hub.build();

        // Should have standard strategy
        assert_eq!(runner.buffer.strategy_name(), "standard");
        assert_eq!(runner.buffer.capacity(), 5000);
    }

    #[test]
    fn test_buffer_strategy_tiered() {
        let hub = Hub::new().buffer_strategy(BufferStrategy::tiered(3000, 2000));

        let (_, _sender, runner) = hub.build();

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
            fn name(&self) -> &str {
                "counter"
            }

            async fn emit(&self, messages: &[Message]) -> Result<(), crate::error::PluginError> {
                self.count
                    .fetch_add(messages.len() as u64, Ordering::SeqCst);
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

        let (_, sender, runner) = hub.build();
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

        let (_, _sender, runner) = hub.build();

        assert_eq!(runner.buffer.strategy_name(), "standard");
        assert_eq!(runner.buffer.capacity(), 7500);
    }

    // ========================================================================
    // Ingestor integration tests
    // ========================================================================

    #[test]
    fn test_hub_ingestor_builder() {
        use crate::ingest::{JsonIngestor, PassthroughIngestor};

        let hub = Hub::new()
            .ingestor(JsonIngestor::new())
            .ingestor(PassthroughIngestor::new())
            .emitter(StdoutEmitter::new());

        // Build and verify we get a RawSender
        let (raw_sender, _, _) = hub.build();

        // RawSender should have the ingestors registered
        assert!(raw_sender.has_ingestor("json"));
        assert!(raw_sender.has_ingestor("json-lines"));
        assert!(raw_sender.has_ingestor("ndjson"));
        assert!(raw_sender.has_ingestor("passthrough"));
        assert!(raw_sender.has_ingestor("polku"));
        assert!(!raw_sender.has_ingestor("unknown"));
    }

    #[tokio::test]
    async fn test_raw_sender_json_ingestion() {
        use crate::ingest::JsonIngestor;
        use std::sync::atomic::AtomicU64;

        // Emitter that captures events
        struct CaptureEmitter {
            count: AtomicU64,
        }

        #[async_trait::async_trait]
        impl crate::emit::Emitter for CaptureEmitter {
            fn name(&self) -> &str {
                "capture"
            }
            async fn emit(&self, messages: &[Message]) -> Result<(), crate::error::PluginError> {
                self.count
                    .fetch_add(messages.len() as u64, Ordering::SeqCst);
                Ok(())
            }
            async fn health(&self) -> bool {
                true
            }
        }

        let emitter = Arc::new(CaptureEmitter {
            count: AtomicU64::new(0),
        });

        let hub = Hub::new()
            .ingestor(JsonIngestor::new())
            .batch_size(10)
            .flush_interval_ms(1)
            .emitter_arc(emitter.clone());

        let (raw_sender, _, runner) = hub.build();

        // Spawn runner
        let handle = tokio::spawn(async move { runner.run().await });

        // Send raw JSON via RawSender
        let json = r#"{"id": "evt-1", "source": "test-svc", "event_type": "user.created"}"#;
        let count = raw_sender
            .send("json", "prod", "json", json.as_bytes())
            .await
            .expect("send should work");

        assert_eq!(count, 1, "Should ingest 1 event from JSON");

        // Send JSON array (multiple events)
        let json_array = r#"[
            {"id": "evt-2", "source": "svc", "event_type": "a"},
            {"id": "evt-3", "source": "svc", "event_type": "b"}
        ]"#;
        let count = raw_sender
            .send("json", "prod", "json", json_array.as_bytes())
            .await
            .expect("send should work");

        assert_eq!(count, 2, "Should ingest 2 events from JSON array");

        // Wait for flush
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Shutdown
        drop(raw_sender);
        let _ = handle.await;

        // All 3 events should have been emitted
        assert_eq!(emitter.count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_raw_sender_unknown_source_error() {
        // Hub without any ingestors
        let hub = Hub::new().emitter(StdoutEmitter::new());

        let (raw_sender, _, _runner) = hub.build();

        // Sending to unknown source should fail
        let result = raw_sender.send("unknown", "prod", "json", b"data").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_raw_sender_with_default_ingestor() {
        use crate::ingest::JsonIngestor;

        let hub = Hub::new()
            .default_ingestor(JsonIngestor::new())
            .emitter(StdoutEmitter::new());

        let (raw_sender, _, _runner) = hub.build();

        // Unknown source should use default ingestor
        let json = r#"{"id": "evt-1", "source": "svc", "event_type": "test"}"#;
        let result = raw_sender
            .send("any-source", "prod", "json", json.as_bytes())
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }
}
