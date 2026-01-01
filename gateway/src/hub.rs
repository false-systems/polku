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

use crate::buffer::RingBuffer;
use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use crate::middleware::{Middleware, MiddlewareChain};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// The Hub - central message pipeline
///
/// Connects inputs → middleware → buffer → outputs.
///
/// # Architecture
///
/// ```text
/// Input Channels ──► MiddlewareChain ──► RingBuffer ──► Outputs (fan-out)
/// ```
pub struct Hub {
    /// Buffer capacity
    buffer_capacity: usize,
    /// Middleware chain (applied before buffering)
    middleware: MiddlewareChain,
    /// Registered emitters
    emitters: Vec<Arc<dyn Emitter>>,
}

impl Hub {
    /// Create a new Hub with default settings
    pub fn new() -> Self {
        Self {
            buffer_capacity: 10_000,
            middleware: MiddlewareChain::new(),
            emitters: Vec::new(),
        }
    }

    /// Set the buffer capacity
    ///
    /// Default is 10,000 messages.
    pub fn buffer_capacity(mut self, capacity: usize) -> Self {
        self.buffer_capacity = capacity;
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

    /// Build a message sender for this hub
    ///
    /// Returns a sender that can be used to inject messages into the pipeline.
    /// This is useful for custom inputs or testing.
    pub fn build(self) -> (MessageSender, HubRunner) {
        let (tx, rx) = mpsc::channel(1024);

        let sender = MessageSender { tx };

        let runner = HubRunner {
            rx,
            buffer: Arc::new(RingBuffer::new(self.buffer_capacity)),
            middleware: self.middleware,
            emitters: self.emitters,
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

/// Hub runner - processes messages through the pipeline
pub struct HubRunner {
    rx: mpsc::Receiver<Message>,
    buffer: Arc<RingBuffer>,
    middleware: MiddlewareChain,
    emitters: Vec<Arc<dyn Emitter>>,
}

impl HubRunner {
    /// Run the hub, processing messages until the channel closes
    ///
    /// This will:
    /// 1. Receive messages from the input channel
    /// 2. Apply middleware chain
    /// 3. Buffer messages
    /// 4. Periodically flush to outputs
    /// 5. On shutdown, drain remaining buffer before exiting
    pub async fn run(mut self) -> Result<(), PluginError> {
        info!(
            emitters = self.emitters.len(),
            middleware = self.middleware.len(),
            buffer_capacity = self.buffer.capacity(),
            "Hub started"
        );

        if self.emitters.is_empty() {
            warn!("No emitters registered - messages will be buffered but not delivered");
        }

        // Shutdown signal: when true, flush loop will drain and exit
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Spawn emitter flusher
        let buffer = Arc::clone(&self.buffer);
        let emitters = self.emitters.clone();
        let flush_handle = tokio::spawn(async move {
            flush_loop(buffer, emitters, shutdown_rx).await;
        });

        // Process incoming messages
        while let Some(msg) = self.rx.recv().await {
            // Apply middleware
            let processed = self.middleware.process(msg).await;

            if let Some(msg) = processed {
                debug!(id = %msg.id, "Message buffered");
                let dropped = self.buffer.push(vec![msg]);
                if dropped > 0 {
                    warn!(dropped = dropped, "Buffer overflow, messages dropped");
                }
            }
        }

        // Channel closed - signal flush loop to drain and stop
        info!("Hub shutting down, draining buffer...");
        let _ = shutdown_tx.send(true);

        // Wait for flush loop to complete (it will drain the buffer first)
        if let Err(e) = flush_handle.await {
            warn!(error = %e, "Flush task failed during shutdown");
        }

        info!(remaining = self.buffer.len(), "Hub shutdown complete");

        Ok(())
    }

    /// Get a reference to the buffer for monitoring
    pub fn buffer(&self) -> &Arc<RingBuffer> {
        &self.buffer
    }
}

/// Background flush loop - sends buffered messages to emitters
///
/// When shutdown is signaled, drains the buffer completely before exiting.
async fn flush_loop(
    buffer: Arc<RingBuffer>,
    emitters: Vec<Arc<dyn Emitter>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    const BATCH_SIZE: usize = 100;
    const FLUSH_INTERVAL_MS: u64 = 10;

    loop {
        // Check if shutdown requested and buffer is empty
        if *shutdown_rx.borrow() && buffer.is_empty() {
            debug!("Flush loop: shutdown complete, buffer drained");
            break;
        }

        // Either wait for interval OR shutdown signal
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(FLUSH_INTERVAL_MS)) => {}
            _ = shutdown_rx.changed() => {
                // Shutdown signaled - continue loop to drain buffer
                debug!("Flush loop: shutdown signaled, draining buffer");
            }
        }

        let messages = buffer.drain(BATCH_SIZE);
        if messages.is_empty() {
            continue;
        }

        // Convert Messages to proto Events for outputs
        let events: Vec<crate::proto::Event> = messages
            .into_iter()
            .map(crate::proto::Event::from)
            .collect();

        // Send to all emitters (fan-out)
        for emitter in &emitters {
            // Check routing
            let routed_events: Vec<_> = events
                .iter()
                .filter(|e| e.route_to.is_empty() || e.route_to.iter().any(|r| r == emitter.name()))
                .cloned()
                .collect();

            if routed_events.is_empty() {
                continue;
            }

            if let Err(e) = emitter.emit(&routed_events).await {
                error!(
                    emitter = emitter.name(),
                    error = %e,
                    count = routed_events.len(),
                    "Failed to emit"
                );
            } else {
                debug!(
                    emitter = emitter.name(),
                    count = routed_events.len(),
                    "Emitted"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::emit::StdoutEmitter;
    use crate::middleware::{Filter, Transform};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_hub_builder() {
        let hub = Hub::new()
            .buffer_capacity(1000)
            .middleware(Transform::new(|msg| msg))
            .emitter(StdoutEmitter::new());

        assert_eq!(hub.buffer_capacity, 1000);
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
            async fn emit(
                &self,
                events: &[crate::proto::Event],
            ) -> Result<(), crate::error::PluginError> {
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

            async fn emit(
                &self,
                events: &[crate::proto::Event],
            ) -> Result<(), crate::error::PluginError> {
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
}
