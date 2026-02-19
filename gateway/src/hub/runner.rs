//! Hub runner - processes messages through the pipeline
//!
//! Contains the runtime components that execute the Hub's message processing loop.

use super::buffer::HubBuffer;
use crate::checkpoint::CheckpointStore;
use crate::emit::Emitter;
use crate::error::PluginError;
use crate::manifest::PipelineManifest;
use crate::message::Message;
use crate::metrics::Metrics;
use crate::middleware::MiddlewareChain;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Hub runner - processes messages through the pipeline
pub struct HubRunner {
    pub(crate) rx: mpsc::Receiver<Message>,
    pub(crate) buffer: Arc<dyn HubBuffer>,
    pub(crate) batch_size: usize,
    pub(crate) flush_interval_ms: u64,
    pub(crate) middleware: MiddlewareChain,
    pub(crate) emitters: Vec<Arc<dyn Emitter>>,
    /// Optional checkpoint store for tracking delivery progress
    pub(crate) checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    /// Monotonically increasing sequence number for checkpoint tracking
    pub(crate) sequence: Arc<AtomicU64>,
    /// Self-describing pipeline topology (generated at build time)
    pub(crate) manifest: Arc<PipelineManifest>,
}

impl HubRunner {
    /// Run the hub, processing messages until the channel closes
    ///
    /// This will:
    /// 1. Receive messages from the input channel
    /// 2. Apply middleware chain
    /// 3. Buffer messages
    /// 4. Flush immediately when buffer hits batch_size (inline flush)
    /// 5. Timer-based flush for partial batches (latency bound)
    /// 6. On shutdown, drain remaining buffer before exiting
    pub async fn run(mut self) -> Result<(), PluginError> {
        info!(
            emitters = self.emitters.len(),
            middleware = self.middleware.len(),
            buffer_capacity = self.buffer.capacity(),
            buffer_strategy = self.buffer.strategy_name(),
            "Hub started"
        );

        // Initialize Prometheus metrics (ignore error if already initialized)
        if let Ok(metrics) = Metrics::init() {
            metrics.set_buffer_capacity(self.buffer.capacity());
            // Initialize emitter health to healthy (1.0) so unpolled emitters
            // don't show as unhealthy in pipeline pressure calculations
            for emitter in &self.emitters {
                metrics.set_emitter_health(emitter.name(), true);
            }
        }

        if self.emitters.is_empty() {
            warn!("No emitters registered - messages will be buffered but not delivered");
        }

        // Shutdown signal: when true, flush loop will drain and exit
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Spawn timer-based flusher for partial batches (latency bound)
        let buffer = Arc::clone(&self.buffer);
        let emitters_for_timer = self.emitters.clone();
        let batch_size = self.batch_size;
        let flush_interval_ms = self.flush_interval_ms;
        let checkpoint_store = self.checkpoint_store.clone();
        let sequence = Arc::clone(&self.sequence);
        let flush_handle = tokio::spawn(async move {
            flush_loop(
                buffer,
                emitters_for_timer,
                shutdown_rx,
                batch_size,
                flush_interval_ms,
                checkpoint_store,
                sequence,
            )
            .await;
        });

        // Process incoming messages
        while let Some(msg) = self.rx.recv().await {
            // Record received message
            if let Some(metrics) = Metrics::get() {
                metrics.record_received(&msg.source, &msg.message_type, 1);
            }

            // Apply middleware
            let processed = self.middleware.process(msg).await;

            if let Some(msg) = processed {
                debug!(id = %msg.id, "Message buffered");
                if !self.buffer.push(msg) {
                    warn!("Buffer overflow, message dropped");
                    // Record dropped due to buffer overflow
                    if let Some(metrics) = Metrics::get() {
                        metrics.record_dropped("buffer_overflow", 1);
                    }
                }

                // INLINE FLUSH: When buffer hits threshold, flush immediately
                // This avoids waiting for the timer when we have a full batch
                if self.buffer.len() >= self.batch_size {
                    self.flush_batch().await;
                }
            } else {
                // Message was filtered by middleware
                if let Some(metrics) = Metrics::get() {
                    metrics.record_dropped("middleware_filtered", 1);
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

    /// Flush a batch of messages to emitters
    ///
    /// Called inline when buffer hits threshold, and by the timer loop.
    async fn flush_batch(&self) {
        let messages = self.buffer.drain(self.batch_size);

        if let Some(metrics) = Metrics::get() {
            metrics.set_buffer_size(self.buffer.len());
        }

        if messages.is_empty() {
            return;
        }

        emit_to_destinations(
            messages,
            &self.emitters,
            &self.sequence,
            &self.checkpoint_store,
        )
        .await;
    }

    /// Get a reference to the buffer for monitoring
    pub fn buffer(&self) -> &Arc<dyn HubBuffer> {
        &self.buffer
    }

    /// Get the pipeline manifest (self-describing topology)
    pub fn manifest(&self) -> &PipelineManifest {
        &self.manifest
    }

    /// Get a shareable reference to the manifest
    pub fn manifest_arc(&self) -> Arc<PipelineManifest> {
        Arc::clone(&self.manifest)
    }
}

/// Emit a batch of messages to their destination emitters
///
/// Partitions messages by destination, dispatches to each emitter,
/// records metrics (per-emitter timing, forwarded counts, throughput),
/// and updates checkpoints.
async fn emit_to_destinations(
    messages: Vec<Message>,
    emitters: &[Arc<dyn Emitter>],
    sequence: &AtomicU64,
    checkpoint_store: &Option<Arc<dyn CheckpointStore>>,
) {
    let flush_start = std::time::Instant::now();
    let total_events = messages.len();

    let batch_start_seq = sequence.fetch_add(total_events as u64, Ordering::SeqCst);

    let PartitionResult {
        batches,
        unroutable,
    } = partition_by_destination_owned(messages, emitters);

    #[allow(clippy::collapsible_if)]
    if unroutable > 0 {
        if let Some(m) = Metrics::get() {
            m.record_dropped("no_matching_emitter", unroutable);
        }
    }

    let mut forward_counts: std::collections::HashMap<(&str, &str), u64> =
        std::collections::HashMap::new();

    for emitter in emitters {
        let routed_events = match batches.get(emitter.name()) {
            Some(events) if !events.is_empty() => events,
            _ => continue,
        };

        let emitter_start = std::time::Instant::now();
        let emit_result = emitter.emit(routed_events).await;
        let emitter_duration = emitter_start.elapsed();

        if let Some(metrics) = Metrics::get() {
            metrics.record_emitter_flush(
                emitter.name(),
                routed_events.len(),
                emitter_duration,
                emit_result.is_ok(),
            );
        }

        if let Err(e) = emit_result {
            error!(
                emitter = emitter.name(),
                error = %e,
                count = routed_events.len(),
                "Failed to emit"
            );
            if let Some(metrics) = Metrics::get() {
                metrics.record_dropped(emitter.name(), routed_events.len() as u64);
            }
        } else {
            debug!(
                emitter = emitter.name(),
                count = routed_events.len(),
                "Emitted"
            );
            for msg in routed_events {
                *forward_counts
                    .entry((emitter.name(), &msg.message_type))
                    .or_default() += 1;
            }
            if let Some(store) = checkpoint_store {
                let emitter_end_seq = batch_start_seq + routed_events.len() as u64 - 1;
                store.set(emitter.name(), emitter_end_seq);
            }
        }
    }

    if let Some(metrics) = Metrics::get() {
        metrics.record_forwarded_batch(&forward_counts);
        metrics.inc_flush();

        let flush_duration = flush_start.elapsed();
        if flush_duration.as_secs_f64() > 0.0 {
            let events_per_sec = total_events as f64 / flush_duration.as_secs_f64();
            metrics.set_events_per_second(events_per_sec);
        }

        update_pressure(metrics, emitters);
    }
}

/// Update the composite pipeline pressure metric from current state
fn update_pressure(metrics: &Metrics, emitters: &[Arc<dyn Emitter>]) {
    let buf_cap = metrics.buffer_capacity.get();
    let buffer_fill = if buf_cap > 0.0 {
        metrics.buffer_size.get() / buf_cap
    } else {
        0.0
    };
    let emitter_count = emitters.len();
    let emit_failure_rate = if emitter_count > 0 {
        let unhealthy: f64 = emitters
            .iter()
            .map(|e| {
                if metrics.emitter_health.with_label_values(&[e.name()]).get() < 1.0 {
                    1.0
                } else {
                    0.0
                }
            })
            .sum();
        unhealthy / emitter_count as f64
    } else {
        0.0
    };
    metrics.update_pipeline_pressure(buffer_fill, emit_failure_rate, 0.0);
}

/// Result of partitioning messages by destination
pub(crate) struct PartitionResult<'a> {
    /// Messages grouped by emitter name
    pub batches: std::collections::HashMap<&'a str, Vec<Message>>,
    /// Number of messages that matched no emitter (dropped)
    pub unroutable: u64,
}

/// Partition messages by destination, returning ready-to-emit batches
///
/// For messages that go to only ONE emitter, moves the message (no clone).
/// For messages that go to MULTIPLE emitters, clones N-1 times (last one is moved).
///
/// # Performance
///
/// - Single pass to count destinations per message: O(messages × emitters)
/// - Second pass to distribute: O(messages × avg_destinations)
/// - Multi-destination messages: N-1 clones instead of N (last destination gets moved value)
pub(crate) fn partition_by_destination_owned<'a>(
    messages: Vec<Message>,
    emitters: &'a [Arc<dyn Emitter>],
) -> PartitionResult<'a> {
    if emitters.is_empty() || messages.is_empty() {
        return PartitionResult {
            batches: std::collections::HashMap::new(),
            unroutable: 0,
        };
    }

    let mut batches: std::collections::HashMap<&'a str, Vec<Message>> =
        std::collections::HashMap::new();

    // Pre-allocate for each emitter
    for emitter in emitters {
        batches.insert(
            emitter.name(),
            Vec::with_capacity(messages.len() / emitters.len()),
        );
    }

    // First pass: count destinations for each message
    let mut dest_counts: Vec<usize> = vec![0; messages.len()];
    let mut destinations: Vec<Vec<&'a str>> =
        vec![Vec::with_capacity(emitters.len()); messages.len()];

    for (idx, msg) in messages.iter().enumerate() {
        let broadcast = msg.route_to.is_empty();

        for emitter in emitters {
            if broadcast || msg.route_to.iter().any(|r| r == emitter.name()) {
                dest_counts[idx] += 1;
                destinations[idx].push(emitter.name());
            }
        }
    }

    // Second pass: distribute messages
    // - Single destination: move (no clone)
    // - Multiple destinations: clone N-1 times, move on final access
    let mut unroutable = 0u64;
    for (idx, msg) in messages.into_iter().enumerate() {
        let count = dest_counts[idx];
        let dests = &destinations[idx];

        match count {
            0 => {
                tracing::debug!(
                    id = %msg.id,
                    route_to = ?msg.route_to,
                    "message matched no emitters, dropping"
                );
                unroutable += 1;
            }
            1 => {
                // Single destination: move without cloning
                if let Some(batch) = batches.get_mut(dests[0]) {
                    batch.push(msg);
                }
            }
            _ => {
                // Multiple destinations: clone for first N-1, move for last
                // This saves one clone compared to cloning for all N destinations
                for (i, dest) in dests.iter().enumerate() {
                    if let Some(batch) = batches.get_mut(*dest) {
                        if i == dests.len() - 1 {
                            // Last destination: move the original
                            batch.push(msg);
                            break;
                        } else {
                            // Not last: clone
                            batch.push(msg.clone());
                        }
                    }
                }
            }
        }
    }

    PartitionResult {
        batches,
        unroutable,
    }
}

/// Background flush loop - sends buffered messages to emitters
///
/// When shutdown is signaled, drains the buffer completely before exiting.
pub(crate) async fn flush_loop(
    buffer: Arc<dyn HubBuffer>,
    emitters: Vec<Arc<dyn Emitter>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    batch_size: usize,
    flush_interval_ms: u64,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    sequence: Arc<AtomicU64>,
) {
    loop {
        // Check if shutdown requested and buffer is empty
        if *shutdown_rx.borrow() && buffer.is_empty() {
            debug!("Flush loop: shutdown complete, buffer drained");
            break;
        }

        // Either wait for interval OR shutdown signal
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(flush_interval_ms)) => {}
            _ = shutdown_rx.changed() => {
                // Shutdown signaled - continue loop to drain buffer
                debug!("Flush loop: shutdown signaled, draining buffer");
            }
        }

        let messages = buffer.drain(batch_size);

        if let Some(metrics) = Metrics::get() {
            metrics.set_buffer_size(buffer.len());
        }

        if messages.is_empty() {
            continue;
        }

        emit_to_destinations(messages, &emitters, &sequence, &checkpoint_store).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::buffer_lockfree::LockFreeBuffer;
    use crate::checkpoint::MemoryCheckpointStore;
    use crate::manifest::{BufferDesc, PipelineManifest, TuningDesc};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    // ========================================================================
    // Test helpers
    // ========================================================================

    /// Simple emitter that counts messages and optionally fails
    struct TestEmitter {
        name: &'static str,
        count: AtomicU64,
        should_fail: AtomicBool,
    }

    impl TestEmitter {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                count: AtomicU64::new(0),
                should_fail: AtomicBool::new(false),
            }
        }

        fn count(&self) -> u64 {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Emitter for TestEmitter {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(PluginError::Send("intentional failure".into()));
            }
            self.count
                .fetch_add(messages.len() as u64, Ordering::SeqCst);
            Ok(())
        }
        async fn health(&self) -> bool {
            !self.should_fail.load(Ordering::SeqCst)
        }
    }

    fn make_msg(msg_type: &str) -> Message {
        Message::new("test-src", msg_type, Bytes::from("payload"))
    }

    fn make_manifest() -> Arc<PipelineManifest> {
        Arc::new(PipelineManifest {
            version: "1".to_string(),
            middleware: vec![],
            emitters: vec![],
            buffer: BufferDesc {
                strategy: "standard".to_string(),
                capacity: 1000,
            },
            tuning: TuningDesc {
                batch_size: 10,
                flush_interval_ms: 100,
                channel_capacity: 64,
            },
        })
    }

    fn make_runner(
        rx: mpsc::Receiver<Message>,
        buffer: Arc<dyn HubBuffer>,
        emitters: Vec<Arc<dyn Emitter>>,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> HubRunner {
        HubRunner {
            rx,
            buffer,
            batch_size,
            flush_interval_ms,
            middleware: MiddlewareChain::new(),
            emitters,
            checkpoint_store: None,
            sequence: Arc::new(AtomicU64::new(0)),
            manifest: make_manifest(),
        }
    }

    // ========================================================================
    // flush_loop tests (DST: time is paused, no real sleeps)
    // ========================================================================

    #[tokio::test(start_paused = true)]
    async fn flush_loop_drains_on_timer() {
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("test"));

        // Pre-fill buffer with 3 messages (below batch_size)
        for i in 0..3 {
            buffer.push(make_msg(&format!("evt-{i}")));
        }
        assert_eq!(buffer.len(), 3);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let buf_clone = Arc::clone(&buffer);
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let handle = tokio::spawn(flush_loop(
            buf_clone,
            emitters,
            shutdown_rx,
            10, // batch_size
            50, // flush_interval_ms
            None,
            Arc::new(AtomicU64::new(0)),
        ));

        // Yield first so the spawned task reaches the select! with sleep
        tokio::task::yield_now().await;

        // Now advance time past the flush interval
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;

        // Yield multiple times to let flush processing complete
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            emitter.count(),
            3,
            "Timer should have flushed all 3 messages"
        );
        assert_eq!(buffer.len(), 0);

        // Shutdown
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn flush_loop_drains_all_on_shutdown() {
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("drain"));

        // Fill buffer with 25 messages, batch_size=10
        // Shutdown should drain all of them (multiple batches)
        for i in 0..25 {
            buffer.push(make_msg(&format!("evt-{i}")));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let buf_clone = Arc::clone(&buffer);
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let handle = tokio::spawn(flush_loop(
            buf_clone,
            emitters,
            shutdown_rx,
            10,   // batch_size
            5000, // long interval - we don't want timer flushes
            None,
            Arc::new(AtomicU64::new(0)),
        ));

        // Signal shutdown immediately
        let _ = shutdown_tx.send(true);

        // Advance past the flush interval to let the loop iterate
        tokio::time::advance(tokio::time::Duration::from_millis(5100)).await;

        // Multiple yields to let 3 batches (25 msgs / batch_size 10) drain
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        handle.await.unwrap();

        assert_eq!(
            emitter.count(),
            25,
            "Shutdown should drain all buffered messages"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn flush_loop_checkpoint_advances() {
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("ckpt"));
        let store = Arc::new(MemoryCheckpointStore::new());

        for i in 0..5 {
            buffer.push(make_msg(&format!("evt-{i}")));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let buf_clone = Arc::clone(&buffer);
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];
        let seq = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(flush_loop(
            buf_clone,
            emitters,
            shutdown_rx,
            10,
            50,
            Some(store.clone()),
            seq,
        ));

        // Yield so the task reaches the select! with sleep
        tokio::task::yield_now().await;

        // Advance past flush interval
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;

        // Yield enough for flush processing to complete
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Checkpoint should be seq 0..4 → checkpoint = 4
        assert_eq!(store.get("ckpt"), Some(4));

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn flush_loop_no_checkpoint_on_failure() {
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("fail"));
        emitter.should_fail.store(true, Ordering::SeqCst);
        let store = Arc::new(MemoryCheckpointStore::new());

        for i in 0..5 {
            buffer.push(make_msg(&format!("evt-{i}")));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let buf_clone = Arc::clone(&buffer);
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let handle = tokio::spawn(flush_loop(
            buf_clone,
            emitters,
            shutdown_rx,
            10,
            50,
            Some(store.clone()),
            Arc::new(AtomicU64::new(0)),
        ));

        // Let flush_loop reach select! before advancing time
        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Checkpoint should NOT advance on failure
        assert_eq!(store.get("fail"), None);

        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    // ========================================================================
    // HubRunner::run tests (DST)
    // ========================================================================

    #[tokio::test(start_paused = true)]
    async fn run_processes_messages_end_to_end() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("e2e"));
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let runner = make_runner(rx, buffer, emitters, 5, 50);

        let handle = tokio::spawn(runner.run());

        // Send 5 messages (= batch_size, triggers inline flush)
        for i in 0..5 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        // Yield to let the runner process + inline flush
        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        assert_eq!(
            emitter.count(),
            5,
            "Inline flush should emit batch_size messages"
        );

        // Drop sender to trigger graceful shutdown
        drop(tx);
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn run_partial_batch_flushed_by_timer() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("timer"));
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let runner = make_runner(rx, buffer, emitters, 100, 50); // batch_size=100

        let handle = tokio::spawn(runner.run());

        // Send only 3 messages (well below batch_size of 100)
        for i in 0..3 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        // Yield so runner receives messages
        tokio::task::yield_now().await;

        // Advance past flush interval to trigger timer-based flush
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        assert_eq!(emitter.count(), 3, "Timer flush should emit partial batch");

        drop(tx);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn run_middleware_filters_messages() {
        use crate::middleware::Filter;

        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("filtered"));
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let mut middleware = MiddlewareChain::new();
        middleware.add(Filter::new(|msg: &Message| msg.message_type == "keep"));

        let runner = HubRunner {
            rx,
            buffer,
            batch_size: 10,
            flush_interval_ms: 50,
            middleware,
            emitters,
            checkpoint_store: None,
            sequence: Arc::new(AtomicU64::new(0)),
            manifest: make_manifest(),
        };

        let handle = tokio::spawn(runner.run());

        // Send 2 "keep" and 1 "drop"
        tx.send(make_msg("keep")).await.unwrap();
        tx.send(make_msg("drop")).await.unwrap();
        tx.send(make_msg("keep")).await.unwrap();

        // Let the runner process
        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        drop(tx);
        handle.await.unwrap().unwrap();

        assert_eq!(emitter.count(), 2, "Only 'keep' messages should be emitted");
    }

    #[tokio::test(start_paused = true)]
    async fn run_buffer_overflow_drops_messages() {
        let (tx, rx) = mpsc::channel(64);
        // Tiny buffer: capacity 3
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(3));
        let emitter = Arc::new(TestEmitter::new("overflow"));
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        // Large batch_size so inline flush doesn't trigger
        let runner = make_runner(rx, buffer, emitters, 1000, 5000);

        let handle = tokio::spawn(runner.run());

        // Send 10 messages into buffer of capacity 3
        // Some will be dropped due to overflow
        for i in 0..10 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        // Let the runner process all 10
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        drop(tx);

        // Let runner observe channel closure before advancing time
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Advance time so the flush loop drains what's left
        tokio::time::advance(tokio::time::Duration::from_millis(5100)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        handle.await.unwrap().unwrap();

        // Only 3 messages should survive (buffer capacity)
        assert!(
            emitter.count() <= 3,
            "Buffer overflow should drop messages, got {}",
            emitter.count()
        );
        assert!(
            emitter.count() >= 1,
            "At least some messages should get through"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_no_emitters_completes_gracefully() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));

        // No emitters
        let runner = make_runner(rx, buffer, vec![], 10, 50);

        let handle = tokio::spawn(runner.run());

        // Send messages
        for i in 0..5 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        drop(tx);

        // Let runner observe channel closure
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Should complete without error even with no emitters"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_multiple_emitters_fan_out() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter_a = Arc::new(TestEmitter::new("alpha"));
        let emitter_b = Arc::new(TestEmitter::new("beta"));
        let emitters: Vec<Arc<dyn Emitter>> = vec![emitter_a.clone(), emitter_b.clone()];

        let runner = make_runner(rx, buffer, emitters, 10, 50);
        let handle = tokio::spawn(runner.run());

        // Broadcast messages (empty route_to → all emitters)
        for i in 0..4 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        drop(tx);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        handle.await.unwrap().unwrap();

        assert_eq!(emitter_a.count(), 4, "Emitter A should get all 4 messages");
        assert_eq!(emitter_b.count(), 4, "Emitter B should get all 4 messages");
    }

    #[tokio::test(start_paused = true)]
    async fn run_routed_messages_reach_correct_emitter() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter_a = Arc::new(TestEmitter::new("alpha"));
        let emitter_b = Arc::new(TestEmitter::new("beta"));
        let emitters: Vec<Arc<dyn Emitter>> = vec![emitter_a.clone(), emitter_b.clone()];

        let runner = make_runner(rx, buffer, emitters, 10, 50);
        let handle = tokio::spawn(runner.run());

        // Route 2 messages to "alpha", 1 to "beta"
        let mut msg1 = make_msg("to-alpha");
        msg1.route_to = vec!["alpha".to_string()].into();
        tx.send(msg1).await.unwrap();

        let mut msg2 = make_msg("to-alpha");
        msg2.route_to = vec!["alpha".to_string()].into();
        tx.send(msg2).await.unwrap();

        let mut msg3 = make_msg("to-beta");
        msg3.route_to = vec!["beta".to_string()].into();
        tx.send(msg3).await.unwrap();

        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        drop(tx);
        handle.await.unwrap().unwrap();

        assert_eq!(emitter_a.count(), 2);
        assert_eq!(emitter_b.count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn run_emitter_failure_does_not_crash() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let emitter = Arc::new(TestEmitter::new("broken"));
        emitter.should_fail.store(true, Ordering::SeqCst);
        let emitters = vec![emitter.clone() as Arc<dyn Emitter>];

        let runner = make_runner(rx, buffer, emitters, 5, 50);
        let handle = tokio::spawn(runner.run());

        for i in 0..5 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        drop(tx);
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Emitter failure should not crash the runner"
        );
        assert_eq!(
            emitter.count(),
            0,
            "Failing emitter should receive 0 messages"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_checkpoint_tracks_per_emitter() {
        let (tx, rx) = mpsc::channel(64);
        let buffer: Arc<dyn HubBuffer> = Arc::new(LockFreeBuffer::new(100));
        let store = Arc::new(MemoryCheckpointStore::new());

        let emitter_ok = Arc::new(TestEmitter::new("ok-emitter"));
        let emitter_fail = Arc::new(TestEmitter::new("fail-emitter"));
        emitter_fail.should_fail.store(true, Ordering::SeqCst);

        let emitters: Vec<Arc<dyn Emitter>> = vec![emitter_ok.clone(), emitter_fail.clone()];

        let runner = HubRunner {
            rx,
            buffer,
            batch_size: 10,
            flush_interval_ms: 50,
            middleware: MiddlewareChain::new(),
            emitters,
            checkpoint_store: Some(store.clone()),
            sequence: Arc::new(AtomicU64::new(0)),
            manifest: make_manifest(),
        };

        let handle = tokio::spawn(runner.run());

        // Send 5 broadcast messages
        for i in 0..5 {
            tx.send(make_msg(&format!("evt-{i}"))).await.unwrap();
        }

        tokio::task::yield_now().await;
        tokio::time::advance(tokio::time::Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        drop(tx);
        handle.await.unwrap().unwrap();

        // ok-emitter should have a checkpoint
        assert!(
            store.get("ok-emitter").is_some(),
            "Successful emitter should have checkpoint"
        );

        // fail-emitter should NOT have a checkpoint
        assert_eq!(
            store.get("fail-emitter"),
            None,
            "Failed emitter should not have checkpoint"
        );
    }

    // ========================================================================
    // partition_by_destination_owned edge cases
    // ========================================================================

    #[test]
    fn partition_empty_messages_returns_empty() {
        let emitters: Vec<Arc<dyn Emitter>> = vec![Arc::new(TestEmitter::new("e"))];
        let PartitionResult { batches, .. } = partition_by_destination_owned(vec![], &emitters);
        assert!(batches.is_empty(), "Empty messages → empty result");
    }

    #[test]
    fn partition_empty_emitters_returns_empty() {
        let PartitionResult { batches, .. } =
            partition_by_destination_owned(vec![make_msg("t")], &[]);
        assert!(batches.is_empty(), "No emitters → empty result");
    }
}
