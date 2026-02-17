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
        let flush_start = std::time::Instant::now();
        let messages = self.buffer.drain(self.batch_size);

        // Update buffer size metric after drain
        if let Some(metrics) = Metrics::get() {
            metrics.set_buffer_size(self.buffer.len());
        }

        if messages.is_empty() {
            return;
        }

        let total_events = messages.len();

        // Assign sequence numbers to this batch for checkpoint tracking.
        // Each message gets a unique sequence number for precise tracking.
        let batch_start_seq = self
            .sequence
            .fetch_add(messages.len() as u64, Ordering::SeqCst);

        // Partition messages by destination with zero-copy optimization
        let PartitionResult {
            batches,
            unroutable,
        } = partition_by_destination_owned(messages, &self.emitters);

        if unroutable > 0
            && let Some(m) = Metrics::get()
        {
            m.record_dropped("no_matching_emitter", unroutable);
        }

        // Pre-aggregate metrics to reduce Prometheus HashMap lookups.
        // Key: (emitter_name, event_type), Value: count
        // This reduces O(events) lookups to O(unique label combos).
        let mut forward_counts: std::collections::HashMap<(&str, &str), u64> =
            std::collections::HashMap::new();

        // Send pre-partitioned batches to each emitter.
        // Each emitter's checkpoint advances by the number of events IT processed,
        // not the total batch size (which may differ due to routing).
        for emitter in &self.emitters {
            let routed_events = match batches.get(emitter.name()) {
                Some(events) if !events.is_empty() => events,
                _ => continue,
            };

            let emitter_start = std::time::Instant::now();
            let emit_result = emitter.emit(routed_events).await;
            let emitter_duration = emitter_start.elapsed();

            // Record per-emitter metrics
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
                // Note: checkpoint NOT updated on failure - emitter is behind
            } else {
                debug!(
                    emitter = emitter.name(),
                    count = routed_events.len(),
                    "Emitted (inline)"
                );
                // Aggregate counts locally instead of per-event Prometheus updates
                for msg in routed_events {
                    *forward_counts
                        .entry((emitter.name(), &msg.message_type))
                        .or_default() += 1;
                }
                // Update checkpoint: advance by number of messages this emitter processed.
                // Uses batch_start_seq + emitter's message count - 1 as the checkpoint.
                // This ensures each emitter tracks its own progress independently.
                if let Some(ref store) = self.checkpoint_store {
                    let emitter_end_seq = batch_start_seq + routed_events.len() as u64 - 1;
                    store.set(emitter.name(), emitter_end_seq);
                }
            }
        }

        // Batch update to Prometheus - single HashMap lookup per unique label combo
        if let Some(metrics) = Metrics::get() {
            metrics.record_forwarded_batch(&forward_counts);
            metrics.inc_flush();

            // Calculate and record throughput
            let flush_duration = flush_start.elapsed();
            if flush_duration.as_secs_f64() > 0.0 {
                let events_per_sec = total_events as f64 / flush_duration.as_secs_f64();
                metrics.set_events_per_second(events_per_sec);
            }

            update_pressure(metrics, &self.emitters);
        }
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

        let flush_start = std::time::Instant::now();
        let messages = buffer.drain(batch_size);

        // Update buffer size metric after drain
        if let Some(metrics) = Metrics::get() {
            metrics.set_buffer_size(buffer.len());
        }

        if messages.is_empty() {
            continue;
        }

        let total_events = messages.len();

        // Assign sequence numbers to this batch for checkpoint tracking.
        // Each message gets a unique sequence number for precise tracking.
        let batch_start_seq = sequence.fetch_add(messages.len() as u64, Ordering::SeqCst);

        // Partition messages by destination with zero-copy optimization
        // - Single-destination messages: moved, not cloned
        // - Multi-destination messages: N-1 clones (last one moved)
        let PartitionResult {
            batches,
            unroutable,
        } = partition_by_destination_owned(messages, &emitters);

        if unroutable > 0
            && let Some(m) = Metrics::get()
        {
            m.record_dropped("no_matching_emitter", unroutable);
        }

        // Pre-aggregate metrics to reduce Prometheus HashMap lookups.
        // Key: (emitter_name, event_type), Value: count
        // This reduces O(events) lookups to O(unique label combos).
        let mut forward_counts: std::collections::HashMap<(&str, &str), u64> =
            std::collections::HashMap::new();

        // Send pre-partitioned batches to each emitter.
        // Each emitter's checkpoint advances by the number of events IT processed,
        // not the total batch size (which may differ due to routing).
        for emitter in &emitters {
            let routed_events = match batches.get(emitter.name()) {
                Some(events) if !events.is_empty() => events,
                _ => continue,
            };

            let emitter_start = std::time::Instant::now();
            let emit_result = emitter.emit(routed_events).await;
            let emitter_duration = emitter_start.elapsed();

            // Record per-emitter metrics
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
                // Record emit failures
                if let Some(metrics) = Metrics::get() {
                    metrics.record_dropped(emitter.name(), routed_events.len() as u64);
                }
                // Note: checkpoint NOT updated on failure - emitter is behind
            } else {
                debug!(
                    emitter = emitter.name(),
                    count = routed_events.len(),
                    "Emitted"
                );
                // Aggregate counts locally instead of per-event Prometheus updates
                for msg in routed_events {
                    *forward_counts
                        .entry((emitter.name(), &msg.message_type))
                        .or_default() += 1;
                }
                // Update checkpoint: advance by number of messages this emitter processed.
                // Uses batch_start_seq + emitter's message count - 1 as the checkpoint.
                // This ensures each emitter tracks its own progress independently.
                if let Some(ref store) = checkpoint_store {
                    let emitter_end_seq = batch_start_seq + routed_events.len() as u64 - 1;
                    store.set(emitter.name(), emitter_end_seq);
                }
            }
        }

        // Batch update to Prometheus - single HashMap lookup per unique label combo
        if let Some(metrics) = Metrics::get() {
            metrics.record_forwarded_batch(&forward_counts);
            metrics.inc_flush();

            // Calculate and record throughput
            let flush_duration = flush_start.elapsed();
            if flush_duration.as_secs_f64() > 0.0 {
                let events_per_sec = total_events as f64 / flush_duration.as_secs_f64();
                metrics.set_events_per_second(events_per_sec);
            }

            update_pressure(metrics, &emitters);
        }
    }
}
