//! Tiered buffer with cascading overflow and compression
//!
//! When the primary (hot) buffer fills, messages overflow to a secondary
//! buffer where they are compressed using zstd. This trades CPU for memory,
//! allowing the system to handle traffic spikes without dropping messages.
//!
//! # Architecture
//!
//! ```text
//! Primary Buffer (hot, fast)
//!        │
//!        │ overflow
//!        ▼
//! Secondary Buffer (compressed, slower)
//! ```
//!
//! # Drain Order
//!
//! 1. First drain from primary (oldest data, fast)
//! 2. Then drain from secondary (overflow data, decompress)
//!
//! This maintains FIFO ordering: when primary fills, new messages overflow
//! to secondary, so primary always contains the oldest messages.

use crate::buffer_lockfree::LockFreeBuffer;
use crate::message::Message;
use crate::metrics::Metrics;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Compressed batch of messages stored in secondary buffer
#[derive(Clone)]
pub struct CompressedBatch {
    /// Zstd-compressed serialized messages
    pub data: Bytes,
    /// Number of messages in this batch
    pub count: usize,
    /// Original uncompressed size (for metrics)
    pub original_size: usize,
}

// ============================================================================
// BATCH ACCUMULATOR
// ============================================================================

/// Accumulates messages before compression for better compression ratios.
///
/// Messages are held until either:
/// - `batch_size` messages have accumulated (count-based flush)
/// - `max_age` has elapsed since the first message (age-based flush)
/// - `flush()` is called explicitly (drain or shutdown)
///
/// Thread-safe via internal Mutex (cold path, low contention).
pub struct BatchAccumulator {
    /// Pending messages waiting to form a batch
    messages: Mutex<Vec<Message>>,
    /// Number of messages to accumulate before flushing
    pub batch_size: usize,
    /// Maximum time to hold messages before forcing flush
    max_age: Duration,
    /// Timestamp when first message was added to current batch (nanos since start)
    /// 0 means no messages pending. Uses u64 to avoid overflow for long uptimes.
    first_message_time: AtomicU64,
    /// Reference instant for time calculations
    start: Instant,
}

impl BatchAccumulator {
    /// Create a new batch accumulator
    ///
    /// # Arguments
    /// * `batch_size` - Flush when this many messages accumulated
    /// * `max_age` - Flush when first message is older than this
    pub fn new(batch_size: usize, max_age: Duration) -> Self {
        Self {
            messages: Mutex::new(Vec::with_capacity(batch_size)),
            batch_size: batch_size.max(1), // At least 1
            max_age,
            first_message_time: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    /// Push a message to the accumulator.
    ///
    /// Returns `Some(batch)` if batch_size reached, `None` otherwise.
    /// The returned batch should be compressed and stored.
    pub fn push(&self, msg: Message) -> Option<Vec<Message>> {
        let mut messages = self.messages.lock();

        // Track first message time
        if messages.is_empty() {
            let now_nanos = self.start.elapsed().as_nanos() as u64;
            self.first_message_time.store(now_nanos, Ordering::Release);
        }

        messages.push(msg);

        // Check if batch is ready
        if messages.len() >= self.batch_size {
            self.first_message_time.store(0, Ordering::Release);
            Some(std::mem::replace(
                &mut *messages,
                Vec::with_capacity(self.batch_size),
            ))
        } else {
            None
        }
    }

    /// Force flush any pending messages.
    ///
    /// Returns `Some(batch)` if there were pending messages, `None` if empty.
    pub fn flush(&self) -> Option<Vec<Message>> {
        let mut messages = self.messages.lock();

        if messages.is_empty() {
            return None;
        }

        self.first_message_time.store(0, Ordering::Release);
        Some(std::mem::replace(
            &mut *messages,
            Vec::with_capacity(self.batch_size),
        ))
    }

    /// Check if batch should be flushed due to age.
    ///
    /// Returns true if there are pending messages AND the first message
    /// has been waiting longer than `max_age`.
    pub fn should_flush_by_age(&self) -> bool {
        let first_time = self.first_message_time.load(Ordering::Acquire);
        if first_time == 0 {
            return false; // No messages pending
        }

        // Use u64 throughout to avoid overflow issues with long uptimes
        let now_nanos = self.start.elapsed().as_nanos() as u64;
        let age_nanos = now_nanos.saturating_sub(first_time);
        // Clamp max_age to u64::MAX to handle extreme Duration values safely
        let max_age_nanos = self.max_age.as_nanos().min(u64::MAX as u128) as u64;

        age_nanos >= max_age_nanos
    }

    /// Get number of pending messages
    pub fn len(&self) -> usize {
        self.messages.lock().len()
    }

    /// Check if accumulator is empty
    pub fn is_empty(&self) -> bool {
        self.messages.lock().is_empty()
    }
}

/// Tiered buffer with primary (fast) and secondary (compressed) tiers
pub struct TieredBuffer {
    /// Hot path - uncompressed, fast access
    primary: LockFreeBuffer,
    /// Overflow - compressed batches
    secondary: LockFreeBuffer,
    /// Accumulator for batching messages before compression
    accumulator: BatchAccumulator,
    /// Compression level (1-22, default 3)
    compression_level: i32,
    /// Metrics
    metrics: TieredMetrics,
}

/// Metrics for tiered buffer monitoring
pub struct TieredMetrics {
    /// Messages pushed to primary
    pub primary_pushed: AtomicU64,
    /// Messages overflowed to secondary
    pub overflowed: AtomicU64,
    /// Bytes saved by compression
    pub bytes_saved: AtomicU64,
    /// Messages dropped (both tiers full)
    pub dropped: AtomicU64,
    /// Messages lost due to decode failures during drain
    pub decode_failed: AtomicU64,
    /// Logical message count in secondary (not batch count)
    pub secondary_messages: AtomicU64,
}

impl Default for TieredMetrics {
    fn default() -> Self {
        Self {
            primary_pushed: AtomicU64::new(0),
            overflowed: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            decode_failed: AtomicU64::new(0),
            secondary_messages: AtomicU64::new(0),
        }
    }
}

impl TieredBuffer {
    /// Create a new tiered buffer
    ///
    /// # Arguments
    /// * `primary_capacity` - Size of hot buffer (messages)
    /// * `secondary_capacity` - Size of overflow buffer (compressed batches)
    /// * `batch_size` - Messages per compressed batch
    pub fn new(primary_capacity: usize, secondary_capacity: usize, batch_size: usize) -> Self {
        Self {
            primary: LockFreeBuffer::new(primary_capacity),
            secondary: LockFreeBuffer::new(secondary_capacity),
            accumulator: BatchAccumulator::new(batch_size, Duration::from_millis(100)),
            compression_level: 3, // Fast compression
            metrics: TieredMetrics::default(),
        }
    }

    /// Set compression level (1-22, higher = better ratio, slower)
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level.clamp(1, 22);
        self
    }

    /// Set max age for partial batch flush (default: 100ms)
    pub fn with_max_batch_age(mut self, age: Duration) -> Self {
        let batch_size = self.accumulator.batch_size;
        self.accumulator = BatchAccumulator::new(batch_size, age);
        self
    }

    /// Push a message to the buffer with overflow to secondary
    ///
    /// Returns true if stored (primary or secondary), false if dropped.
    ///
    /// Uses try_push to avoid cloning on the success path. When primary is full,
    /// messages are accumulated into batches before compression for better ratios.
    pub fn push(&self, msg: Message) -> bool {
        // Try primary first - no clone needed on success path
        let msg = match self.primary.try_push(msg) {
            Ok(()) => {
                self.metrics.primary_pushed.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Err(rejected) => rejected, // Primary full, got message back
        };

        // Primary full - add to accumulator for batch compression
        // Check if we should flush due to age first
        #[allow(clippy::collapsible_if)]
        if self.accumulator.should_flush_by_age() {
            if let Some(batch) = self.accumulator.flush() {
                self.store_compressed_batch(batch);
            }
        }

        // Now add the message to accumulator
        if let Some(batch) = self.accumulator.push(msg) {
            // Batch ready - compress and store
            self.store_compressed_batch(batch)
        } else {
            // Message accepted into accumulator, will be compressed later
            true
        }
    }

    /// Drain up to n messages, respecting FIFO order across tiers
    ///
    /// Drains primary (oldest) first, then secondary (overflow/newer).
    /// Flushes any pending accumulator messages before draining secondary.
    ///
    /// Note: May return slightly more than `n` messages when a compressed batch
    /// is decompressed, as we don't split batches mid-decompression.
    pub fn drain(&self, n: usize) -> Vec<Message> {
        let mut result = Vec::with_capacity(n);

        // First drain from primary (oldest, uncompressed)
        result.extend(self.primary.drain(n));
        if result.len() >= n {
            return result;
        }

        // Flush accumulator - if store fails, return messages directly to avoid loss
        #[allow(clippy::collapsible_if)]
        if let Some(batch) = self.accumulator.flush() {
            if !self.store_compressed_batch_internal(&batch) {
                // Store failed - return batch messages directly to avoid data loss
                result.extend(batch);
                if result.len() >= n {
                    return result;
                }
            }
        }

        // Then drain from secondary (newer overflow, compressed)
        let remaining = n.saturating_sub(result.len());
        if remaining == 0 {
            return result;
        }

        let secondary_msgs = self.secondary.drain(remaining);
        let drained_from_secondary = !secondary_msgs.is_empty();

        for wrapper in secondary_msgs {
            // Parse batch count from message_type for tracking
            let expected_count = wrapper
                .message_type
                .strip_prefix("batch.")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1);

            if wrapper.source == "compressed_batch" {
                // New batch format - decompress entire batch
                match zstd::decode_all(wrapper.payload.as_ref()) {
                    Ok(decompressed) => {
                        let batch_messages = self.deserialize_batch(&decompressed);
                        let recovered = batch_messages.len();
                        // Track any messages lost during deserialization
                        if recovered < expected_count {
                            let lost = (expected_count - recovered) as u64;
                            self.metrics
                                .decode_failed
                                .fetch_add(lost, Ordering::Relaxed);
                            tracing::warn!(
                                expected = expected_count,
                                recovered = recovered,
                                "Partial batch recovery during drain"
                            );
                        }
                        // Update secondary_messages counter
                        self.metrics
                            .secondary_messages
                            .fetch_sub(recovered as u64, Ordering::Relaxed);
                        result.extend(batch_messages);
                    }
                    Err(e) => {
                        // Decode failed - track and log
                        self.metrics
                            .decode_failed
                            .fetch_add(expected_count as u64, Ordering::Relaxed);
                        self.metrics
                            .secondary_messages
                            .fetch_sub(expected_count as u64, Ordering::Relaxed);
                        tracing::error!(
                            count = expected_count,
                            error = %e,
                            "Failed to decompress batch during drain"
                        );
                    }
                }
            } else if wrapper.source == "compressed" {
                // Legacy single-message format
                match self.decompress_message(&wrapper) {
                    Some(msg) => {
                        self.metrics
                            .secondary_messages
                            .fetch_sub(1, Ordering::Relaxed);
                        result.push(msg);
                    }
                    None => {
                        self.metrics.decode_failed.fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .secondary_messages
                            .fetch_sub(1, Ordering::Relaxed);
                        tracing::error!("Failed to decompress legacy message during drain");
                    }
                }
            } else {
                result.push(wrapper);
            }
        }

        // Update secondary tier size metric after draining
        // Note: clippy suggests `if ... && let` but that's unstable (RFC 2497)
        #[allow(clippy::collapsible_if)]
        if drained_from_secondary {
            if let Some(m) = Metrics::get() {
                m.set_tiered_secondary_size(self.secondary.len());
            }
        }

        result
    }

    /// Internal version that takes a reference to avoid moving the batch
    fn store_compressed_batch_internal(&self, messages: &[Message]) -> bool {
        if messages.is_empty() {
            return true;
        }

        let count = messages.len();
        let first_id = messages[0].id;
        let first_timestamp = messages[0].timestamp;
        let serialized = self.serialize_batch(messages);
        let original_size = serialized.len();

        match zstd::encode_all(serialized.as_slice(), self.compression_level) {
            Ok(compressed) => {
                let compressed_len = compressed.len();
                let wrapper = Message::with_id(
                    first_id,
                    first_timestamp,
                    "compressed_batch",
                    format!("batch.{count}"),
                    Bytes::from(compressed),
                );

                if self.secondary.push(wrapper) {
                    self.metrics
                        .overflowed
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.metrics
                        .secondary_messages
                        .fetch_add(count as u64, Ordering::Relaxed);
                    let saved = original_size.saturating_sub(compressed_len);
                    self.metrics
                        .bytes_saved
                        .fetch_add(saved as u64, Ordering::Relaxed);

                    if let Some(m) = Metrics::get() {
                        m.record_compression_savings(saved as u64);
                        m.set_tiered_secondary_size(self.secondary.len());
                    }
                    true
                } else {
                    false // Let caller handle the messages
                }
            }
            Err(_) => false,
        }
    }

    /// Decompress a legacy single-message format
    fn decompress_message(&self, wrapper: &Message) -> Option<Message> {
        match zstd::decode_all(wrapper.payload.as_ref()) {
            Ok(decompressed) => self.deserialize_message(&decompressed, wrapper),
            Err(_) => None,
        }
    }

    /// Message serialization preserving all fields
    ///
    /// Format (all strings are length-prefixed with u32):
    /// ```text
    /// [id_len: u32][id: bytes]
    /// [source_len: u32][source: bytes]
    /// [type_len: u32][type: bytes]
    /// [timestamp: i64]
    /// [meta_count: u32][[key_len: u32][key][val_len: u32][val]]...
    /// [route_count: u32][[route_len: u32][route]]...
    /// [payload_len: u32][payload: bytes]
    /// ```
    fn serialize_message(&self, msg: &Message) -> Vec<u8> {
        let mut buf = Vec::new();

        // String helper: len(4) + bytes
        let write_str = |buf: &mut Vec<u8>, s: &str| {
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        };

        write_str(&mut buf, &msg.id.to_string());
        write_str(&mut buf, &msg.source);
        write_str(&mut buf, &msg.message_type);
        buf.extend_from_slice(&msg.timestamp.to_le_bytes());

        // Metadata: count(4) + entries
        let metadata = msg.metadata();
        buf.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        for (k, v) in metadata {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }

        // Routes: count(4) + entries
        buf.extend_from_slice(&(msg.route_to.len() as u32).to_le_bytes());
        for route in &msg.route_to {
            write_str(&mut buf, route);
        }

        // Payload: length-prefixed for batch compatibility
        buf.extend_from_slice(&(msg.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&msg.payload);
        buf
    }

    /// Serialize multiple messages into a single buffer for batch compression.
    ///
    /// Format:
    /// ```text
    /// [count: u32]
    /// [msg1_len: u32][msg1_data: bytes]
    /// [msg2_len: u32][msg2_data: bytes]
    /// ...
    /// ```
    fn serialize_batch(&self, messages: &[Message]) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header: message count
        buf.extend_from_slice(&(messages.len() as u32).to_le_bytes());

        // Each message: length-prefixed
        for msg in messages {
            let serialized = self.serialize_message(msg);
            buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            buf.extend_from_slice(&serialized);
        }

        buf
    }

    /// Deserialize a batch of messages.
    ///
    /// Returns as many messages as could be successfully deserialized.
    /// Truncated or corrupted data returns partial results without panic.
    fn deserialize_batch(&self, data: &[u8]) -> Vec<Message> {
        /// Hard limit on messages per batch to prevent CPU exhaustion on corrupted data
        const MAX_BATCH_COUNT: usize = 10_000;

        let mut messages = Vec::new();
        let mut cursor = 0;

        // Read count
        if cursor + 4 > data.len() {
            return messages;
        }
        let count =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap_or([0; 4])) as usize;
        cursor += 4;

        // Sanity check: apply hard cap and data-based limit
        let max_reasonable_count = data.len() / 10; // Each message is at least ~10 bytes
        let count = count.min(max_reasonable_count).min(MAX_BATCH_COUNT);

        // Read each message
        for _ in 0..count {
            // Read message length
            if cursor + 4 > data.len() {
                break;
            }
            let msg_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap_or([0; 4])) as usize;
            cursor += 4;

            // Read message data
            if cursor + msg_len > data.len() {
                break;
            }
            let msg_data = &data[cursor..cursor + msg_len];
            cursor += msg_len;

            // Deserialize message
            if let Some(msg) = self.deserialize_message_from_batch(msg_data) {
                messages.push(msg);
            }
        }

        messages
    }

    /// Deserialize a single message from batch format (with length-prefixed payload)
    fn deserialize_message_from_batch(&self, data: &[u8]) -> Option<Message> {
        let mut cursor = 0;

        // Helper to read length-prefixed string
        let read_str = |data: &[u8], cursor: &mut usize| -> Option<String> {
            if *cursor + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().ok()?) as usize;
            *cursor += 4;
            if *cursor + len > data.len() {
                return None;
            }
            let s = String::from_utf8(data[*cursor..*cursor + len].to_vec()).ok()?;
            *cursor += len;
            Some(s)
        };

        let id = read_str(data, &mut cursor)?;
        let source = read_str(data, &mut cursor)?;
        let message_type = read_str(data, &mut cursor)?;

        // Read timestamp
        if cursor + 8 > data.len() {
            return None;
        }
        let timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;

        // Read metadata
        if cursor + 4 > data.len() {
            return None;
        }
        let meta_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        let mut metadata = std::collections::HashMap::new();
        for _ in 0..meta_count {
            let k = read_str(data, &mut cursor)?;
            let v = read_str(data, &mut cursor)?;
            metadata.insert(k, v);
        }

        // Read routes
        if cursor + 4 > data.len() {
            return None;
        }
        let route_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        let mut route_to = Vec::with_capacity(route_count);
        for _ in 0..route_count {
            route_to.push(read_str(data, &mut cursor)?);
        }

        // Read payload (length-prefixed in batch format)
        if cursor + 4 > data.len() {
            return None;
        }
        let payload_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + payload_len > data.len() {
            return None;
        }
        let payload = Bytes::copy_from_slice(&data[cursor..cursor + payload_len]);

        Some(Message {
            id: id.into(),
            timestamp,
            source: source.into(),
            message_type: message_type.into(),
            payload,
            metadata: if metadata.is_empty() {
                None
            } else {
                Some(Box::new(metadata))
            },
            route_to: smallvec::SmallVec::from_vec(route_to),
        })
    }

    /// Message deserialization restoring all fields (legacy format without length-prefixed payload)
    fn deserialize_message(&self, data: &[u8], _wrapper: &Message) -> Option<Message> {
        let mut cursor = 0;

        // Helper to read length-prefixed string
        let read_str = |data: &[u8], cursor: &mut usize| -> Option<String> {
            if *cursor + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().ok()?) as usize;
            *cursor += 4;
            if *cursor + len > data.len() {
                return None;
            }
            let s = String::from_utf8(data[*cursor..*cursor + len].to_vec()).ok()?;
            *cursor += len;
            Some(s)
        };

        let id = read_str(data, &mut cursor)?;
        let source = read_str(data, &mut cursor)?;
        let message_type = read_str(data, &mut cursor)?;

        // Read timestamp
        if cursor + 8 > data.len() {
            return None;
        }
        let timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;

        // Read metadata
        if cursor + 4 > data.len() {
            return None;
        }
        let meta_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        let mut metadata = std::collections::HashMap::new();
        for _ in 0..meta_count {
            let k = read_str(data, &mut cursor)?;
            let v = read_str(data, &mut cursor)?;
            metadata.insert(k, v);
        }

        // Read routes
        if cursor + 4 > data.len() {
            return None;
        }
        let route_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        let mut route_to = Vec::with_capacity(route_count);
        for _ in 0..route_count {
            route_to.push(read_str(data, &mut cursor)?);
        }

        // Rest is payload
        let payload = Bytes::copy_from_slice(&data[cursor..]);

        Some(Message {
            id: id.into(),
            timestamp,
            source: source.into(),
            message_type: message_type.into(),
            payload,
            metadata: if metadata.is_empty() {
                None
            } else {
                Some(Box::new(metadata))
            },
            route_to: smallvec::SmallVec::from_vec(route_to),
        })
    }

    /// Get primary buffer length
    pub fn primary_len(&self) -> usize {
        self.primary.len()
    }

    /// Get secondary buffer length (compressed batches)
    pub fn secondary_len(&self) -> usize {
        self.secondary.len()
    }

    /// Get accumulator length (pending messages)
    pub fn accumulator_len(&self) -> usize {
        self.accumulator.len()
    }

    /// Flush any pending messages in accumulator to secondary.
    ///
    /// Call this before shutdown to avoid losing messages.
    ///
    /// Returns `true` if all pending messages were successfully stored in
    /// the secondary buffer, or `false` if they were dropped due to
    /// compression failure or a full secondary buffer.
    pub fn flush_pending(&self) -> bool {
        if let Some(batch) = self.accumulator.flush() {
            self.store_compressed_batch(batch)
        } else {
            // Nothing to flush, so no messages are lost.
            true
        }
    }

    /// Store a batch of messages as compressed data in secondary buffer.
    ///
    /// Returns true if stored, false if secondary is full (batch dropped).
    fn store_compressed_batch(&self, messages: Vec<Message>) -> bool {
        if messages.is_empty() {
            return true;
        }

        let count = messages.len();
        let first_id = messages[0].id;
        let first_timestamp = messages[0].timestamp;
        let serialized = self.serialize_batch(&messages);
        let original_size = serialized.len();

        match zstd::encode_all(serialized.as_slice(), self.compression_level) {
            Ok(compressed) => {
                // Capture length before moving compressed into Bytes
                let compressed_len = compressed.len();

                // Store as wrapper message with "compressed_batch" marker
                let wrapper = Message::with_id(
                    first_id,
                    first_timestamp,
                    "compressed_batch", // New marker for batches
                    format!("batch.{count}"),
                    Bytes::from(compressed), // Move, don't clone
                );

                if self.secondary.push(wrapper) {
                    self.metrics
                        .overflowed
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.metrics
                        .secondary_messages
                        .fetch_add(count as u64, Ordering::Relaxed);
                    let saved = original_size.saturating_sub(compressed_len);
                    self.metrics
                        .bytes_saved
                        .fetch_add(saved as u64, Ordering::Relaxed);

                    // Export to Prometheus
                    if let Some(m) = Metrics::get() {
                        m.record_compression_savings(saved as u64);
                        m.set_tiered_secondary_size(self.secondary.len());
                    }
                    true
                } else {
                    // Secondary full - drop entire batch
                    self.metrics
                        .dropped
                        .fetch_add(count as u64, Ordering::Relaxed);
                    if let Some(m) = Metrics::get() {
                        m.record_buffer_overflow(count as u64);
                    }
                    false
                }
            }
            Err(_) => {
                // Compression failed - drop batch
                self.metrics
                    .dropped
                    .fetch_add(count as u64, Ordering::Relaxed);
                if let Some(m) = Metrics::get() {
                    m.record_buffer_overflow(count as u64);
                }
                false
            }
        }
    }

    /// Get total logical length (primary + secondary messages + accumulator)
    ///
    /// Note: Uses tracked message count for secondary tier (not batch count)
    /// to provide accurate logical message count for flush logic.
    pub fn len(&self) -> usize {
        self.primary.len()
            + self.metrics.secondary_messages.load(Ordering::Relaxed) as usize
            + self.accumulator.len()
    }

    /// Check if all buffers are empty
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.secondary.is_empty() && self.accumulator.is_empty()
    }

    /// Get total messages overflowed to secondary
    pub fn total_overflowed(&self) -> u64 {
        self.metrics.overflowed.load(Ordering::Relaxed)
    }

    /// Get total bytes saved by compression
    pub fn bytes_saved(&self) -> u64 {
        self.metrics.bytes_saved.load(Ordering::Relaxed)
    }

    /// Get total dropped messages
    pub fn total_dropped(&self) -> u64 {
        self.metrics.dropped.load(Ordering::Relaxed)
    }

    /// Get total messages lost due to decode failures
    pub fn total_decode_failed(&self) -> u64 {
        self.metrics.decode_failed.load(Ordering::Relaxed)
    }

    /// Get primary buffer capacity
    pub fn primary_capacity(&self) -> usize {
        self.primary.capacity()
    }

    /// Get secondary buffer capacity
    pub fn secondary_capacity(&self) -> usize {
        self.secondary.capacity()
    }

    /// Get total capacity (primary + secondary)
    pub fn capacity(&self) -> usize {
        self.primary.capacity() + self.secondary.capacity()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cloned_ref_to_slice_refs
)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_message(id: &str, payload_size: usize) -> Message {
        let payload = vec![0x42u8; payload_size];
        Message::with_id(id, 12345, "test-source", "test-event", Bytes::from(payload))
    }

    fn make_message_with_fields(
        id: &str,
        source: &str,
        msg_type: &str,
        timestamp: i64,
        payload: &[u8],
    ) -> Message {
        Message::with_id(
            id,
            timestamp,
            source,
            msg_type,
            Bytes::from(payload.to_vec()),
        )
    }

    // ==========================================================================
    // BATCH ACCUMULATOR TESTS (TDD - write first, implement later)
    // ==========================================================================

    mod batch_accumulator_tests {
        use super::*;

        #[test]
        fn test_accumulator_empty_on_creation() {
            let acc = BatchAccumulator::new(10, Duration::from_millis(100));
            assert!(acc.is_empty());
            assert_eq!(acc.len(), 0);
        }

        #[test]
        fn test_accumulator_push_returns_none_until_batch_size() {
            let acc = BatchAccumulator::new(3, Duration::from_millis(100));

            // First two pushes return None (not full yet)
            assert!(acc.push(make_message("m1", 100)).is_none());
            assert_eq!(acc.len(), 1);

            assert!(acc.push(make_message("m2", 100)).is_none());
            assert_eq!(acc.len(), 2);

            // Third push triggers batch return
            let batch = acc.push(make_message("m3", 100));
            assert!(batch.is_some());
            let batch = batch.unwrap();
            assert_eq!(batch.len(), 3);

            // Accumulator should be empty after batch returned
            assert!(acc.is_empty());
        }

        #[test]
        fn test_accumulator_flush_returns_partial_batch() {
            let acc = BatchAccumulator::new(10, Duration::from_millis(100));

            acc.push(make_message("m1", 100));
            acc.push(make_message("m2", 100));
            assert_eq!(acc.len(), 2);

            // Flush returns partial batch
            let batch = acc.flush();
            assert!(batch.is_some());
            assert_eq!(batch.unwrap().len(), 2);

            // Accumulator should be empty after flush
            assert!(acc.is_empty());
        }

        #[test]
        fn test_accumulator_flush_on_empty_returns_none() {
            let acc = BatchAccumulator::new(10, Duration::from_millis(100));
            assert!(acc.flush().is_none());
        }

        #[test]
        fn test_accumulator_preserves_message_order() {
            let acc = BatchAccumulator::new(5, Duration::from_millis(100));

            // Push 4 messages, 5th triggers batch return
            acc.push(make_message("msg-0", 100));
            acc.push(make_message("msg-1", 100));
            acc.push(make_message("msg-2", 100));
            acc.push(make_message("msg-3", 100));
            let batch = acc.push(make_message("msg-4", 100)).unwrap();

            // Verify order preserved
            assert_eq!(batch[0].id, "msg-0");
            assert_eq!(batch[1].id, "msg-1");
            assert_eq!(batch[2].id, "msg-2");
            assert_eq!(batch[3].id, "msg-3");
            assert_eq!(batch[4].id, "msg-4");
        }

        #[test]
        fn test_accumulator_batch_size_one_returns_immediately() {
            let acc = BatchAccumulator::new(1, Duration::from_millis(100));

            let batch = acc.push(make_message("m1", 100));
            assert!(batch.is_some());
            assert_eq!(batch.unwrap().len(), 1);
            assert!(acc.is_empty());
        }

        #[test]
        fn test_accumulator_concurrent_push_is_safe() {
            use std::sync::Arc;
            use std::thread;

            let acc = Arc::new(BatchAccumulator::new(100, Duration::from_millis(1000)));
            let mut handles = vec![];

            // 4 threads each pushing 25 messages
            for t in 0..4 {
                let acc_clone = Arc::clone(&acc);
                handles.push(thread::spawn(move || {
                    let mut batches_returned = 0;
                    for i in 0..25 {
                        if acc_clone
                            .push(make_message(&format!("t{t}-{i}"), 50))
                            .is_some()
                        {
                            batches_returned += 1;
                        }
                    }
                    batches_returned
                }));
            }

            let total_batches: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

            // Should have gotten exactly 1 batch (100 messages / 100 batch_size)
            // The accumulator should be empty
            assert_eq!(total_batches, 1);
            assert!(acc.is_empty());
        }

        #[test]
        fn test_accumulator_tracks_age_from_first_message() {
            let acc = BatchAccumulator::new(100, Duration::from_millis(50));

            // No age tracking when empty
            assert!(!acc.should_flush_by_age());

            // Push first message - age tracking starts
            acc.push(make_message("m1", 100));

            // Not old enough yet
            assert!(!acc.should_flush_by_age());

            // Wait for expiry
            std::thread::sleep(Duration::from_millis(60));

            // Now should indicate age-based flush needed
            assert!(acc.should_flush_by_age());
        }

        #[test]
        fn test_accumulator_age_resets_after_flush() {
            let acc = BatchAccumulator::new(100, Duration::from_millis(50));

            acc.push(make_message("m1", 100));
            std::thread::sleep(Duration::from_millis(60));
            assert!(acc.should_flush_by_age());

            // Flush resets age tracking
            acc.flush();
            assert!(!acc.should_flush_by_age());

            // New message starts fresh age
            acc.push(make_message("m2", 100));
            assert!(!acc.should_flush_by_age());
        }
    }

    // ==========================================================================
    // BATCH SERIALIZATION TESTS
    // ==========================================================================

    mod batch_serialization_tests {
        use super::*;

        #[test]
        fn test_serialize_empty_batch() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let messages: Vec<Message> = vec![];
            let serialized = buffer.serialize_batch(&messages);

            // Should have count header (4 bytes) with value 0
            assert_eq!(serialized.len(), 4);
            let count = u32::from_le_bytes(serialized[0..4].try_into().unwrap());
            assert_eq!(count, 0);
        }

        #[test]
        fn test_serialize_deserialize_single_message_batch() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let msg = make_message_with_fields("id-1", "svc-a", "evt.created", 12345, b"payload");

            let serialized = buffer.serialize_batch(&[msg.clone()]);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 1);
            assert_eq!(deserialized[0].id, msg.id);
            assert_eq!(deserialized[0].source, msg.source);
            assert_eq!(deserialized[0].message_type, msg.message_type);
            assert_eq!(deserialized[0].timestamp, msg.timestamp);
            assert_eq!(deserialized[0].payload, msg.payload);
        }

        #[test]
        fn test_serialize_deserialize_multiple_messages() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let messages: Vec<Message> = (0..5)
                .map(|i| {
                    make_message_with_fields(
                        &format!("id-{i}"),
                        &format!("svc-{i}"),
                        &format!("evt.type.{i}"),
                        1000 + i as i64,
                        format!("payload-{i}").as_bytes(),
                    )
                })
                .collect();

            let serialized = buffer.serialize_batch(&messages);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 5);
            for (i, (original, recovered)) in messages.iter().zip(deserialized.iter()).enumerate() {
                // IDs match after round-trip (compare string representations)
                assert_eq!(
                    recovered.id.to_string(),
                    original.id.to_string(),
                    "ID mismatch at index {i}"
                );
                assert_eq!(recovered.source, format!("svc-{i}"));
                assert_eq!(recovered.message_type, format!("evt.type.{i}"));
                assert_eq!(recovered.timestamp, 1000 + i as i64);
            }
        }

        #[test]
        fn test_serialize_preserves_metadata() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let mut msg = make_message("id-1", 100);
            msg.metadata_mut()
                .insert("trace_id".to_string(), "abc-123".to_string());
            msg.metadata_mut()
                .insert("tenant".to_string(), "acme".to_string());

            let serialized = buffer.serialize_batch(&[msg]);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 1);
            assert_eq!(
                deserialized[0].metadata().get("trace_id"),
                Some(&"abc-123".to_string())
            );
            assert_eq!(
                deserialized[0].metadata().get("tenant"),
                Some(&"acme".to_string())
            );
        }

        #[test]
        fn test_serialize_preserves_routes() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let mut msg = make_message("id-1", 100);
            msg.route_to = smallvec::smallvec!["kafka".to_string(), "s3".to_string()];

            let serialized = buffer.serialize_batch(&[msg]);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 1);
            assert_eq!(deserialized[0].route_to.len(), 2);
            assert_eq!(deserialized[0].route_to[0], "kafka");
            assert_eq!(deserialized[0].route_to[1], "s3");
        }

        #[test]
        fn test_serialize_handles_empty_payload() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let msg = make_message_with_fields("id-1", "svc", "evt", 12345, b"");

            let serialized = buffer.serialize_batch(&[msg]);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 1);
            assert!(deserialized[0].payload.is_empty());
        }

        #[test]
        fn test_serialize_handles_large_payload() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let large_payload = vec![0xAB; 100_000]; // 100KB
            let msg = make_message_with_fields("id-1", "svc", "evt", 12345, &large_payload);

            let serialized = buffer.serialize_batch(&[msg]);
            let deserialized = buffer.deserialize_batch(&serialized);

            assert_eq!(deserialized.len(), 1);
            assert_eq!(deserialized[0].payload.len(), 100_000);
            assert!(deserialized[0].payload.iter().all(|&b| b == 0xAB));
        }

        #[test]
        fn test_deserialize_truncated_data_returns_partial() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let messages: Vec<Message> = (0..3)
                .map(|i| make_message(&format!("m{i}"), 100))
                .collect();

            let serialized = buffer.serialize_batch(&messages);

            // Truncate in the middle of the third message
            let truncated = &serialized[..serialized.len() - 50];
            let deserialized = buffer.deserialize_batch(truncated);

            // Should recover first two messages
            assert_eq!(deserialized.len(), 2);
        }

        #[test]
        fn test_deserialize_corrupted_count_handles_gracefully() {
            let buffer = TieredBuffer::new(10, 10, 5);

            // Create data with impossibly large count
            let mut bad_data = vec![0; 100];
            bad_data[0..4].copy_from_slice(&u32::MAX.to_le_bytes());

            let deserialized = buffer.deserialize_batch(&bad_data);

            // Should return empty or partial, not panic
            assert!(deserialized.len() < 1000);
        }

        #[test]
        fn test_deserialize_empty_data_returns_empty() {
            let buffer = TieredBuffer::new(10, 10, 5);
            let deserialized = buffer.deserialize_batch(&[]);
            assert!(deserialized.is_empty());
        }
    }

    // ==========================================================================
    // TIERED BUFFER WITH BATCHING TESTS
    // ==========================================================================

    mod tiered_buffer_batch_tests {
        use super::*;

        #[test]
        fn test_batch_compression_better_ratio_than_individual() {
            // Create two buffers - one with batch_size=1 (like current), one with batch_size=10
            let individual = TieredBuffer::new(1, 100, 1);
            let batched = TieredBuffer::new(1, 100, 10);

            // Push 10 similar messages to each (similar data compresses better together)
            for i in 0..11 {
                let msg = make_message_with_fields(
                    &format!("id-{i}"),
                    "same-service",
                    "same.event.type",
                    12345,
                    b"repeated payload data that should compress well together",
                );
                individual.push(msg.clone());
                batched.push(msg);
            }

            // Batched should save more bytes due to cross-message compression
            let individual_saved = individual.bytes_saved();
            let batched_saved = batched.bytes_saved();

            // Both should have saved something
            assert!(individual_saved > 0, "Individual should save bytes");
            assert!(batched_saved > 0, "Batched should save bytes");

            // Batched should be more efficient (save more bytes per message)
            // This test validates the core value proposition
            assert!(
                batched_saved >= individual_saved,
                "Batched ({batched_saved}) should save at least as much as individual ({individual_saved})"
            );
        }

        #[test]
        fn test_overflow_accumulates_until_batch_size() {
            let buffer = TieredBuffer::new(2, 10, 3); // batch_size = 3

            // Push 2 to fill primary
            buffer.push(make_message("m0", 100));
            buffer.push(make_message("m1", 100));
            assert_eq!(buffer.primary_len(), 2);
            assert_eq!(buffer.secondary_len(), 0);

            // Push 3 more - should accumulate in batch, then compress
            buffer.push(make_message("m2", 100)); // accumulator: 1
            buffer.push(make_message("m3", 100)); // accumulator: 2
            assert_eq!(buffer.secondary_len(), 0); // Not yet compressed

            buffer.push(make_message("m4", 100)); // accumulator: 3 -> flush to secondary
            assert_eq!(buffer.secondary_len(), 1); // One compressed batch
            assert_eq!(buffer.total_overflowed(), 3); // 3 messages overflowed
        }

        #[test]
        fn test_drain_flushes_pending_accumulator() {
            let buffer = TieredBuffer::new(2, 10, 10); // Large batch_size

            // Create messages with known IDs
            let m0 = make_message("m0", 100);
            let m1 = make_message("m1", 100);
            let m2 = make_message("m2", 100);
            let m3 = make_message("m3", 100);

            // Store original IDs for comparison
            let id0 = m0.id.to_string();
            let id1 = m1.id.to_string();
            let id2 = m2.id.to_string();
            let id3 = m3.id.to_string();

            // Fill primary
            buffer.push(m0);
            buffer.push(m1);

            // Partial batch in accumulator
            buffer.push(m2);
            buffer.push(m3);

            // Drain should flush accumulator and return all messages
            let drained = buffer.drain(10);
            assert_eq!(drained.len(), 4);

            // Verify all messages recovered (compare string representations)
            assert_eq!(drained[0].id.to_string(), id0);
            assert_eq!(drained[1].id.to_string(), id1);
            assert_eq!(drained[2].id.to_string(), id2);
            assert_eq!(drained[3].id.to_string(), id3);
        }

        #[test]
        fn test_drain_maintains_fifo_with_batches() {
            let buffer = TieredBuffer::new(1, 10, 2); // tiny primary, batch_size=2

            // Push 5 messages with increasing timestamps
            for i in 0..5 {
                let mut msg = make_message(&format!("m{i}"), 100);
                msg.timestamp = (i * 1000) as i64;
                buffer.push(msg);
            }

            let drained = buffer.drain(10);

            // Verify FIFO order by timestamp
            for (i, msg) in drained.iter().enumerate() {
                assert_eq!(
                    msg.timestamp,
                    (i * 1000) as i64,
                    "Message {i} has wrong timestamp"
                );
            }
        }

        #[test]
        fn test_backwards_compat_reads_legacy_single_compressed() {
            let buffer = TieredBuffer::new(1, 10, 5);

            // Simulate legacy format: source="compressed" with single message
            let original = make_message("legacy-msg", 500);
            let original_id = original.id.to_string();
            let serialized = buffer.serialize_message(&original);
            let compressed =
                zstd::encode_all(serialized.as_slice(), 3).expect("compression should work");

            // Create legacy wrapper
            let wrapper = Message::with_id(
                original.id,
                original.timestamp,
                "compressed", // Legacy marker
                original.message_type.to_string(),
                Bytes::from(compressed),
            );

            // Push directly to secondary (simulating existing data)
            buffer.secondary.push(wrapper);

            // Drain should recover the message
            let drained = buffer.drain(10);
            assert_eq!(drained.len(), 1);
            // Compare string representations since ID goes through serialization
            assert_eq!(drained[0].id.to_string(), original_id);
        }

        #[test]
        fn test_flush_pending_moves_accumulator_to_secondary() {
            let buffer = TieredBuffer::new(2, 10, 10);

            // Fill primary
            buffer.push(make_message("m0", 100));
            buffer.push(make_message("m1", 100));

            // Partial batch
            buffer.push(make_message("m2", 100));
            buffer.push(make_message("m3", 100));

            assert_eq!(buffer.secondary_len(), 0);
            assert_eq!(buffer.accumulator_len(), 2);

            // Flush pending
            buffer.flush_pending();

            assert_eq!(buffer.secondary_len(), 1); // Compressed batch
            assert_eq!(buffer.accumulator_len(), 0); // Accumulator empty
        }

        #[test]
        fn test_len_includes_accumulator() {
            let buffer = TieredBuffer::new(2, 10, 10);

            buffer.push(make_message("m0", 100)); // primary
            buffer.push(make_message("m1", 100)); // primary
            buffer.push(make_message("m2", 100)); // accumulator
            buffer.push(make_message("m3", 100)); // accumulator

            // len() should count all messages including pending
            assert_eq!(buffer.len(), 4);
            assert_eq!(buffer.primary_len(), 2);
            assert_eq!(buffer.accumulator_len(), 2);
        }

        #[test]
        fn test_secondary_full_drops_batch() {
            let buffer = TieredBuffer::new(1, 1, 2); // Tiny secondary (1 batch)

            // Fill primary
            buffer.push(make_message("m0", 100));

            // First batch goes to secondary
            buffer.push(make_message("m1", 100));
            buffer.push(make_message("m2", 100));
            assert_eq!(buffer.secondary_len(), 1);

            // Second batch - secondary full, should drop
            buffer.push(make_message("m3", 100));
            buffer.push(make_message("m4", 100));

            // Both messages in second batch should be dropped
            assert_eq!(buffer.total_dropped(), 2);
        }

        #[test]
        fn test_compression_failure_drops_batch() {
            // This is hard to trigger with zstd, but we should handle it
            // For now, just verify the buffer doesn't panic with edge cases
            let buffer = TieredBuffer::new(1, 10, 1);

            // Empty payload message
            let msg = make_message_with_fields("id", "svc", "evt", 0, b"");
            buffer.push(msg.clone());
            buffer.push(msg); // Overflow

            // Should not panic, message either stored or dropped
            assert!(buffer.total_overflowed() + buffer.total_dropped() >= 1);
        }

        #[test]
        fn test_with_max_batch_age_builder() {
            let buffer =
                TieredBuffer::new(10, 10, 100).with_max_batch_age(Duration::from_millis(50));

            // Fill primary
            for i in 0..10 {
                buffer.push(make_message(&format!("p{i}"), 100));
            }

            // Start accumulating
            buffer.push(make_message("a0", 100));
            buffer.push(make_message("a1", 100));

            // Wait for age expiry
            std::thread::sleep(Duration::from_millis(60));

            // Next push should trigger age-based flush
            buffer.push(make_message("a2", 100));

            // Accumulator should have been flushed (either by age check or we have a fresh batch)
            // This test verifies the builder wires up correctly
            assert!(buffer.secondary_len() >= 1 || buffer.accumulator_len() <= 1);
        }
    }

    #[test]
    fn test_primary_buffer_normal_operation() {
        let buffer = TieredBuffer::new(10, 10, 5);

        // Push 5 messages - should go to primary
        for i in 0..5 {
            assert!(buffer.push(make_message(&format!("msg-{i}"), 100)));
        }

        assert_eq!(buffer.primary_len(), 5);
        assert_eq!(buffer.secondary_len(), 0);
        assert_eq!(buffer.total_overflowed(), 0);
    }

    #[test]
    fn test_overflow_to_secondary() {
        // Small primary (3), batch_size=2 so overflow goes to secondary after 2 messages
        let buffer = TieredBuffer::new(3, 10, 2);

        // Push 5 messages - 3 go to primary, 2 go to accumulator then secondary
        for i in 0..5 {
            assert!(buffer.push(make_message(&format!("msg-{i}"), 100)));
        }

        assert_eq!(buffer.primary_len(), 3);
        // With batch_size=2, 2 messages form a batch -> 1 compressed batch in secondary
        assert_eq!(buffer.secondary_len(), 1);
        assert_eq!(buffer.total_overflowed(), 2);
    }

    #[test]
    fn test_compression_reduces_size() {
        // batch_size=1 to get immediate compression (like legacy behavior)
        let buffer = TieredBuffer::new(1, 10, 1);

        // Push 2 messages with compressible data (repeated bytes)
        let msg1 = make_message("msg-1", 1000); // 1KB of 0x42
        let msg2 = make_message("msg-2", 1000);

        assert!(buffer.push(msg1)); // Goes to primary
        assert!(buffer.push(msg2)); // Overflows, batch_size=1 so immediately compressed

        // Compression should save bytes (repeated data compresses well)
        assert!(buffer.bytes_saved() > 0, "Compression should save bytes");
    }

    #[test]
    fn test_drain_respects_fifo_order() {
        let buffer = TieredBuffer::new(2, 10, 5);

        // Push 4 messages with distinct timestamps for ordering verification
        for i in 0..4 {
            let mut msg = make_message(&format!("msg-{i}"), 100);
            msg.timestamp = i as i64 * 1000; // Distinct timestamps
            buffer.push(msg);
        }

        // Drain should return true FIFO order:
        // Primary was filled first (msg-0, msg-1), then overflow to secondary (msg-2, msg-3)
        let drained = buffer.drain(4);
        assert_eq!(drained.len(), 4);

        // Verify FIFO order by timestamp (primary oldest first, then secondary)
        assert_eq!(drained[0].timestamp, 0);
        assert_eq!(drained[1].timestamp, 1000);
        assert_eq!(drained[2].timestamp, 2000);
        assert_eq!(drained[3].timestamp, 3000);

        // Primary messages keep their IDs (not compressed)
        assert_eq!(drained[0].id, "msg-0");
        assert_eq!(drained[1].id, "msg-1");
        // Secondary messages went through compression - IDs are valid but transformed
        assert!(!drained[2].id.to_string().is_empty());
        assert!(!drained[3].id.to_string().is_empty());
    }

    #[test]
    fn test_round_trip_preserves_message() {
        let buffer = TieredBuffer::new(1, 10, 5);

        // Create message with all fields populated
        let mut original = Message::with_id(
            "test-id-123",
            999_888_777,
            "my-service",
            "user.created",
            Bytes::from(b"hello world payload".to_vec()),
        );
        original
            .metadata_mut()
            .insert("trace_id".to_string(), "abc-123".to_string());
        original
            .metadata_mut()
            .insert("tenant".to_string(), "acme".to_string());
        original.route_to = smallvec::smallvec!["kafka".to_string(), "webhook".to_string()];

        // Push twice - first goes to primary, second overflows (compressed)
        buffer.push(original.clone());
        buffer.push(original.clone());

        // Drain primary first (FIFO), then get the compressed one from secondary
        let drained = buffer.drain(2);
        assert_eq!(drained.len(), 2);

        // Second message went to secondary (compressed) - verify full round-trip
        let recovered = &drained[1];
        assert_eq!(recovered.id, original.id);
        assert_eq!(recovered.timestamp, original.timestamp);
        assert_eq!(recovered.source, original.source);
        assert_eq!(recovered.message_type, original.message_type);
        assert_eq!(recovered.payload, original.payload);
        // Verify metadata preserved through compression
        assert_eq!(
            recovered.metadata().get("trace_id"),
            Some(&"abc-123".to_string())
        );
        assert_eq!(
            recovered.metadata().get("tenant"),
            Some(&"acme".to_string())
        );
        // Verify routes preserved through compression
        assert_eq!(recovered.route_to.len(), 2);
        assert_eq!(recovered.route_to[0], "kafka");
        assert_eq!(recovered.route_to[1], "webhook");
    }

    #[test]
    fn test_both_buffers_full_drops_messages() {
        // Tiny buffers with batch_size=2 so batches form and fill secondary
        let buffer = TieredBuffer::new(2, 1, 2); // secondary can hold 1 batch

        // Push 6 messages:
        // - 2 go to primary
        // - 2 go to accumulator -> batch -> secondary (1 batch)
        // - 2 more go to accumulator -> batch -> secondary FULL -> dropped
        for i in 0..6 {
            buffer.push(make_message(&format!("msg-{i}"), 100));
        }

        assert_eq!(buffer.total_dropped(), 2, "Should drop last 2 messages");
        // len = 2 primary + 2 messages in secondary batch = 4
        // But secondary.len() counts batches, not messages
        // Our len() now counts primary + secondary + accumulator
        assert_eq!(buffer.primary_len(), 2);
        assert_eq!(buffer.secondary_len(), 1); // 1 batch
    }

    #[test]
    fn test_empty_buffer_drain() {
        let buffer = TieredBuffer::new(10, 10, 5);
        let drained = buffer.drain(100);
        assert!(drained.is_empty());
    }

    #[test]
    fn test_compression_level_affects_ratio() {
        // Higher compression level should give better ratio
        // Use batch_size=1 to get immediate compression
        let buffer_fast = TieredBuffer::new(1, 10, 1).with_compression_level(1);
        let buffer_best = TieredBuffer::new(1, 10, 1).with_compression_level(19);

        // Large compressible message
        let msg = make_message("msg", 10_000);

        // Fill primary, overflow to secondary (batch_size=1 means immediate compression)
        buffer_fast.push(msg.clone());
        buffer_fast.push(msg.clone());

        buffer_best.push(msg.clone());
        buffer_best.push(msg.clone());

        // Both should compress, but level 19 might save more (or same for simple data)
        assert!(buffer_fast.bytes_saved() > 0);
        assert!(buffer_best.bytes_saved() > 0);
    }

    // ==========================================================================
    // BUG-EXPOSING TESTS
    // ==========================================================================

    /// BUG: TieredBuffer clones message on EVERY push, even when primary succeeds.
    ///
    /// The code does: `self.primary.push(msg.clone())` which means every single
    /// message is cloned, regardless of whether it goes to primary or overflows.
    ///
    /// EXPECTED: When primary has space, push should consume the message directly
    /// without cloning. Clone should only happen on overflow to secondary.
    #[test]
    fn test_bug_unnecessary_clone_on_every_push() {
        use crate::buffer_lockfree::LockFreeBuffer;
        use std::time::Instant;

        let iterations = 1000;
        let payload_size = 10_000; // 10KB

        // Prepare messages for LockFreeBuffer
        let lockfree_msgs: Vec<_> = (0..iterations)
            .map(|i| make_message(&format!("lf-{i}"), payload_size))
            .collect();

        // Prepare messages for TieredBuffer
        let tiered_msgs: Vec<_> = (0..iterations)
            .map(|i| make_message(&format!("tb-{i}"), payload_size))
            .collect();

        // Time LockFreeBuffer (no clone)
        let lockfree = LockFreeBuffer::new(iterations + 100);
        let start = Instant::now();
        for msg in lockfree_msgs {
            lockfree.push(msg);
        }
        let lockfree_time = start.elapsed();

        // Time TieredBuffer (BUG: clones every message)
        let tiered = TieredBuffer::new(iterations + 100, 100, 5);
        let start = Instant::now();
        for msg in tiered_msgs {
            tiered.push(msg);
        }
        let tiered_time = start.elapsed();

        // EXPECTED: TieredBuffer should be about the same speed as LockFreeBuffer
        // for primary-only pushes (no overflow, no compression needed).
        //
        // BUG: TieredBuffer is slower because it clones every message unnecessarily.
        // We expect tiered_time to be noticeably higher than lockfree_time.
        //
        // Allow 50% overhead tolerance for non-clone factors
        let max_acceptable_overhead = lockfree_time.as_nanos() as f64 * 1.5;

        assert!(
            (tiered_time.as_nanos() as f64) <= max_acceptable_overhead,
            "BUG: TieredBuffer took {:?} vs LockFreeBuffer {:?}. \
             TieredBuffer should not be >50% slower for primary-only pushes. \
             The extra time is due to unnecessary cloning on every push.",
            tiered_time,
            lockfree_time
        );
    }

    /// BUG: Clone overhead compounds with message size.
    ///
    /// EXPECTED: Push time should be O(1) regardless of payload size when going to primary.
    /// BUG: Push time scales with payload size because of the unnecessary clone.
    #[test]
    fn test_bug_clone_overhead_scales_with_size() {
        use std::time::Instant;

        // Test with small payloads
        let small_msgs: Vec<_> = (0..1000)
            .map(|i| make_message(&format!("s-{i}"), 100)) // 100 bytes each
            .collect();

        // Test with large payloads
        let large_msgs: Vec<_> = (0..1000)
            .map(|i| make_message(&format!("l-{i}"), 10_000)) // 10KB each
            .collect();

        let small_buffer = TieredBuffer::new(2000, 100, 5);
        let start = Instant::now();
        for msg in small_msgs {
            small_buffer.push(msg);
        }
        let small_time = start.elapsed();

        let large_buffer = TieredBuffer::new(2000, 100, 5);
        let start = Instant::now();
        for msg in large_msgs {
            large_buffer.push(msg);
        }
        let large_time = start.elapsed();

        // EXPECTED: Push time should be roughly the same regardless of payload size,
        // because Bytes uses refcounting and moving a message is O(1).
        //
        // BUG: Large payloads take much longer because clone() copies the entire payload.
        // We expect large_time >> small_time (100x larger payload = much slower)
        //
        // Allow 3x difference for legitimate overhead
        let ratio = large_time.as_nanos() as f64 / small_time.as_nanos() as f64;

        assert!(
            ratio <= 3.0,
            "BUG: Large payload push took {:?} vs small payload {:?} (ratio: {:.1}x). \
             Push time should be O(1) regardless of payload size. \
             The scaling is due to unnecessary cloning of payload data.",
            large_time,
            small_time,
            ratio
        );
    }
}
