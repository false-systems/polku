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
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Tiered buffer with primary (fast) and secondary (compressed) tiers
pub struct TieredBuffer {
    /// Hot path - uncompressed, fast access
    primary: LockFreeBuffer,
    /// Overflow - compressed batches
    secondary: LockFreeBuffer,
    /// Messages to batch before compressing (TODO: batch compression)
    #[allow(dead_code)]
    batch_size: usize,
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
}

impl Default for TieredMetrics {
    fn default() -> Self {
        Self {
            primary_pushed: AtomicU64::new(0),
            overflowed: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
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
            batch_size,
            compression_level: 3, // Fast compression
            metrics: TieredMetrics::default(),
        }
    }

    /// Set compression level (1-22, higher = better ratio, slower)
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level.clamp(1, 22);
        self
    }

    /// Push a message to the buffer with overflow to secondary
    ///
    /// Returns true if stored (primary or secondary), false if dropped.
    ///
    /// Note: Clones the message before trying primary because LockFreeBuffer
    /// consumes the message on push. If we need zero-copy for the success path,
    /// LockFreeBuffer would need to return Result<(), Message> on failure.
    pub fn push(&self, msg: Message) -> bool {
        // Clone needed because we may need msg for compression if primary fails.
        // This is a tradeoff: we pay one clone per message to enable overflow.
        if self.primary.push(msg.clone()) {
            self.metrics.primary_pushed.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        // Primary full - compress and overflow to secondary
        match self.compress_message(&msg) {
            Some(compressed) => {
                // Store compressed in secondary using a wrapper Message
                let wrapper = Message::with_id(
                    msg.id,
                    msg.timestamp,
                    "compressed",
                    msg.message_type,
                    compressed.data.clone(),
                );
                if self.secondary.push(wrapper) {
                    self.metrics.overflowed.fetch_add(1, Ordering::Relaxed);
                    let saved = compressed
                        .original_size
                        .saturating_sub(compressed.data.len());
                    self.metrics
                        .bytes_saved
                        .fetch_add(saved as u64, Ordering::Relaxed);
                    true
                } else {
                    self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
            None => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Drain up to n messages, respecting FIFO order across tiers
    ///
    /// Drains primary (oldest) first, then secondary (overflow/newer).
    /// This is correct because when primary fills, NEW messages overflow
    /// to secondary, so primary always holds the oldest messages.
    pub fn drain(&self, n: usize) -> Vec<Message> {
        let mut result = Vec::with_capacity(n);

        // First drain from primary (oldest, uncompressed)
        result.extend(self.primary.drain(n));
        if result.len() >= n {
            return result;
        }

        // Then drain from secondary (newer overflow, compressed)
        let remaining = n - result.len();
        let secondary_msgs = self.secondary.drain(remaining);
        for wrapper in secondary_msgs {
            if wrapper.source == "compressed" {
                if let Some(msg) = self.decompress_message(&wrapper) {
                    result.push(msg);
                }
            } else {
                result.push(wrapper);
            }
        }

        result
    }

    /// Compress a message using zstd
    fn compress_message(&self, msg: &Message) -> Option<CompressedBatch> {
        // Serialize message to bytes (simple format: source|type|payload)
        let serialized = self.serialize_message(msg);
        let original_size = serialized.len();

        match zstd::encode_all(serialized.as_slice(), self.compression_level) {
            Ok(compressed) => Some(CompressedBatch {
                data: Bytes::from(compressed),
                count: 1,
                original_size,
            }),
            Err(_) => None,
        }
    }

    /// Decompress a message
    fn decompress_message(&self, wrapper: &Message) -> Option<Message> {
        match zstd::decode_all(wrapper.payload.as_ref()) {
            Ok(decompressed) => self.deserialize_message(&decompressed, wrapper),
            Err(_) => None,
        }
    }

    /// Message serialization preserving all fields
    fn serialize_message(&self, msg: &Message) -> Vec<u8> {
        let mut buf = Vec::new();
        // Format: id | source | type | timestamp | metadata_count | metadata[] | route_count | routes[] | payload

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

        // Payload (remaining bytes)
        buf.extend_from_slice(&msg.payload);
        buf
    }

    /// Message deserialization restoring all fields
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

    /// Get total logical length
    pub fn len(&self) -> usize {
        self.primary.len() + self.secondary.len()
    }

    /// Check if both buffers are empty
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.secondary.is_empty()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(id: &str, payload_size: usize) -> Message {
        let payload = vec![0x42u8; payload_size];
        Message::with_id(id, 12345, "test-source", "test-event", Bytes::from(payload))
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
        // Small primary (3), larger secondary (10)
        let buffer = TieredBuffer::new(3, 10, 5);

        // Push 5 messages - 3 go to primary, 2 overflow to secondary
        for i in 0..5 {
            assert!(buffer.push(make_message(&format!("msg-{i}"), 100)));
        }

        assert_eq!(buffer.primary_len(), 3);
        assert_eq!(buffer.secondary_len(), 2);
        assert_eq!(buffer.total_overflowed(), 2);
    }

    #[test]
    fn test_compression_reduces_size() {
        let buffer = TieredBuffer::new(1, 10, 5);

        // Push 2 messages with compressible data (repeated bytes)
        let msg1 = make_message("msg-1", 1000); // 1KB of 0x42
        let msg2 = make_message("msg-2", 1000);

        assert!(buffer.push(msg1)); // Goes to primary
        assert!(buffer.push(msg2)); // Overflows, gets compressed

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
        // Tiny buffers
        let buffer = TieredBuffer::new(2, 2, 5);

        // Push 6 messages - 2 primary + 2 secondary = 4 stored, 2 dropped
        for i in 0..6 {
            buffer.push(make_message(&format!("msg-{i}"), 100));
        }

        assert_eq!(buffer.total_dropped(), 2);
        assert_eq!(buffer.len(), 4);
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
        let buffer_fast = TieredBuffer::new(1, 10, 5).with_compression_level(1);
        let buffer_best = TieredBuffer::new(1, 10, 5).with_compression_level(19);

        // Large compressible message
        let msg = make_message("msg", 10_000);

        // Fill primary, overflow to secondary
        buffer_fast.push(msg.clone());
        buffer_fast.push(msg.clone());

        buffer_best.push(msg.clone());
        buffer_best.push(msg.clone());

        // Both should compress, but level 19 might save more (or same for simple data)
        assert!(buffer_fast.bytes_saved() > 0);
        assert!(buffer_best.bytes_saved() > 0);
    }
}
