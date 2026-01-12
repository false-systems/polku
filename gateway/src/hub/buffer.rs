//! Buffer abstraction for pluggable buffer strategies
//!
//! Provides the `HubBuffer` trait and `BufferStrategy` enum for configuring
//! how the Hub handles message buffering and overflow.

use crate::buffer_lockfree::LockFreeBuffer;
use crate::buffer_tiered::TieredBuffer;
use crate::message::Message;
use std::sync::Arc;

/// Trait for buffer implementations used by Hub
///
/// This allows swapping between different buffer strategies:
/// - `LockFreeBuffer`: Fast, simple, drops on overflow
/// - `TieredBuffer`: Compressed overflow for graceful degradation
pub trait HubBuffer: Send + Sync {
    /// Push a message into the buffer
    ///
    /// Returns `true` if stored, `false` if dropped.
    fn push(&self, msg: Message) -> bool;

    /// Drain up to `n` messages from the buffer
    fn drain(&self, n: usize) -> Vec<Message>;

    /// Current number of messages in the buffer
    fn len(&self) -> usize;

    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Buffer capacity
    fn capacity(&self) -> usize;

    /// Strategy name for logging
    fn strategy_name(&self) -> &'static str;
}

impl HubBuffer for LockFreeBuffer {
    fn push(&self, msg: Message) -> bool {
        LockFreeBuffer::push(self, msg)
    }

    fn drain(&self, n: usize) -> Vec<Message> {
        LockFreeBuffer::drain(self, n)
    }

    fn len(&self) -> usize {
        LockFreeBuffer::len(self)
    }

    fn capacity(&self) -> usize {
        LockFreeBuffer::capacity(self)
    }

    fn strategy_name(&self) -> &'static str {
        "standard"
    }
}

impl HubBuffer for TieredBuffer {
    fn push(&self, msg: Message) -> bool {
        TieredBuffer::push(self, msg)
    }

    fn drain(&self, n: usize) -> Vec<Message> {
        TieredBuffer::drain(self, n)
    }

    fn len(&self) -> usize {
        TieredBuffer::len(self)
    }

    fn capacity(&self) -> usize {
        TieredBuffer::capacity(self)
    }

    fn strategy_name(&self) -> &'static str {
        "tiered"
    }
}

/// Buffer strategy configuration for Hub
///
/// Controls how the Hub handles message buffering and overflow.
#[derive(Clone)]
pub enum BufferStrategy {
    /// Standard lock-free buffer (default)
    ///
    /// Fast and simple. When full, new messages are dropped.
    /// Best for: steady-state traffic, low-latency requirements.
    Standard {
        /// Buffer capacity in messages
        capacity: usize,
    },

    /// Tiered buffer with compressed overflow
    ///
    /// Primary buffer (fast) + secondary buffer (compressed).
    /// When primary fills, messages overflow to compressed secondary.
    /// Best for: traffic spikes, graceful degradation.
    Tiered {
        /// Primary (hot) buffer capacity
        primary_capacity: usize,
        /// Secondary (compressed overflow) buffer capacity
        secondary_capacity: usize,
        /// Compression level (1-22, default 3)
        compression_level: i32,
    },
}

impl BufferStrategy {
    /// Create a standard buffer strategy
    pub fn standard(capacity: usize) -> Self {
        Self::Standard { capacity }
    }

    /// Create a tiered buffer strategy with default compression
    pub fn tiered(primary_capacity: usize, secondary_capacity: usize) -> Self {
        Self::Tiered {
            primary_capacity,
            secondary_capacity,
            compression_level: 3,
        }
    }

    /// Create a tiered buffer with custom compression level
    pub fn tiered_with_compression(
        primary_capacity: usize,
        secondary_capacity: usize,
        compression_level: i32,
    ) -> Self {
        Self::Tiered {
            primary_capacity,
            secondary_capacity,
            compression_level: compression_level.clamp(1, 22),
        }
    }

    /// Build the buffer from this strategy
    pub(crate) fn build(&self) -> Arc<dyn HubBuffer> {
        match self {
            Self::Standard { capacity } => Arc::new(LockFreeBuffer::new(*capacity)),
            Self::Tiered {
                primary_capacity,
                secondary_capacity,
                compression_level,
            } => Arc::new(
                TieredBuffer::new(*primary_capacity, *secondary_capacity, 1)
                    .with_compression_level(*compression_level),
            ),
        }
    }

    /// Get total capacity for this strategy
    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        match self {
            Self::Standard { capacity } => *capacity,
            Self::Tiered {
                primary_capacity,
                secondary_capacity,
                ..
            } => primary_capacity + secondary_capacity,
        }
    }
}

impl Default for BufferStrategy {
    fn default() -> Self {
        Self::Standard { capacity: 10_000 }
    }
}
