//! Zero-copy shared message for high-performance fan-out
//!
//! When a message needs to go to multiple emitters, cloning is wasteful.
//! `SharedMessage` wraps `Message` in an `Arc`, so fan-out is just
//! incrementing a reference count - no data copying.
//!
//! # When to Use
//!
//! - **SharedMessage**: Fan-out to multiple destinations, retry scenarios
//! - **Message**: Single path through pipeline, simpler code
//!
//! # Performance
//!
//! ```text
//! Fan-out to 5 emitters:
//!   Message (clone):       5 string copies + 5 metadata copies
//!   SharedMessage (Arc):   5 atomic increments (nanoseconds)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::{Message, SharedMessage};
//!
//! let msg = Message::new("svc", "evt", Bytes::from("payload"));
//! let shared = SharedMessage::new(msg);
//!
//! // Fan-out: just pointer copies
//! let for_kafka = shared.clone();
//! let for_stdout = shared.clone();
//! let for_metrics = shared.clone();
//! ```

use crate::message::Message;
use std::ops::Deref;
use std::sync::Arc;

/// A zero-copy reference to a Message
///
/// Internally uses `Arc<Message>`, so cloning is O(1) - just an atomic
/// reference count increment. The underlying Message data is never copied.
#[derive(Debug, Clone)]
pub struct SharedMessage {
    inner: Arc<Message>,
}

impl SharedMessage {
    /// Create a new SharedMessage from an owned Message
    ///
    /// This is the only allocation - after this, all clones are free.
    #[inline]
    pub fn new(msg: Message) -> Self {
        Self {
            inner: Arc::new(msg),
        }
    }

    /// Create from an existing Arc<Message>
    #[inline]
    pub fn from_arc(arc: Arc<Message>) -> Self {
        Self { inner: arc }
    }

    /// Get the underlying Arc
    #[inline]
    pub fn into_arc(self) -> Arc<Message> {
        self.inner
    }

    /// Get reference count (useful for debugging/metrics)
    #[inline]
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Try to unwrap the inner Message if this is the only reference
    ///
    /// Returns `Ok(Message)` if ref_count == 1, otherwise `Err(self)`.
    /// Useful when you know you're the last consumer and want to avoid
    /// a final clone.
    #[inline]
    pub fn try_unwrap(self) -> Result<Message, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(msg) => Ok(msg),
            Err(arc) => Err(Self { inner: arc }),
        }
    }

    /// Make a mutable copy if needed (copy-on-write semantics)
    ///
    /// If this is the only reference, returns the inner Message.
    /// Otherwise, clones the Message data.
    #[inline]
    pub fn make_mut(&mut self) -> &mut Message {
        Arc::make_mut(&mut self.inner)
    }
}

/// Deref to Message for transparent access
impl Deref for SharedMessage {
    type Target = Message;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Convert owned Message to SharedMessage
impl From<Message> for SharedMessage {
    #[inline]
    fn from(msg: Message) -> Self {
        Self::new(msg)
    }
}

/// Convert Arc<Message> to SharedMessage
impl From<Arc<Message>> for SharedMessage {
    #[inline]
    fn from(arc: Arc<Message>) -> Self {
        Self::from_arc(arc)
    }
}

/// AsRef for compatibility with code expecting &Message
impl AsRef<Message> for SharedMessage {
    #[inline]
    fn as_ref(&self) -> &Message {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_zero_copy_clone() {
        let msg = Message::new("svc", "evt", Bytes::from(vec![0u8; 10_000]));
        let payload_ptr = msg.payload.as_ptr();

        let shared = SharedMessage::new(msg);

        // Clone 5 times (simulating fan-out)
        let clones: Vec<_> = (0..5).map(|_| shared.clone()).collect();

        // All point to the same underlying data
        for clone in &clones {
            assert_eq!(clone.payload.as_ptr(), payload_ptr);
        }

        // Reference count should be 6 (original + 5 clones)
        assert_eq!(shared.ref_count(), 6);
    }

    #[test]
    fn test_deref_access() {
        let msg = Message::new("my-service", "user.created", Bytes::from("{}"));
        let shared = SharedMessage::new(msg);

        // Can access Message fields directly via Deref
        assert_eq!(shared.source, "my-service");
        assert_eq!(shared.message_type, "user.created");
        assert_eq!(shared.payload_str(), Some("{}"));
    }

    #[test]
    fn test_try_unwrap() {
        let msg = Message::new("svc", "evt", Bytes::new());
        let shared = SharedMessage::new(msg);

        // Should succeed - only one reference
        let unwrapped = shared.try_unwrap().expect("should unwrap");
        assert_eq!(unwrapped.source, "svc");
    }

    #[test]
    fn test_try_unwrap_fails_with_multiple_refs() {
        let msg = Message::new("svc", "evt", Bytes::new());
        let shared = SharedMessage::new(msg);
        let _clone = shared.clone();

        // Should fail - multiple references
        let result = shared.try_unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_make_mut_cow() {
        let msg = Message::new("svc", "evt", Bytes::new());
        let mut shared = SharedMessage::new(msg);

        // No other references - should not clone
        {
            let inner = shared.make_mut();
            inner.source = "modified".into();
        }

        assert_eq!(shared.source, "modified");
    }

    #[test]
    fn test_from_conversions() {
        let msg = Message::new("svc", "evt", Bytes::new());

        // From Message
        let shared: SharedMessage = msg.into();
        assert_eq!(shared.source, "svc");

        // From Arc<Message>
        let arc = Arc::new(Message::new("arc-svc", "evt", Bytes::new()));
        let shared2: SharedMessage = arc.into();
        assert_eq!(shared2.source, "arc-svc");
    }
}
