//! Generic Message type for POLKU
//!
//! The Message is the universal envelope that flows through the pipeline.
//! It's protocol-agnostic and uses `Bytes` for zero-copy payload handling.
//!
//! # Zero-Copy Design
//!
//! ```text
//! Input receives 10KB payload as Bytes
//!                     │
//!                     ▼
//! Message created with payload.clone()  ← Just increments refcount
//!                     │
//!     ┌───────────────┼───────────────┐
//!     ▼               ▼               ▼
//! Output A        Output B        Output C
//! (all share same underlying bytes - no copies!)
//! ```
//!
//! # String Interning
//!
//! `source` and `message_type` use interned strings for efficient cloning.
//! When the same string is used across multiple messages, it's stored once
//! and shared via a small integer key. This reduces memory usage and makes
//! `Message::clone()` much faster.

use crate::intern::InternedStr;
use bytes::Bytes;
use std::collections::HashMap;

/// The universal message envelope - protocol agnostic, zero-copy
///
/// # Example
///
/// ```
/// use bytes::Bytes;
/// use polku_gateway::message::Message;
///
/// let msg = Message::new("my-service", "user.created", Bytes::from(r#"{"id": 1}"#));
/// assert_eq!(msg.source, "my-service");
/// assert_eq!(msg.message_type, "user.created");
/// ```
#[derive(Debug, Clone)]
pub struct Message {
    /// Unique identifier (ULID by default)
    pub id: String,

    /// Unix timestamp in nanoseconds
    pub timestamp: i64,

    /// Origin identifier (e.g., service name, agent ID)
    ///
    /// Uses string interning - cloning is O(1) regardless of string length.
    pub source: InternedStr,

    /// User-defined message type (e.g., "user.created", "order.shipped")
    ///
    /// Uses string interning - cloning is O(1) regardless of string length.
    pub message_type: InternedStr,

    /// Headers and context (propagated through the pipeline)
    pub metadata: HashMap<String, String>,

    /// Opaque payload - zero-copy via Bytes
    ///
    /// POLKU doesn't interpret this. Your Input plugins deserialize it,
    /// your Output plugins serialize it for their destination.
    pub payload: Bytes,

    /// Routing hints for fan-out control
    ///
    /// If empty, message goes to all outputs.
    /// If specified, only matching outputs receive it.
    pub route_to: Vec<String>,
}

impl Message {
    /// Create a new Message with auto-generated ID and current timestamp
    ///
    /// # Arguments
    /// * `source` - Origin identifier
    /// * `message_type` - User-defined type
    /// * `payload` - Message payload (use `Bytes::from()` to convert)
    pub fn new(source: impl Into<InternedStr>, message_type: impl Into<InternedStr>, payload: Bytes) -> Self {
        Self {
            id: ulid::Ulid::new().to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            source: source.into(),
            message_type: message_type.into(),
            metadata: HashMap::new(),
            payload,
            route_to: Vec::new(),
        }
    }

    /// Create a Message with all fields specified
    pub fn with_id(
        id: impl Into<String>,
        timestamp: i64,
        source: impl Into<InternedStr>,
        message_type: impl Into<InternedStr>,
        payload: Bytes,
    ) -> Self {
        Self {
            id: id.into(),
            timestamp,
            source: source.into(),
            message_type: message_type.into(),
            metadata: HashMap::new(),
            payload,
            route_to: Vec::new(),
        }
    }

    /// Add metadata to the message
    ///
    /// # Example
    /// ```
    /// use bytes::Bytes;
    /// use polku_gateway::message::Message;
    ///
    /// let msg = Message::new("svc", "evt", Bytes::new())
    ///     .with_metadata("correlation_id", "abc-123")
    ///     .with_metadata("tenant", "acme");
    /// ```
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set routing targets
    ///
    /// # Example
    /// ```
    /// use bytes::Bytes;
    /// use polku_gateway::message::Message;
    ///
    /// // Only send to specific outputs
    /// let msg = Message::new("svc", "evt", Bytes::new())
    ///     .with_routes(vec!["kafka".into(), "metrics".into()]);
    /// ```
    pub fn with_routes(mut self, routes: Vec<String>) -> Self {
        self.route_to = routes;
        self
    }

    /// Check if message should be routed to a specific output
    ///
    /// Returns `true` if:
    /// - `route_to` is empty (broadcast to all), OR
    /// - `route_to` contains the output name
    pub fn should_route_to(&self, output_name: &str) -> bool {
        self.route_to.is_empty() || self.route_to.iter().any(|r| r == output_name)
    }

    /// Get payload as a string slice (if valid UTF-8)
    pub fn payload_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }

    /// Get payload length in bytes
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

/// Convert from proto Event to Message
impl From<crate::proto::Event> for Message {
    fn from(event: crate::proto::Event) -> Self {
        Self {
            id: event.id,
            timestamp: event.timestamp_unix_ns,
            source: event.source.into(),
            message_type: event.event_type.into(),
            metadata: event.metadata,
            payload: Bytes::from(event.payload),
            route_to: event.route_to,
        }
    }
}

/// Convert from Message to proto Event
impl From<Message> for crate::proto::Event {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id,
            timestamp_unix_ns: msg.timestamp,
            source: msg.source.into(),
            event_type: msg.message_type.into(),
            metadata: msg.metadata,
            payload: msg.payload.to_vec(),
            route_to: msg.route_to.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let payload = Bytes::from(r#"{"user_id": 123}"#);
        let msg = Message::new("user-service", "user.created", payload.clone());

        assert!(!msg.id.is_empty());
        assert!(msg.timestamp > 0);
        assert_eq!(msg.source, "user-service");
        assert_eq!(msg.message_type, "user.created");
        assert_eq!(msg.payload, payload);
        assert!(msg.route_to.is_empty());
    }

    #[test]
    fn test_message_with_metadata() {
        let msg = Message::new("svc", "evt", Bytes::new())
            .with_metadata("trace_id", "abc-123")
            .with_metadata("tenant", "acme");

        assert_eq!(msg.metadata.get("trace_id"), Some(&"abc-123".to_string()));
        assert_eq!(msg.metadata.get("tenant"), Some(&"acme".to_string()));
    }

    #[test]
    fn test_message_routing() {
        // Empty routes = broadcast to all
        let broadcast = Message::new("svc", "evt", Bytes::new());
        assert!(broadcast.should_route_to("any-output"));
        assert!(broadcast.should_route_to("another"));

        // Specific routes
        let targeted = Message::new("svc", "evt", Bytes::new())
            .with_routes(vec!["kafka".into(), "metrics".into()]);
        assert!(targeted.should_route_to("kafka"));
        assert!(targeted.should_route_to("metrics"));
        assert!(!targeted.should_route_to("stdout"));
    }

    #[test]
    fn test_zero_copy_clone() {
        let original = Bytes::from(vec![0u8; 10000]); // 10KB payload
        let msg = Message::new("svc", "evt", original.clone());

        // Clone the message
        let cloned = msg.clone();

        // Both should point to the same underlying data
        // (Bytes uses Arc internally, so this is a reference count increment)
        assert_eq!(msg.payload.as_ptr(), cloned.payload.as_ptr());
        assert_eq!(msg.payload.len(), cloned.payload.len());
    }

    #[test]
    fn test_payload_str() {
        let json = Message::new("svc", "evt", Bytes::from(r#"{"valid": "json"}"#));
        assert_eq!(json.payload_str(), Some(r#"{"valid": "json"}"#));

        let binary = Message::new("svc", "evt", Bytes::from(vec![0xFF, 0xFE]));
        assert!(binary.payload_str().is_none());
    }

    #[test]
    fn test_proto_conversion() {
        let msg = Message::new("svc", "user.created", Bytes::from("test"))
            .with_metadata("key", "value")
            .with_routes(vec!["out1".into()]);

        // Convert to proto
        let proto: crate::proto::Event = msg.clone().into();
        assert_eq!(proto.id, msg.id);
        assert_eq!(proto.source, "svc");
        assert_eq!(proto.event_type, "user.created");
        assert_eq!(proto.metadata.get("key"), Some(&"value".to_string()));

        // Convert back
        let back: Message = proto.into();
        assert_eq!(back.id, msg.id);
        assert_eq!(back.source, msg.source);
        assert_eq!(back.message_type, msg.message_type);
    }
}
