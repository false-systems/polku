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
//!
//! # Binary Message ID
//!
//! The `id` field uses a compact 16-byte representation instead of a 26-char string.
//! This saves ~34 bytes per message and makes cloning O(1) (Copy).

use crate::emit::Event;
use crate::intern::InternedStr;
use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Type alias for route storage - inline up to 2 routes
pub type Routes = SmallVec<[String; 2]>;

/// Type alias for metadata storage - lazy allocation
pub type Metadata = Option<Box<HashMap<String, String>>>;

/// Helper to get metadata or empty map
#[inline]
fn metadata_ref(m: &Metadata) -> &HashMap<String, String> {
    static EMPTY: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    m.as_ref()
        .map(|b| b.as_ref())
        .unwrap_or_else(|| EMPTY.get_or_init(HashMap::new))
}

/// Compact message identifier that preserves original IDs
///
/// Stores ULID internally for efficient comparison and hashing, but preserves
/// the original string for non-ULID IDs (UUIDs, custom IDs) so they can be
/// returned unchanged when converting back to proto Events.
///
/// Note: Using non-ULID IDs incurs an additional heap allocation to store the
/// original string (`Box<str>`), and because of this extra field `MessageId`
/// is no longer `Copy` (it is `Clone` only). This is a trade-off to preserve
/// original identifiers exactly while keeping ULID-based IDs compact and fast.
#[derive(Clone)]
pub struct MessageId {
    ulid: ulid::Ulid,
    /// Original string for non-ULID IDs. None if this was a valid ULID or generated.
    original: Option<Box<str>>,
}

impl MessageId {
    /// Generate a new unique ID
    #[inline]
    pub fn new() -> Self {
        Self {
            ulid: ulid::Ulid::new(),
            original: None,
        }
    }

    /// Create from a string (parses ULID or preserves original)
    pub fn from_string(s: &str) -> Self {
        // Try to parse as valid ULID first
        if let Ok(ulid) = ulid::Ulid::from_string(s) {
            return Self {
                ulid,
                original: None,
            };
        }

        // For non-ULID strings (UUIDs, custom IDs), store original and create internal ULID
        let hash = Self::hash_string(s);
        let bytes = hash.to_le_bytes();
        // Create ULID from hash bytes (deterministic for dedup/comparison)
        let ulid_bytes: [u8; 16] = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        Self {
            ulid: ulid::Ulid::from_bytes(ulid_bytes),
            original: Some(s.into()),
        }
    }

    /// Get the ULID (for internal use)
    #[inline]
    pub fn as_ulid(&self) -> ulid::Ulid {
        self.ulid
    }

    /// Check if this was created from an arbitrary string (not a real ULID)
    #[inline]
    pub fn is_synthetic(&self) -> bool {
        self.original.is_some()
    }

    /// Get the original string representation
    /// Returns the original ID if non-ULID, otherwise the ULID string
    #[inline]
    pub fn as_str(&self) -> std::borrow::Cow<'_, str> {
        match &self.original {
            Some(orig) => std::borrow::Cow::Borrowed(orig.as_ref()),
            None => std::borrow::Cow::Owned(self.ulid.to_string()),
        }
    }

    /// Simple string hash (FNV-1a)
    fn hash_string(s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.original {
            Some(orig) => write!(f, "MessageId({})", orig),
            None => write!(f, "MessageId({})", self.ulid),
        }
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.original {
            Some(orig) => write!(f, "{}", orig),
            None => write!(f, "{}", self.ulid),
        }
    }
}

impl PartialEq for MessageId {
    fn eq(&self, other: &Self) -> bool {
        self.ulid == other.ulid
    }
}

impl Eq for MessageId {}

impl PartialEq<str> for MessageId {
    fn eq(&self, other: &str) -> bool {
        // Compare by original string if present, otherwise by ULID without allocating
        match &self.original {
            Some(orig) => orig.as_ref() == other,
            None => {
                if let Ok(other_ulid) = other.parse() {
                    self.ulid == other_ulid
                } else {
                    false
                }
            }
        }
    }
}

impl PartialEq<&str> for MessageId {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<String> for MessageId {
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

impl Hash for MessageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ulid.0.hash(state);
    }
}

impl From<&str> for MessageId {
    fn from(s: &str) -> Self {
        Self::from_string(s)
    }
}

impl From<String> for MessageId {
    fn from(s: String) -> Self {
        Self::from_string(&s)
    }
}

impl From<MessageId> for String {
    fn from(id: MessageId) -> Self {
        match id.original {
            Some(orig) => orig.into(),
            None => id.ulid.to_string(),
        }
    }
}

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
    /// Unique identifier (binary ULID - 16 bytes, Copy)
    pub id: MessageId,

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
    ///
    /// Lazily allocated - None when empty to save 48 bytes per message.
    pub metadata: Metadata,

    /// Opaque payload - zero-copy via Bytes
    ///
    /// POLKU doesn't interpret this. Your Input plugins deserialize it,
    /// your Output plugins serialize it for their destination.
    pub payload: Bytes,

    /// Routing hints for fan-out control
    ///
    /// If empty, message goes to all outputs.
    /// If specified, only matching outputs receive it.
    /// Uses SmallVec to inline up to 2 routes (most common case).
    pub route_to: Routes,
}

impl Message {
    /// Create a new Message with auto-generated ID and current timestamp
    ///
    /// # Arguments
    /// * `source` - Origin identifier
    /// * `message_type` - User-defined type
    /// * `payload` - Message payload (use `Bytes::from()` to convert)
    pub fn new(
        source: impl Into<InternedStr>,
        message_type: impl Into<InternedStr>,
        payload: Bytes,
    ) -> Self {
        Self {
            id: MessageId::new(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            source: source.into(),
            message_type: message_type.into(),
            metadata: None,
            payload,
            route_to: SmallVec::new(),
        }
    }

    /// Create a Message with all fields specified
    pub fn with_id(
        id: impl Into<MessageId>,
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
            metadata: None,
            payload,
            route_to: SmallVec::new(),
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
        self.metadata
            .get_or_insert_with(|| Box::new(HashMap::new()))
            .insert(key.into(), value.into());
        self
    }

    /// Get metadata reference (returns empty map if None)
    #[inline]
    pub fn metadata(&self) -> &HashMap<String, String> {
        metadata_ref(&self.metadata)
    }

    /// Get mutable metadata, allocating if needed
    #[inline]
    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        self.metadata
            .get_or_insert_with(|| Box::new(HashMap::new()))
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
    pub fn with_routes(mut self, routes: impl Into<Routes>) -> Self {
        self.route_to = routes.into();
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
impl From<Event> for Message {
    fn from(event: Event) -> Self {
        Self {
            id: MessageId::from_string(&event.id),
            timestamp: event.timestamp_unix_ns,
            source: event.source.into(),
            message_type: event.event_type.into(),
            metadata: if event.metadata.is_empty() {
                None
            } else {
                Some(Box::new(event.metadata))
            },
            payload: Bytes::from(event.payload),
            route_to: SmallVec::from_vec(event.route_to),
        }
    }
}

/// Convert from Message to proto Event
impl From<Message> for Event {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id.to_string(),
            timestamp_unix_ns: msg.timestamp,
            source: msg.source.into(),
            event_type: msg.message_type.into(),
            metadata: msg.metadata.map(|b| *b).unwrap_or_default(),
            payload: msg.payload.to_vec(),
            route_to: msg.route_to.into_vec(),
            // New typed fields - defaults for legacy messages
            severity: 0, // SEVERITY_UNSPECIFIED
            outcome: 0,  // OUTCOME_UNSPECIFIED
            data: None,  // No typed data for messages converted from internal format
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

        // MessageId is always valid (16 bytes)
        assert!(!msg.id.to_string().is_empty());
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

        assert_eq!(msg.metadata().get("trace_id"), Some(&"abc-123".to_string()));
        assert_eq!(msg.metadata().get("tenant"), Some(&"acme".to_string()));
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
        let proto: Event = msg.clone().into();
        assert_eq!(msg.id.to_string(), proto.id);
        assert_eq!(proto.source, "svc");
        assert_eq!(proto.event_type, "user.created");
        assert_eq!(proto.metadata.get("key"), Some(&"value".to_string()));

        // Convert back
        let back: Message = proto.into();
        assert_eq!(back.id, msg.id);
        assert_eq!(back.source, msg.source);
        assert_eq!(back.message_type, msg.message_type);
    }

    #[test]
    fn test_message_size() {
        // Track Message size for memory optimization
        let message_size = std::mem::size_of::<Message>();

        // Memory layout optimizations applied:
        // - MessageId: binary 16-byte ULID + hash (32 bytes, Copy)
        // - source/type: InternedStr (4 bytes each, Copy)
        // - metadata: Option<Box<HashMap>> (8 bytes, lazy allocation)
        // - route_to: SmallVec<[String; 2]> (64 bytes, inline for 0-2 routes)
        //
        // Key savings vs old layout:
        // - Metadata heap: 0 bytes when empty (vs 48 byte HashMap overhead)
        // - Routes heap: 0 bytes for ≤2 routes (vs Vec allocation)
        // - Clone cost: O(1) for id/source/type (vs String clones)

        assert!(
            message_size <= 160,
            "Message size {} exceeds 160 byte limit",
            message_size
        );
    }

    #[test]
    fn test_message_id_comparison() {
        // Real ULIDs can be compared
        let id1 = MessageId::new();
        let id2 = id1.clone();
        assert_eq!(id1, id2);

        // Synthetic IDs from strings
        let id3 = MessageId::from_string("test-id");
        let id4 = MessageId::from_string("test-id");
        assert_eq!(id3, id4);
        assert_eq!(id3, "test-id");

        // Different strings = different IDs
        let id5 = MessageId::from_string("other-id");
        assert_ne!(id3, id5);
    }

    #[test]
    fn test_message_id_preserves_original() {
        // UUIDs should be preserved
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let id = MessageId::from_string(uuid);
        eprintln!("UUID in:  '{}'", uuid);
        eprintln!("UUID out: '{}'", id.to_string());
        assert_eq!(id.to_string(), uuid, "UUID should be preserved");
        assert!(id.is_synthetic());

        // Verify deterministic ULID generation - same string = same MessageId
        let id_copy = MessageId::from_string(uuid);
        assert_eq!(id, id_copy);
        assert_eq!(id.as_ulid(), id_copy.as_ulid());

        // Custom IDs should be preserved
        let custom = "my-custom-event-id-12345";
        let id2 = MessageId::from_string(custom);
        assert_eq!(id2.to_string(), custom);

        // Valid ULIDs should work normally
        let ulid_str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id3 = MessageId::from_string(ulid_str);
        assert_eq!(id3.to_string(), ulid_str);
        assert!(!id3.is_synthetic());

        // Generated IDs return ULID format
        let id4 = MessageId::new();
        assert!(!id4.is_synthetic());
        assert_eq!(id4.to_string().len(), 26); // ULID is 26 chars
    }

    #[test]
    fn test_proto_event_id_round_trip() {
        // Create message with UUID
        let uuid = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let mut event = Event::default();
        event.id = uuid.to_string();
        event.source = "test".to_string();
        event.event_type = "test.event".to_string();

        // Convert to Message and back
        let msg: Message = event.into();
        let event_out: Event = msg.into();

        // ID should be preserved!
        assert_eq!(event_out.id, uuid, "UUID should survive Message round-trip");
    }
}
