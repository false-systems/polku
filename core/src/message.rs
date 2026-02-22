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

use crate::intern::InternedStr;
use crate::proto::Event;
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

/// Compact message identifier (16 bytes instead of 26-char string)
///
/// Stores ULID in binary form. Implements Display for string formatting
/// and PartialEq<str> for easy comparisons.
#[derive(Clone, Copy)]
pub struct MessageId {
    ulid: ulid::Ulid,
    /// Original string for non-ULID IDs (tests, legacy). Empty if generated.
    /// We store a hash to enable PartialEq<str> without storing the string.
    original_hash: u64,
}

impl MessageId {
    /// Generate a new unique ID
    #[inline]
    pub fn new() -> Self {
        Self {
            ulid: ulid::Ulid::new(),
            original_hash: 0,
        }
    }

    /// Create from a string (parses ULID or creates deterministic ID)
    pub fn from_string(s: &str) -> Self {
        // Try to parse as valid ULID first
        if let Ok(ulid) = ulid::Ulid::from_string(s) {
            return Self {
                ulid,
                original_hash: 0,
            };
        }

        // For arbitrary strings (tests), create deterministic ID from hash.
        // Use two independent hashes to fill all 16 bytes without repetition,
        // improving collision resistance over the previous [h, h] approach.
        let hash = Self::hash_string(s);
        let hash2 = Self::hash_string(s).wrapping_mul(0x517cc1b727220a95); // mix with golden ratio constant
        let mut ulid_bytes = [0u8; 16];
        ulid_bytes[..8].copy_from_slice(&hash.to_le_bytes());
        ulid_bytes[8..].copy_from_slice(&hash2.to_le_bytes());
        Self {
            ulid: ulid::Ulid::from_bytes(ulid_bytes),
            original_hash: hash,
        }
    }

    /// Get the ULID
    #[inline]
    pub fn as_ulid(&self) -> ulid::Ulid {
        self.ulid
    }

    /// Check if this was created from an arbitrary string (not a real ULID)
    #[inline]
    pub fn is_synthetic(&self) -> bool {
        self.original_hash != 0
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
        write!(f, "MessageId({})", self.ulid)
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ulid)
    }
}

impl PartialEq for MessageId {
    fn eq(&self, other: &Self) -> bool {
        self.ulid == other.ulid
    }
}

impl Eq for MessageId {}

/// Compare MessageId with a string.
///
/// For synthetic IDs (created via `from_string` with a non-ULID string),
/// comparison uses hash matching against the original string — NOT the
/// ULID string representation. This means `id == "test-id"` works but
/// `id == id.to_string()` does not for synthetic IDs. Real ULID-based
/// IDs compare by string representation as expected.
impl PartialEq<str> for MessageId {
    fn eq(&self, other: &str) -> bool {
        if self.original_hash != 0 {
            return self.original_hash == Self::hash_string(other);
        }
        self.ulid.to_string() == other
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
        id.ulid.to_string()
    }
}

/// The universal message envelope - protocol agnostic, zero-copy
///
/// # Example
///
/// ```
/// use bytes::Bytes;
/// use polku_core::Message;
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
///
/// Preserves classification fields in metadata:
/// - `severity` > 0 → `metadata["polku.severity"]`
/// - `outcome` > 0 → `metadata["polku.outcome"]`
impl From<Event> for Message {
    fn from(event: Event) -> Self {
        let mut metadata = event.metadata;

        // Preserve severity if non-default (0 = UNSPECIFIED)
        if event.severity != 0 {
            metadata.insert(
                crate::metadata_keys::SEVERITY.to_string(),
                event.severity.to_string(),
            );
        }

        // Preserve outcome if non-default (0 = UNSPECIFIED)
        if event.outcome != 0 {
            metadata.insert(
                crate::metadata_keys::OUTCOME.to_string(),
                event.outcome.to_string(),
            );
        }

        Self {
            id: MessageId::from_string(&event.id),
            timestamp: event.timestamp_unix_ns,
            source: event.source.into(),
            message_type: event.event_type.into(),
            metadata: if metadata.is_empty() {
                None
            } else {
                Some(Box::new(metadata))
            },
            payload: Bytes::from(event.payload),
            route_to: SmallVec::from_vec(event.route_to),
        }
    }
}

/// Convert from Message to proto Event
///
/// Reads classification fields back from metadata:
/// - `metadata["polku.severity"]` → `severity`
/// - `metadata["polku.outcome"]` → `outcome`
///
/// Consumed metadata keys are removed from the Event's metadata map.
impl From<Message> for Event {
    fn from(msg: Message) -> Self {
        let mut metadata = msg.metadata.map(|b| *b).unwrap_or_default();

        // Read severity from metadata (default 0 = UNSPECIFIED)
        let severity = metadata
            .remove(crate::metadata_keys::SEVERITY)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        // Read outcome from metadata (default 0 = UNSPECIFIED)
        let outcome = metadata
            .remove(crate::metadata_keys::OUTCOME)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        Self {
            id: msg.id.to_string(),
            timestamp_unix_ns: msg.timestamp,
            source: msg.source.into(),
            event_type: msg.message_type.into(),
            metadata,
            payload: msg.payload.to_vec(),
            route_to: msg.route_to.into_vec(),
            severity,
            outcome,
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
        let id2 = id1;
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

    // ==================================================================
    // Bug-revealing tests: Event<->Message round-trip
    // ==================================================================

    #[test]
    fn test_event_to_message_preserves_severity() {
        let event = Event {
            id: "test-1".into(),
            timestamp_unix_ns: 1000,
            source: "tapio".into(),
            event_type: "network.connection".into(),
            severity: 4, // SEVERITY_ERROR
            outcome: 1,  // OUTCOME_SUCCESS
            metadata: Default::default(),
            payload: vec![1, 2, 3],
            route_to: vec![],
        };

        let msg: Message = event.into();

        // Bug #1: severity is dropped during From<Event> for Message
        assert_eq!(
            msg.metadata().get(crate::metadata_keys::SEVERITY),
            Some(&"4".to_string()),
            "Event severity should be preserved in metadata"
        );
        assert_eq!(
            msg.metadata().get(crate::metadata_keys::OUTCOME),
            Some(&"1".to_string()),
            "Event outcome should be preserved in metadata"
        );
    }

    #[test]
    fn test_message_to_event_reads_severity_from_metadata() {
        let msg = Message::new("tapio", "network.connection", Bytes::from(vec![1, 2, 3]))
            .with_metadata(crate::metadata_keys::SEVERITY, "4")
            .with_metadata(crate::metadata_keys::OUTCOME, "2");

        let event: Event = msg.into();

        // Bug #2: From<Message> for Event hardcodes severity: 0, outcome: 0
        assert_eq!(
            event.severity, 4,
            "Event severity should be read from metadata"
        );
        assert_eq!(
            event.outcome, 2,
            "Event outcome should be read from metadata"
        );
    }

    #[test]
    fn test_severity_round_trip() {
        // Event -> Message -> Event should preserve severity/outcome
        let original = Event {
            id: "rt-1".into(),
            timestamp_unix_ns: 1000,
            source: "svc".into(),
            event_type: "evt".into(),
            severity: 5, // CRITICAL
            outcome: 3,  // TIMEOUT
            metadata: Default::default(),
            payload: vec![],
            route_to: vec![],
        };

        let msg: Message = original.clone().into();
        let round_tripped: Event = msg.into();

        assert_eq!(
            round_tripped.severity, original.severity,
            "Severity lost in round-trip"
        );
        assert_eq!(
            round_tripped.outcome, original.outcome,
            "Outcome lost in round-trip"
        );
    }

    #[test]
    fn test_unspecified_severity_not_stored() {
        // When severity/outcome are 0 (unspecified), don't clutter metadata
        let event = Event {
            id: "zero-1".into(),
            timestamp_unix_ns: 1000,
            source: "svc".into(),
            event_type: "evt".into(),
            severity: 0,
            outcome: 0,
            metadata: Default::default(),
            payload: vec![],
            route_to: vec![],
        };

        let msg: Message = event.into();

        // Unspecified (0) should not pollute metadata
        assert!(
            msg.metadata().get(crate::metadata_keys::SEVERITY).is_none(),
            "Unspecified severity should not be stored in metadata"
        );
        assert!(
            msg.metadata().get(crate::metadata_keys::OUTCOME).is_none(),
            "Unspecified outcome should not be stored in metadata"
        );
    }
}
