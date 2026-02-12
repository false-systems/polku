//! polku-core - Core types for POLKU protocol hub
//!
//! This crate provides the foundational types that are shared between
//! the POLKU gateway and external plugins (emitters, ingestors):
//!
//! - [`Message`] - the universal pipeline envelope (zero-copy, protocol-agnostic)
//! - [`Emitter`] trait - async interface for sending messages to destinations
//! - [`PluginError`] - error type for plugin operations
//! - [`Event`] - the proto-generated wire format (gRPC boundaries only)
//! - [`InternedStr`] - zero-cost string interning for high-frequency fields
//! - [`metadata_keys`] - reserved metadata key constants
//!
//! # Why this crate exists
//!
//! External emitter plugins (like `ahti-emitter`) need to implement the `Emitter`
//! trait and use the `Message` type. Without `polku-core`, they would depend on
//! `polku-gateway`, but `polku-gateway` might also want to optionally depend on
//! those emitters, creating a cyclic dependency.
//!
//! By extracting core types here, we break the cycle:
//!
//! ```text
//! polku-core ◄── polku-gateway
//!     ▲
//!     └────────── ahti-emitter (in false-systems-plugins)
//! ```
//!
//! Now `polku-gateway` can optionally depend on `ahti-emitter` without cycles.

#![deny(unsafe_code)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![warn(clippy::panic)]
#![warn(missing_docs)]

mod emit;
mod error;
/// String interning for zero-cost cloning of repeated strings
pub mod intern;
/// The universal message envelope
pub mod message;
/// Reserved metadata key constants for POLKU messages
pub mod metadata_keys;

// Proto types generated from polku/v1/event.proto
pub mod proto {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    #![allow(clippy::panic)]
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(missing_docs)]

    include!("proto/polku.event.v1.rs");
}

pub use emit::Emitter;
pub use error::PluginError;
pub use intern::InternedStr;
pub use message::{Message, MessageId, Metadata, Routes};
pub use proto::Event;

// Re-export typed event data types for convenience
pub use proto::{
    ContainerEventData, K8sEventData, KernelEventData, NetworkEventData, Outcome, ProcessEventData,
    ResourceEventData, Severity,
};

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // PluginError Tests
    // ==========================================================================

    #[test]
    fn test_plugin_error_init_display() {
        let err = PluginError::Init("connection refused".to_string());
        assert_eq!(err.to_string(), "initialization failed: connection refused");
    }

    #[test]
    fn test_plugin_error_transform_display() {
        let err = PluginError::Transform("invalid JSON".to_string());
        assert_eq!(err.to_string(), "transform failed: invalid JSON");
    }

    #[test]
    fn test_plugin_error_send_display() {
        let err = PluginError::Send("timeout".to_string());
        assert_eq!(err.to_string(), "send failed: timeout");
    }

    #[test]
    fn test_plugin_error_connection_display() {
        let err = PluginError::Connection("DNS lookup failed".to_string());
        assert_eq!(err.to_string(), "connection error: DNS lookup failed");
    }

    #[test]
    fn test_plugin_error_not_ready_display() {
        let err = PluginError::NotReady;
        assert_eq!(err.to_string(), "plugin not ready");
    }

    #[test]
    fn test_plugin_error_shutdown_display() {
        let err = PluginError::Shutdown("flush failed".to_string());
        assert_eq!(err.to_string(), "shutdown error: flush failed");
    }

    #[test]
    fn test_plugin_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PluginError>();
    }

    // ==========================================================================
    // Event Tests
    // ==========================================================================

    #[test]
    fn test_event_create_default() {
        let event = Event::default();
        assert!(event.id.is_empty());
        assert_eq!(event.timestamp_unix_ns, 0);
        assert!(event.source.is_empty());
        assert!(event.event_type.is_empty());
        assert!(event.metadata.is_empty());
        assert!(event.payload.is_empty());
        assert!(event.route_to.is_empty());
    }

    #[test]
    fn test_event_with_fields() {
        let mut event = Event::default();
        event.id = "01HTEST123456789".to_string();
        event.timestamp_unix_ns = 1704067200_000_000_000; // 2024-01-01 00:00:00 UTC
        event.source = "test-source".to_string();
        event.event_type = "test.event".to_string();
        event
            .metadata
            .insert("key1".to_string(), "value1".to_string());
        event.payload = vec![1, 2, 3, 4];
        event.route_to.push("output-a".to_string());

        assert_eq!(event.id, "01HTEST123456789");
        assert_eq!(event.timestamp_unix_ns, 1704067200_000_000_000);
        assert_eq!(event.source, "test-source");
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(event.payload, vec![1, 2, 3, 4]);
        assert_eq!(event.route_to, vec!["output-a".to_string()]);
    }

    #[test]
    fn test_event_clone() {
        let mut event = Event::default();
        event.id = "original".to_string();
        event.source = "source".to_string();

        let cloned = event.clone();
        assert_eq!(cloned.id, "original");
        assert_eq!(cloned.source, "source");
    }

    #[test]
    fn test_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Event>();
    }

    // ==========================================================================
    // Emitter Trait Tests
    // ==========================================================================

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test emitter that tracks calls for verification
    struct TestEmitter {
        name: &'static str,
        emit_count: AtomicU64,
        last_batch_size: AtomicU64,
        healthy: std::sync::atomic::AtomicBool,
        shutdown_called: std::sync::atomic::AtomicBool,
    }

    impl TestEmitter {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                emit_count: AtomicU64::new(0),
                last_batch_size: AtomicU64::new(0),
                healthy: std::sync::atomic::AtomicBool::new(true),
                shutdown_called: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn set_healthy(&self, healthy: bool) {
            self.healthy.store(healthy, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl Emitter for TestEmitter {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
            self.emit_count.fetch_add(1, Ordering::Relaxed);
            self.last_batch_size
                .store(messages.len() as u64, Ordering::Relaxed);
            Ok(())
        }

        async fn health(&self) -> bool {
            self.healthy.load(Ordering::Relaxed)
        }

        async fn shutdown(&self) -> Result<(), PluginError> {
            self.shutdown_called.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_emitter_name() {
        let emitter = TestEmitter::new("test-emitter");
        assert_eq!(emitter.name(), "test-emitter");
    }

    #[tokio::test]
    async fn test_emitter_emit_empty_batch() {
        let emitter = TestEmitter::new("test");
        let result = emitter.emit(&[]).await;
        assert!(result.is_ok());
        assert_eq!(emitter.emit_count.load(Ordering::Relaxed), 1);
        assert_eq!(emitter.last_batch_size.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_emitter_emit_batch() {
        let emitter = TestEmitter::new("test");

        let messages: Vec<Message> = (0..5)
            .map(|i| Message::new("test", format!("event-{}", i), bytes::Bytes::new()))
            .collect();

        let result = emitter.emit(&messages).await;
        assert!(result.is_ok());
        assert_eq!(emitter.emit_count.load(Ordering::Relaxed), 1);
        assert_eq!(emitter.last_batch_size.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn test_emitter_health_check() {
        let emitter = TestEmitter::new("test");

        assert!(emitter.health().await);

        emitter.set_healthy(false);
        assert!(!emitter.health().await);

        emitter.set_healthy(true);
        assert!(emitter.health().await);
    }

    #[tokio::test]
    async fn test_emitter_shutdown() {
        let emitter = TestEmitter::new("test");

        assert!(!emitter.shutdown_called.load(Ordering::Relaxed));

        let result = emitter.shutdown().await;
        assert!(result.is_ok());
        assert!(emitter.shutdown_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_emitter_is_object_safe() {
        // Verify trait is object-safe by using it as a trait object
        let emitter: Arc<dyn Emitter> = Arc::new(TestEmitter::new("boxed"));

        assert_eq!(emitter.name(), "boxed");
        assert!(emitter.health().await);

        let messages = vec![Message::new("test", "test", bytes::Bytes::new())];
        assert!(emitter.emit(&messages).await.is_ok());
    }

    /// Emitter that always fails - for testing error handling
    struct FailingEmitter;

    #[async_trait::async_trait]
    impl Emitter for FailingEmitter {
        fn name(&self) -> &'static str {
            "failing"
        }

        async fn emit(&self, _messages: &[Message]) -> Result<(), PluginError> {
            Err(PluginError::Send("always fails".to_string()))
        }

        async fn health(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_emitter_returns_error() {
        let emitter = FailingEmitter;

        let result = emitter
            .emit(&[Message::new("test", "test", bytes::Bytes::new())])
            .await;
        assert!(result.is_err());

        match result {
            Err(PluginError::Send(msg)) => assert_eq!(msg, "always fails"),
            _ => panic!("Expected PluginError::Send"),
        }
    }

    #[tokio::test]
    async fn test_emitter_default_shutdown_succeeds() {
        // Test that the default shutdown implementation returns Ok
        struct MinimalEmitter;

        #[async_trait::async_trait]
        impl Emitter for MinimalEmitter {
            fn name(&self) -> &'static str {
                "minimal"
            }
            async fn emit(&self, _messages: &[Message]) -> Result<(), PluginError> {
                Ok(())
            }
            async fn health(&self) -> bool {
                true
            }
            // Note: not overriding shutdown - uses default
        }

        let emitter = MinimalEmitter;
        assert!(emitter.shutdown().await.is_ok());
    }
}
