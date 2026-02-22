//! Emitter Registry (Emit Context)
//!
//! Stores emitters for fan-out delivery. All registered emitters
//! receive events (unless routing filters them).
//!
//! # Invariants
//!
//! - All emitters receive broadcasts (empty route_to)
//! - Routing filters by emitter name
//! - Order is preserved (first registered = first in list)

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Registry for emitters
///
/// Thread-safe container for managing emitters. Typically populated
/// at startup and then used read-only during operation.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::emit::{EmitterRegistry, StdoutEmitter};
///
/// let mut registry = EmitterRegistry::new();
/// registry.add(Arc::new(StdoutEmitter::new()));
/// ```
pub struct EmitterRegistry {
    /// Emitters (fan-out to all)
    emitters: Vec<Arc<dyn Emitter>>,
}

impl EmitterRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            emitters: Vec::new(),
        }
    }

    /// Add an emitter to the registry
    pub fn add(&mut self, emitter: Arc<dyn Emitter>) {
        info!(emitter = emitter.name(), "Registered emitter");
        self.emitters.push(emitter);
    }

    /// Get all emitters
    pub fn all(&self) -> &[Arc<dyn Emitter>] {
        &self.emitters
    }

    /// Find emitter by name
    pub fn by_name(&self, name: &str) -> Option<&Arc<dyn Emitter>> {
        self.emitters.iter().find(|e| e.name() == name)
    }

    /// Get the number of registered emitters
    pub fn count(&self) -> usize {
        self.emitters.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.emitters.is_empty()
    }

    /// Emit events to all emitters
    ///
    /// Events are sent to each emitter. Failures are logged but don't
    /// stop delivery to other emitters.
    ///
    /// Returns the number of successful deliveries.
    pub async fn emit_to_all(&self, messages: &[Message]) -> usize {
        if self.emitters.is_empty() {
            warn!("No emitters registered, events will be dropped");
            return 0;
        }

        let mut success_count = 0;

        for emitter in &self.emitters {
            match emitter.emit(messages).await {
                Ok(()) => {
                    debug!(
                        emitter = emitter.name(),
                        count = messages.len(),
                        "Messages emitted"
                    );
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        emitter = emitter.name(),
                        error = %e,
                        count = messages.len(),
                        "Failed to emit messages"
                    );
                }
            }
        }

        success_count
    }

    /// Emit messages with routing hints
    ///
    /// Messages are sent only to emitters whose names match the route_to field.
    /// If route_to is empty, sends to all emitters (broadcast).
    pub async fn emit_with_routing(&self, messages: &[Message]) -> usize {
        let mut all_emitters: Vec<&Message> = Vec::new();
        let mut routed: HashMap<&str, Vec<&Message>> = HashMap::new();

        for msg in messages {
            if msg.route_to.is_empty() {
                all_emitters.push(msg);
            } else {
                for route in &msg.route_to {
                    routed.entry(route.as_str()).or_default().push(msg);
                }
            }
        }

        let mut success_count = 0;

        if !all_emitters.is_empty() {
            let messages_vec: Vec<Message> = all_emitters.into_iter().cloned().collect();
            success_count += self.emit_to_all(&messages_vec).await;
        }

        for emitter in &self.emitters {
            if let Some(messages) = routed.get(emitter.name()) {
                let messages_vec: Vec<Message> = messages.iter().copied().cloned().collect();
                match emitter.emit(&messages_vec).await {
                    Ok(()) => {
                        debug!(
                            emitter = emitter.name(),
                            count = messages_vec.len(),
                            "Routed messages emitted"
                        );
                        success_count += 1;
                    }
                    Err(e) => {
                        error!(
                            emitter = emitter.name(),
                            error = %e,
                            count = messages_vec.len(),
                            "Failed to emit routed events"
                        );
                    }
                }
            }
        }

        success_count
    }

    /// Check health of all emitters
    pub async fn health(&self) -> HashMap<String, bool> {
        let mut health = HashMap::new();

        for emitter in &self.emitters {
            let is_healthy = emitter.health().await;
            health.insert(emitter.name().to_string(), is_healthy);
        }

        health
    }

    /// Graceful shutdown of all emitters
    pub async fn shutdown(&self) -> Result<(), PluginError> {
        info!("Shutting down {} emitters", self.emitters.len());

        for emitter in &self.emitters {
            if let Err(e) = emitter.shutdown().await {
                error!(emitter = emitter.name(), error = %e, "Error during emitter shutdown");
            }
        }

        Ok(())
    }
}

impl Default for EmitterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // ==========================================================================
    // Mock Emitter for testing
    // ==========================================================================

    struct MockEmitter {
        name: &'static str,
        healthy: bool,
        shutdown_called: AtomicBool,
        emit_count: AtomicUsize,
    }

    impl MockEmitter {
        fn named(name: &'static str) -> Self {
            Self {
                name,
                healthy: true,
                shutdown_called: AtomicBool::new(false),
                emit_count: AtomicUsize::new(0),
            }
        }

        fn healthy(name: &'static str) -> Self {
            Self {
                name,
                healthy: true,
                shutdown_called: AtomicBool::new(false),
                emit_count: AtomicUsize::new(0),
            }
        }

        fn unhealthy(name: &'static str) -> Self {
            Self {
                name,
                healthy: false,
                shutdown_called: AtomicBool::new(false),
                emit_count: AtomicUsize::new(0),
            }
        }

        fn was_shutdown(&self) -> bool {
            self.shutdown_called.load(Ordering::SeqCst)
        }

        fn emit_count(&self) -> usize {
            self.emit_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Emitter for MockEmitter {
        fn name(&self) -> &str {
            self.name
        }

        async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
            self.emit_count.fetch_add(messages.len(), Ordering::SeqCst);
            Ok(())
        }

        async fn health(&self) -> bool {
            self.healthy
        }

        async fn shutdown(&self) -> Result<(), PluginError> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    // ==========================================================================
    // Construction tests
    // ==========================================================================

    #[test]
    fn new_registry_has_no_emitters() {
        let registry = EmitterRegistry::new();
        assert_eq!(registry.count(), 0);
        assert!(registry.all().is_empty());
        assert!(registry.is_empty());
    }

    // ==========================================================================
    // Registration tests
    // ==========================================================================

    #[test]
    fn add_emitter_appends_to_list() {
        let mut registry = EmitterRegistry::new();
        registry.add(Arc::new(MockEmitter::named("first")));
        registry.add(Arc::new(MockEmitter::named("second")));

        assert_eq!(registry.count(), 2);
        assert!(!registry.is_empty());

        let names: Vec<_> = registry.all().iter().map(|e| e.name()).collect();
        assert_eq!(names, &["first", "second"]);
    }

    // ==========================================================================
    // Lookup tests
    // ==========================================================================

    #[test]
    fn by_name_returns_matching_emitter() {
        let mut registry = EmitterRegistry::new();
        registry.add(Arc::new(MockEmitter::named("kafka")));
        registry.add(Arc::new(MockEmitter::named("stdout")));

        let emitter = registry.by_name("kafka");
        assert!(emitter.is_some());
        assert_eq!(emitter.unwrap().name(), "kafka");
    }

    #[test]
    fn by_name_returns_none_for_unknown() {
        let registry = EmitterRegistry::new();
        assert!(registry.by_name("unknown").is_none());
    }

    // ==========================================================================
    // Health tests
    // ==========================================================================

    #[tokio::test]
    async fn health_returns_status_for_all_emitters() {
        let mut registry = EmitterRegistry::new();
        registry.add(Arc::new(MockEmitter::healthy("healthy-one")));
        registry.add(Arc::new(MockEmitter::unhealthy("unhealthy-one")));

        let health = registry.health().await;

        assert_eq!(health.get("healthy-one"), Some(&true));
        assert_eq!(health.get("unhealthy-one"), Some(&false));
    }

    // ==========================================================================
    // Shutdown tests
    // ==========================================================================

    #[tokio::test]
    async fn shutdown_calls_shutdown_on_all_emitters() {
        let emitter1 = Arc::new(MockEmitter::named("e1"));
        let emitter2 = Arc::new(MockEmitter::named("e2"));

        let mut registry = EmitterRegistry::new();
        registry.add(emitter1.clone());
        registry.add(emitter2.clone());

        registry.shutdown().await.unwrap();

        assert!(emitter1.was_shutdown());
        assert!(emitter2.was_shutdown());
    }

    // ==========================================================================
    // Emit tests
    // ==========================================================================

    #[tokio::test]
    async fn emit_to_all_sends_to_all_emitters() {
        let emitter1 = Arc::new(MockEmitter::named("e1"));
        let emitter2 = Arc::new(MockEmitter::named("e2"));

        let mut registry = EmitterRegistry::new();
        registry.add(emitter1.clone());
        registry.add(emitter2.clone());

        let messages = vec![Message::new("test", "test", Bytes::new())];

        let success = registry.emit_to_all(&messages).await;

        assert_eq!(success, 2);
        assert_eq!(emitter1.emit_count(), 1);
        assert_eq!(emitter2.emit_count(), 1);
    }

    #[tokio::test]
    async fn emit_to_all_with_no_emitters_returns_zero() {
        let registry = EmitterRegistry::new();

        let messages = vec![Message::new("test", "test", Bytes::new())];

        let success = registry.emit_to_all(&messages).await;
        assert_eq!(success, 0);
    }

    #[tokio::test]
    async fn emit_with_routing_sends_to_targeted_emitters() {
        let kafka = Arc::new(MockEmitter::named("kafka"));
        let stdout = Arc::new(MockEmitter::named("stdout"));

        let mut registry = EmitterRegistry::new();
        registry.add(kafka.clone());
        registry.add(stdout.clone());

        let messages = vec![
            Message::new("test", "to-kafka", Bytes::new()).with_routes(vec!["kafka".into()]),
            Message::new("test", "to-stdout", Bytes::new()).with_routes(vec!["stdout".into()]),
        ];

        registry.emit_with_routing(&messages).await;

        assert_eq!(kafka.emit_count(), 1);
        assert_eq!(stdout.emit_count(), 1);
    }

    #[tokio::test]
    async fn emit_with_routing_broadcasts_empty_route() {
        let kafka = Arc::new(MockEmitter::named("kafka"));
        let stdout = Arc::new(MockEmitter::named("stdout"));

        let mut registry = EmitterRegistry::new();
        registry.add(kafka.clone());
        registry.add(stdout.clone());

        let messages = vec![Message::new("test", "broadcast", Bytes::new())];

        registry.emit_with_routing(&messages).await;

        assert_eq!(kafka.emit_count(), 1);
        assert_eq!(stdout.emit_count(), 1);
    }
}
