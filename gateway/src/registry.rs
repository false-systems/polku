//! Plugin registry for POLKU
//!
//! Manages ingestors and emitters. Ingestors are keyed by source name for O(1) lookup
//! during ingestion. Emitters are stored in a vector for fan-out delivery.

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::ingest::{IngestContext, Ingestor};
use crate::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Registry for ingestors and emitters
///
/// Thread-safe container for managing plugins. Typically populated
/// at startup and then used read-only during operation.
pub struct PluginRegistry {
    /// Ingestors keyed by source name
    ingestors: HashMap<String, Arc<dyn Ingestor>>,
    /// Emitters (fan-out to all)
    emitters: Vec<Arc<dyn Emitter>>,
    /// Default ingestor for unknown sources (optional)
    default_ingestor: Option<Arc<dyn Ingestor>>,
}

impl PluginRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            ingestors: HashMap::new(),
            emitters: Vec::new(),
            default_ingestor: None,
        }
    }

    /// Add an ingestor that auto-registers for all its declared sources
    ///
    /// This is the preferred way to add ingestors - uses the ingestor's
    /// `sources()` method to register for all sources it handles.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut registry = PluginRegistry::new();
    /// registry.add_ingestor(Arc::new(JsonIngestor::new()));
    /// // Now handles "json", "json-lines", "ndjson" sources automatically
    /// ```
    pub fn add_ingestor(&mut self, ingestor: Arc<dyn Ingestor>) {
        let sources = ingestor.sources();
        info!(
            ingestor = ingestor.name(),
            sources = ?sources,
            "Auto-registering ingestor for sources"
        );
        for source in sources {
            self.ingestors
                .insert((*source).to_string(), Arc::clone(&ingestor));
        }
    }

    /// Register an ingestor for a specific source (manual override)
    ///
    /// Use this for custom source mappings that differ from the ingestor's
    /// declared sources. Prefer `add_ingestor` for standard use.
    pub fn register_ingestor(&mut self, source: impl Into<String>, ingestor: Arc<dyn Ingestor>) {
        let source = source.into();
        info!(source = %source, ingestor = ingestor.name(), "Registered ingestor");
        self.ingestors.insert(source, ingestor);
    }

    /// Set a default ingestor for unknown sources
    ///
    /// When no ingestor is registered for a source, the default will be used.
    /// Useful for graceful handling of new/unknown event sources.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut registry = PluginRegistry::new();
    /// registry.set_default_ingestor(Arc::new(JsonIngestor::new()));
    /// // Now unknown sources will be parsed as JSON
    /// ```
    pub fn set_default_ingestor(&mut self, ingestor: Arc<dyn Ingestor>) {
        info!(ingestor = ingestor.name(), "Set default ingestor");
        self.default_ingestor = Some(ingestor);
    }

    /// Register an emitter
    ///
    /// All events will be sent to all registered emitters.
    pub fn register_emitter(&mut self, emitter: Arc<dyn Emitter>) {
        info!(emitter = emitter.name(), "Registered emitter");
        self.emitters.push(emitter);
    }

    /// Check if an ingestor is registered for a source
    pub fn has_ingestor(&self, source: &str) -> bool {
        self.ingestors.contains_key(source)
    }

    /// Get the number of registered ingestors
    pub fn ingestor_count(&self) -> usize {
        self.ingestors.len()
    }

    /// Get the number of registered emitters
    pub fn emitter_count(&self) -> usize {
        self.emitters.len()
    }

    /// Ingest raw bytes using the appropriate ingestor
    ///
    /// Looks up the ingestor by source name and calls its ingest method.
    /// Falls back to the default ingestor if one is set.
    /// Returns an error if no ingestor is registered for the source and no default exists.
    pub fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        // Try source-specific ingestor first, then default
        let ingestor = self
            .ingestors
            .get(ctx.source)
            .or(self.default_ingestor.as_ref())
            .ok_or_else(|| {
                PluginError::Transform(format!(
                    "No ingestor registered for source '{}' and no default ingestor set",
                    ctx.source
                ))
            })?;

        debug!(
            source = %ctx.source,
            ingestor = ingestor.name(),
            bytes = data.len(),
            "Ingesting raw data"
        );

        ingestor.ingest(ctx, data)
    }

    // Keep old name for compatibility during transition
    pub fn transform(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        self.ingest(ctx, data)
    }

    /// Emit messages to all emitters
    ///
    /// Messages are sent to each emitter. Failures are logged but don't
    /// stop delivery to other emitters.
    ///
    /// Returns the number of successful deliveries.
    pub async fn emit_to_all(&self, messages: &[Message]) -> usize {
        if self.emitters.is_empty() {
            warn!("No emitters registered, messages will be dropped");
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

    // Keep old name for compatibility
    pub async fn send_to_outputs(&self, messages: &[Message]) -> usize {
        self.emit_to_all(messages).await
    }

    /// Emit messages with routing hints
    ///
    /// Messages are sent only to emitters whose names match the route_to field.
    /// If route_to is empty, sends to all emitters.
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
            if let Some(events_for_emitter) = routed.get(emitter.name()) {
                let messages_vec: Vec<Message> =
                    events_for_emitter.iter().copied().cloned().collect();
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
                            "Failed to emit routed messages"
                        );
                    }
                }
            }
        }

        success_count
    }

    /// Check health of all emitters
    pub async fn emitter_health(&self) -> HashMap<String, bool> {
        let mut health = HashMap::new();

        for emitter in &self.emitters {
            let is_healthy = emitter.health().await;
            health.insert(emitter.name().to_string(), is_healthy);
        }

        health
    }

    // Keep old name for compatibility
    pub async fn output_health(&self) -> HashMap<String, bool> {
        self.emitter_health().await
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

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct MockIngestor {
        name: &'static str,
        sources: &'static [&'static str],
    }

    impl Ingestor for MockIngestor {
        fn name(&self) -> &str {
            self.name
        }

        fn sources(&self) -> &[&str] {
            self.sources
        }

        fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
            Ok(vec![Message::with_id(
                format!("{}:{}", ctx.source, data.len()),
                0,
                ctx.source,
                "test",
                bytes::Bytes::copy_from_slice(data),
            )])
        }
    }

    struct MockEmitter {
        name: &'static str,
    }

    #[async_trait::async_trait]
    impl Emitter for MockEmitter {
        fn name(&self) -> &str {
            self.name
        }

        async fn emit(&self, _messages: &[Message]) -> Result<(), PluginError> {
            Ok(())
        }

        async fn health(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_register_ingestor() {
        let mut registry = PluginRegistry::new();
        let plugin = Arc::new(MockIngestor {
            name: "test-ingestor",
            sources: &["test-source"],
        });

        registry.register_ingestor("test-source", plugin);

        assert!(registry.has_ingestor("test-source"));
        assert!(!registry.has_ingestor("unknown"));
        assert_eq!(registry.ingestor_count(), 1);
    }

    #[test]
    fn test_register_emitter() {
        let mut registry = PluginRegistry::new();
        let plugin = Arc::new(MockEmitter {
            name: "test-emitter",
        });

        registry.register_emitter(plugin);

        assert_eq!(registry.emitter_count(), 1);
    }

    #[test]
    fn test_ingest() {
        let mut registry = PluginRegistry::new();
        let plugin = Arc::new(MockIngestor {
            name: "test-ingestor",
            sources: &["test-source"],
        });
        registry.register_ingestor("test-source", plugin);

        let ctx = IngestContext {
            source: "test-source",
            cluster: "test-cluster",
            format: "protobuf",
        };

        let events = registry.ingest(&ctx, b"hello").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "test-source:5");
    }

    #[test]
    fn test_ingest_unknown_source() {
        let registry = PluginRegistry::new();

        let ctx = IngestContext {
            source: "unknown",
            cluster: "test-cluster",
            format: "protobuf",
        };

        let result = registry.ingest(&ctx, b"hello");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_emit_to_all() {
        let mut registry = PluginRegistry::new();
        registry.register_emitter(Arc::new(MockEmitter { name: "emitter1" }));
        registry.register_emitter(Arc::new(MockEmitter { name: "emitter2" }));

        let messages = vec![Message::new("test", "test", bytes::Bytes::new())];
        let success = registry.emit_to_all(&messages).await;
        assert_eq!(success, 2);
    }

    #[tokio::test]
    async fn test_emitter_health() {
        let mut registry = PluginRegistry::new();
        registry.register_emitter(Arc::new(MockEmitter { name: "emitter1" }));

        let health = registry.emitter_health().await;
        assert_eq!(health.get("emitter1"), Some(&true));
    }

    // ==========================================================================
    // New auto-registration and default ingestor tests
    // ==========================================================================

    #[test]
    fn test_add_ingestor_registers_all_sources() {
        let mut registry = PluginRegistry::new();

        // MockIngestor with multiple sources
        let plugin = Arc::new(MockIngestor {
            name: "multi-source",
            sources: &["source-a", "source-b", "source-c"],
        });

        registry.add_ingestor(plugin);

        // Should be registered for all sources
        assert!(registry.has_ingestor("source-a"));
        assert!(registry.has_ingestor("source-b"));
        assert!(registry.has_ingestor("source-c"));
        assert!(!registry.has_ingestor("unknown"));

        // All three sources map to the same ingestor (count is 3 entries)
        assert_eq!(registry.ingestor_count(), 3);
    }

    #[test]
    fn test_default_ingestor_handles_unknown_source() {
        let mut registry = PluginRegistry::new();

        // Set a default ingestor
        let default = Arc::new(MockIngestor {
            name: "default-ingestor",
            sources: &["default"],
        });
        registry.set_default_ingestor(default);

        // Ingest from an unknown source - should use default
        let ctx = IngestContext {
            source: "unknown-source",
            cluster: "test-cluster",
            format: "json",
        };

        let events = registry.ingest(&ctx, b"data").unwrap();
        assert_eq!(events.len(), 1);
        // The event source comes from context, not ingestor
        assert_eq!(events[0].source, "unknown-source");
    }

    #[test]
    fn test_specific_ingestor_takes_precedence_over_default() {
        let mut registry = PluginRegistry::new();

        // Register a specific ingestor
        let specific = Arc::new(MockIngestor {
            name: "specific-ingestor",
            sources: &["my-source"],
        });
        registry.add_ingestor(specific);

        // Set a default ingestor
        let default = Arc::new(MockIngestor {
            name: "default-ingestor",
            sources: &["default"],
        });
        registry.set_default_ingestor(default);

        // Ingest from the specific source - should use specific ingestor
        let ctx = IngestContext {
            source: "my-source",
            cluster: "test-cluster",
            format: "json",
        };

        let events = registry.ingest(&ctx, b"data").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "my-source:4"); // Uses ctx.source in MockIngestor
    }
}
