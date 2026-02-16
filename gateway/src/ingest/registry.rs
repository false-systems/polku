//! Ingestor Registry (Ingest Context)
//!
//! Maps source names to ingestors for O(1) lookup during ingestion.
//! Each source can have exactly one ingestor.
//!
//! # Invariants
//!
//! - Each source maps to exactly one ingestor
//! - Unknown sources use default ingestor (if set) or return error
//! - Sources are case-sensitive strings

use crate::error::PluginError;
use crate::ingest::{IngestContext, Ingestor};
use crate::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Registry for ingestors
///
/// Thread-safe container for managing ingestors. Typically populated
/// at startup and then used read-only during operation.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::ingest::{IngestorRegistry, JsonIngestor};
///
/// let mut registry = IngestorRegistry::new();
/// registry.add(Arc::new(JsonIngestor::new()));
///
/// // Now handles "json", "json-lines", "ndjson" sources automatically
/// ```
pub struct IngestorRegistry {
    /// Ingestors keyed by source name
    ingestors: HashMap<String, Arc<dyn Ingestor>>,
    /// Default ingestor for unknown sources (optional)
    default: Option<Arc<dyn Ingestor>>,
}

impl IngestorRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            ingestors: HashMap::new(),
            default: None,
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
    /// let mut registry = IngestorRegistry::new();
    /// registry.add(Arc::new(JsonIngestor::new()));
    /// // Now handles "json", "json-lines", "ndjson" sources automatically
    /// ```
    pub fn add(&mut self, ingestor: Arc<dyn Ingestor>) {
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
    /// declared sources. Prefer `add` for standard use.
    pub fn register(&mut self, source: impl Into<String>, ingestor: Arc<dyn Ingestor>) {
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
    /// let mut registry = IngestorRegistry::new();
    /// registry.set_default(Arc::new(JsonIngestor::new()));
    /// // Now unknown sources will be parsed as JSON
    /// ```
    pub fn set_default(&mut self, ingestor: Arc<dyn Ingestor>) {
        info!(ingestor = ingestor.name(), "Set default ingestor");
        self.default = Some(ingestor);
    }

    /// Check if an ingestor is registered for a source
    pub fn has(&self, source: &str) -> bool {
        self.ingestors.contains_key(source)
    }

    /// Get the number of source mappings (not unique ingestors)
    pub fn count(&self) -> usize {
        self.ingestors.len()
    }

    /// Ingest raw bytes using the appropriate ingestor
    ///
    /// Looks up the ingestor by source name and calls its ingest method.
    /// Falls back to the default ingestor if one is set.
    /// Returns an error if no ingestor is registered for the source and no default exists.
    pub fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        let ingestor = self
            .ingestors
            .get(ctx.source)
            .or(self.default.as_ref())
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

    /// Alias for `ingest` - kept for backward compatibility
    pub fn transform(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        self.ingest(ctx, data)
    }
}

impl Default for IngestorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ==========================================================================
    // Mock Ingestor for testing
    // ==========================================================================

    struct MockIngestor {
        name: &'static str,
        sources: &'static [&'static str],
    }

    impl MockIngestor {
        fn named(name: &'static str) -> Self {
            Self { name, sources: &[] }
        }

        fn with_sources(sources: &'static [&'static str]) -> Self {
            Self {
                name: "mock",
                sources,
            }
        }
    }

    impl Ingestor for MockIngestor {
        fn name(&self) -> &'static str {
            self.name
        }

        fn sources(&self) -> &'static [&'static str] {
            self.sources
        }

        fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
            Ok(vec![Message::with_id(
                format!("{}:{}", ctx.source, data.len()),
                0,
                self.name, // Use ingestor name as source for testing
                "test",
                Bytes::copy_from_slice(data),
            )])
        }
    }

    // ==========================================================================
    // Construction tests
    // ==========================================================================

    #[test]
    fn new_registry_has_no_ingestors() {
        let registry = IngestorRegistry::new();
        assert_eq!(registry.count(), 0);
        assert!(!registry.has("any-source"));
    }

    // ==========================================================================
    // Registration tests
    // ==========================================================================

    #[test]
    fn add_ingestor_registers_for_all_declared_sources() {
        let mut registry = IngestorRegistry::new();
        let ingestor = Arc::new(MockIngestor::with_sources(&["json", "ndjson"]));

        registry.add(ingestor);

        assert!(registry.has("json"));
        assert!(registry.has("ndjson"));
        assert_eq!(registry.count(), 2);
    }

    #[test]
    fn register_maps_specific_source_to_ingestor() {
        let mut registry = IngestorRegistry::new();
        let ingestor = Arc::new(MockIngestor::named("custom"));

        registry.register("my-source", ingestor);

        assert!(registry.has("my-source"));
    }

    #[test]
    fn later_registration_overwrites_earlier_for_same_source() {
        let mut registry = IngestorRegistry::new();
        let first = Arc::new(MockIngestor::named("first"));
        let second = Arc::new(MockIngestor::named("second"));

        registry.register("source", first);
        registry.register("source", second);

        let ctx = IngestContext {
            source: "source",
            cluster: "test",
            format: "test",
        };
        let events = registry.ingest(&ctx, b"data").unwrap();
        assert_eq!(events[0].source, "second");
    }

    // ==========================================================================
    // Default ingestor tests
    // ==========================================================================

    #[test]
    fn ingest_unknown_source_without_default_returns_error() {
        let registry = IngestorRegistry::new();
        let ctx = IngestContext {
            source: "unknown",
            cluster: "test",
            format: "test",
        };

        let result = registry.ingest(&ctx, b"data");

        assert!(result.is_err());
    }

    #[test]
    fn ingest_unknown_source_with_default_uses_default() {
        let mut registry = IngestorRegistry::new();
        registry.set_default(Arc::new(MockIngestor::named("default")));

        let ctx = IngestContext {
            source: "unknown",
            cluster: "test",
            format: "test",
        };
        let events = registry.ingest(&ctx, b"data").unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, "default");
    }

    #[test]
    fn specific_ingestor_takes_precedence_over_default() {
        let mut registry = IngestorRegistry::new();
        registry.set_default(Arc::new(MockIngestor::named("default")));
        registry.register("specific", Arc::new(MockIngestor::named("specific")));

        let ctx = IngestContext {
            source: "specific",
            cluster: "test",
            format: "test",
        };
        let events = registry.ingest(&ctx, b"data").unwrap();

        assert_eq!(events[0].source, "specific");
    }

    // ==========================================================================
    // Ingestion tests
    // ==========================================================================

    #[test]
    fn ingest_calls_correct_ingestor_for_source() {
        let mut registry = IngestorRegistry::new();
        registry.register("source-a", Arc::new(MockIngestor::named("ingestor-a")));
        registry.register("source-b", Arc::new(MockIngestor::named("ingestor-b")));

        let ctx_a = IngestContext {
            source: "source-a",
            cluster: "test",
            format: "test",
        };
        let ctx_b = IngestContext {
            source: "source-b",
            cluster: "test",
            format: "test",
        };

        let events_a = registry.ingest(&ctx_a, b"data").unwrap();
        let events_b = registry.ingest(&ctx_b, b"data").unwrap();

        assert_eq!(events_a[0].source, "ingestor-a");
        assert_eq!(events_b[0].source, "ingestor-b");
    }
}
