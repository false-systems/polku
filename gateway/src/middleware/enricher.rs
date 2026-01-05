//! Enricher middleware
//!
//! Adds metadata to messages from an external source.
//! Useful for adding context like user info, geo data, or feature flags.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for the enrichment function
pub type EnrichFn = Arc<
    dyn Fn(&Message) -> Pin<Box<dyn Future<Output = HashMap<String, String>> + Send>> + Send + Sync,
>;

/// Enricher middleware
///
/// Adds metadata to messages using a provided enrichment function.
/// The function is called for each message and can perform async operations
/// like database lookups or API calls.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::middleware::Enricher;
///
/// let enricher = Enricher::new(|msg| {
///     Box::pin(async move {
///         let mut meta = HashMap::new();
///         meta.insert("enriched".to_string(), "true".to_string());
///         meta
///     })
/// });
/// ```
pub struct Enricher {
    enrich_fn: EnrichFn,
}

impl Enricher {
    /// Create a new enricher with the given enrichment function
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(&Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HashMap<String, String>> + Send + 'static,
    {
        Self {
            enrich_fn: Arc::new(move |msg| Box::pin(f(msg))),
        }
    }

    /// Create an enricher that adds static metadata to all messages
    pub fn with_static(metadata: HashMap<String, String>) -> Self {
        Self::new(move |_| {
            let meta = metadata.clone();
            async move { meta }
        })
    }
}

#[async_trait]
impl Middleware for Enricher {
    fn name(&self) -> &'static str {
        "enricher"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        let additional = (self.enrich_fn)(&msg).await;
        for (key, value) in additional {
            msg.metadata_mut().insert(key, value);
        }
        Some(msg)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_enricher_adds_metadata() {
        let enricher = Enricher::new(|_msg| async {
            let mut meta = HashMap::new();
            meta.insert("enriched".to_string(), "true".to_string());
            meta
        });

        let msg = Message::new("test", "evt", Bytes::new());
        let result = enricher.process(msg).await.unwrap();

        assert_eq!(result.metadata().get("enriched"), Some(&"true".to_string()));
    }

    #[tokio::test]
    async fn test_enricher_with_static_metadata() {
        let mut static_meta = HashMap::new();
        static_meta.insert("env".to_string(), "production".to_string());
        static_meta.insert("version".to_string(), "1.0".to_string());

        let enricher = Enricher::with_static(static_meta);

        let msg = Message::new("test", "evt", Bytes::new());
        let result = enricher.process(msg).await.unwrap();

        assert_eq!(
            result.metadata().get("env"),
            Some(&"production".to_string())
        );
        assert_eq!(result.metadata().get("version"), Some(&"1.0".to_string()));
    }

    #[tokio::test]
    async fn test_enricher_uses_message_context() {
        let enricher = Enricher::new(|msg| {
            let source = msg.source.clone();
            async move {
                let mut meta = HashMap::new();
                meta.insert("source_upper".to_string(), source.to_uppercase());
                meta
            }
        });

        let msg = Message::new("my-service", "evt", Bytes::new());
        let result = enricher.process(msg).await.unwrap();

        assert_eq!(
            result.metadata().get("source_upper"),
            Some(&"MY-SERVICE".to_string())
        );
    }

    #[tokio::test]
    async fn test_enricher_preserves_existing_metadata() {
        let enricher = Enricher::new(|_| async {
            let mut meta = HashMap::new();
            meta.insert("new_key".to_string(), "new_value".to_string());
            meta
        });

        let msg = Message::new("test", "evt", Bytes::new())
            .with_metadata("existing".to_string(), "value".to_string());

        let result = enricher.process(msg).await.unwrap();

        assert_eq!(
            result.metadata().get("existing"),
            Some(&"value".to_string())
        );
        assert_eq!(
            result.metadata().get("new_key"),
            Some(&"new_value".to_string())
        );
    }

    #[tokio::test]
    async fn test_enricher_overwrites_on_conflict() {
        let enricher = Enricher::new(|_| async {
            let mut meta = HashMap::new();
            meta.insert("key".to_string(), "new".to_string());
            meta
        });

        let msg = Message::new("test", "evt", Bytes::new())
            .with_metadata("key".to_string(), "old".to_string());

        let result = enricher.process(msg).await.unwrap();

        // New value overwrites old
        assert_eq!(result.metadata().get("key"), Some(&"new".to_string()));
    }
}
