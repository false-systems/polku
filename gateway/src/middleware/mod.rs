//! Middleware system for POLKU
//!
//! Middleware processes messages as they flow through the pipeline.
//! Each middleware can transform, filter, or route messages.
//!
//! # Message Flow
//!
//! ```text
//! Input ──► Middleware Chain ──► Buffer ──► Outputs
//!              │
//!              ├─► Transform (modify payload/metadata)
//!              ├─► Filter (drop based on criteria)
//!              ├─► Route (set route_to targets)
//!              └─► Enrich (add context)
//! ```
//!
//! # Example
//!
//! ```ignore
//! struct LoggingMiddleware;
//!
//! #[async_trait]
//! impl Middleware for LoggingMiddleware {
//!     fn name(&self) -> &'static str { "logging" }
//!
//!     async fn process(&self, msg: Message) -> Option<Message> {
//!         tracing::info!(id = %msg.id, "Processing message");
//!         Some(msg)  // Pass through
//!     }
//! }
//! ```

use crate::message::Message;
use async_trait::async_trait;

/// Middleware trait for message processing
///
/// Middleware is applied to messages before they enter the buffer.
/// Chain multiple middleware for complex processing pipelines.
///
/// # Return Value
///
/// - `Some(message)` - Pass the message through (possibly modified)
/// - `None` - Drop/filter the message
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Middleware name for identification and logging
    fn name(&self) -> &'static str;

    /// Process a message
    ///
    /// # Arguments
    /// * `msg` - The message to process
    ///
    /// # Returns
    /// - `Some(Message)` to continue processing
    /// - `None` to drop the message
    async fn process(&self, msg: Message) -> Option<Message>;
}

/// A middleware chain that processes messages through multiple middleware in order
pub struct MiddlewareChain {
    middlewares: Vec<Box<dyn Middleware>>,
}

impl MiddlewareChain {
    /// Create an empty middleware chain
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Add a middleware to the chain
    pub fn add<M: Middleware + 'static>(&mut self, middleware: M) {
        self.middlewares.push(Box::new(middleware));
    }

    /// Process a message through all middleware in order
    ///
    /// Returns `None` if any middleware filters the message.
    pub async fn process(&self, mut msg: Message) -> Option<Message> {
        for mw in &self.middlewares {
            msg = mw.process(msg).await?;
        }
        Some(msg)
    }

    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    /// Get number of middleware in the chain
    pub fn len(&self) -> usize {
        self.middlewares.len()
    }
}

impl Default for MiddlewareChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Pass-through middleware that does nothing (useful for testing)
pub struct PassThrough;

#[async_trait]
impl Middleware for PassThrough {
    fn name(&self) -> &'static str {
        "passthrough"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        Some(msg)
    }
}

/// Filter middleware that drops messages based on a predicate
///
/// # Example
///
/// ```ignore
/// let filter = Filter::new(|msg| msg.message_type.starts_with("important."));
/// ```
pub struct Filter<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> Filter<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    /// Create a filter with the given predicate
    ///
    /// Messages that return `true` are kept, `false` are dropped.
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

#[async_trait]
impl<F> Middleware for Filter<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    fn name(&self) -> &'static str {
        "filter"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        if (self.predicate)(&msg) {
            Some(msg)
        } else {
            None
        }
    }
}

/// Transform middleware that modifies messages
///
/// # Example
///
/// ```ignore
/// let transform = Transform::new(|mut msg| {
///     msg.metadata.insert("processed".into(), "true".into());
///     msg
/// });
/// ```
pub struct Transform<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    transform_fn: F,
}

impl<F> Transform<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    /// Create a transform with the given function
    pub fn new(transform_fn: F) -> Self {
        Self { transform_fn }
    }
}

#[async_trait]
impl<F> Middleware for Transform<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    fn name(&self) -> &'static str {
        "transform"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        Some((self.transform_fn)(msg))
    }
}

mod router;
pub use router::Router;
mod rate_limiter;
pub use rate_limiter::RateLimiter;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_passthrough() {
        let mw = PassThrough;
        let msg = Message::new("test", "evt", Bytes::new());
        let id = msg.id.clone();

        let result = mw.process(msg).await;
        assert!(result.is_some());
        assert_eq!(result.expect("should have message").id, id);
    }

    #[tokio::test]
    async fn test_filter_keep() {
        let filter = Filter::new(|msg: &Message| msg.message_type == "keep");
        let msg = Message::new("test", "keep", Bytes::new());

        let result = filter.process(msg).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_filter_drop() {
        let filter = Filter::new(|msg: &Message| msg.message_type == "keep");
        let msg = Message::new("test", "drop_me", Bytes::new());

        let result = filter.process(msg).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_transform() {
        let transform = Transform::new(|mut msg: Message| {
            msg.metadata.insert("transformed".into(), "yes".into());
            msg
        });

        let msg = Message::new("test", "evt", Bytes::new());
        let result = transform.process(msg).await;

        assert!(result.is_some());
        let msg = result.expect("should have message");
        assert_eq!(msg.metadata.get("transformed"), Some(&"yes".to_string()));
    }

    #[tokio::test]
    async fn test_middleware_chain() {
        let mut chain = MiddlewareChain::new();

        // First: add metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata.insert("step1".into(), "done".into());
            msg
        }));

        // Second: filter (keep all)
        chain.add(Filter::new(|_: &Message| true));

        // Third: add more metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata.insert("step2".into(), "done".into());
            msg
        }));

        let msg = Message::new("test", "evt", Bytes::new());
        let result = chain.process(msg).await;

        assert!(result.is_some());
        let msg = result.expect("should have message");
        assert_eq!(msg.metadata.get("step1"), Some(&"done".to_string()));
        assert_eq!(msg.metadata.get("step2"), Some(&"done".to_string()));
    }

    #[tokio::test]
    async fn test_middleware_chain_filter() {
        let mut chain = MiddlewareChain::new();

        // First: add metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata.insert("step1".into(), "done".into());
            msg
        }));

        // Second: filter (drop all)
        chain.add(Filter::new(|_: &Message| false));

        // Third: should never run
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata.insert("step2".into(), "done".into());
            msg
        }));

        let msg = Message::new("test", "evt", Bytes::new());
        let result = chain.process(msg).await;

        assert!(result.is_none());
    }
}
