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
use crate::metrics::Metrics;
use async_trait::async_trait;
use std::fmt::Write;

/// Middleware trait for message processing
///
/// Middleware is applied to messages before they enter the buffer.
/// Chain multiple middleware for complex processing pipelines.
///
/// # Return Value
///
/// - `Some(message)` - Pass the message through (possibly modified)
/// - `None` - Drop/filter the message
///
/// # Lifecycle
///
/// Stateful middleware (like Aggregator) should override `flush()` to return
/// any pending messages before shutdown. Call `flush()` on all middleware
/// before dropping to avoid data loss.
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

    /// Flush any pending messages
    ///
    /// Stateful middleware (e.g., Aggregator) should override this to return
    /// buffered messages. Default implementation returns None (no pending messages).
    ///
    /// # Returns
    /// - `Some(Message)` if there were pending messages to flush
    /// - `None` if no pending messages
    fn flush(&self) -> Option<Message> {
        None
    }
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
    ///
    /// When `metadata["polku.trace"] == "true"`, a JSON trace is recorded in
    /// `metadata["_polku.trace"]` with an entry per middleware showing action
    /// and duration in microseconds.
    pub async fn process(&self, mut msg: Message) -> Option<Message> {
        let tracing_enabled = msg
            .metadata()
            .get(polku_core::metadata_keys::TRACE_ENABLED)
            .is_some_and(|v| v == "true");
        let metrics = Metrics::get();

        if !tracing_enabled && metrics.is_none() {
            // Fast path: no tracing, no metrics — zero overhead
            for mw in &self.middlewares {
                msg = mw.process(msg).await?;
            }
            return Some(msg);
        }

        // Instrumented path: record per-middleware timing
        let mut trace = if tracing_enabled {
            Some(String::from("["))
        } else {
            None
        };

        for (i, mw) in self.middlewares.iter().enumerate() {
            let start = std::time::Instant::now();
            match mw.process(msg).await {
                Some(m) => {
                    let elapsed = start.elapsed();
                    if let Some(metrics) = metrics {
                        metrics.record_middleware(mw.name(), elapsed, "passed");
                    }
                    if let Some(ref mut trace) = trace {
                        if i > 0 {
                            trace.push(',');
                        }
                        let _ = write!(
                            trace,
                            r#"{{"mw":"{}","action":"passed","us":{}}}"#,
                            mw.name(),
                            elapsed.as_micros()
                        );
                    }
                    msg = m;
                }
                None => {
                    let elapsed = start.elapsed();
                    if let Some(metrics) = metrics {
                        metrics.record_middleware(mw.name(), elapsed, "filtered");
                    }
                    if let Some(ref mut trace) = trace {
                        if i > 0 {
                            trace.push(',');
                        }
                        let _ = write!(
                            trace,
                            r#"{{"mw":"{}","action":"filtered","us":{}}}"#,
                            mw.name(),
                            elapsed.as_micros()
                        );
                        trace.push(']');
                        tracing::debug!(
                            trace = %trace,
                            middleware = mw.name(),
                            "Message filtered (trace attached to log)"
                        );
                    }
                    return None;
                }
            }
        }

        if let Some(mut trace) = trace {
            trace.push(']');
            msg.metadata_mut()
                .insert(polku_core::metadata_keys::TRACE_LOG.to_string(), trace);
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

    /// Get names of all middleware in chain order
    pub fn names(&self) -> Vec<&'static str> {
        self.middlewares.iter().map(|mw| mw.name()).collect()
    }

    /// Flush all middleware in the chain
    ///
    /// Calls `flush()` on each middleware and collects any pending messages.
    /// Use this before shutdown to avoid losing buffered messages.
    ///
    /// # Returns
    /// A vector of messages flushed from all middleware (may be empty).
    pub fn flush(&self) -> Vec<Message> {
        self.middlewares
            .iter()
            .filter_map(|mw| mw.flush())
            .collect()
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
mod token_bucket;
pub use rate_limiter::RateLimiter;
mod dedup;
pub use dedup::Deduplicator;
mod sampler;
pub use sampler::Sampler;
mod throttle;
pub use throttle::Throttle;
mod enricher;
pub use enricher::Enricher;
mod validator;
pub use validator::{InvalidAction, ValidationResult, Validator};
mod aggregator;
pub use aggregator::{AggregateStrategy, Aggregator};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_passthrough() {
        let mw = PassThrough;
        let msg = Message::new("test", "evt", Bytes::new());
        let id = msg.id; // MessageId is Copy

        let result = mw.process(msg).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, id);
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
            msg.metadata_mut()
                .insert("transformed".into(), "yes".into());
            msg
        });

        let msg = Message::new("test", "evt", Bytes::new());
        let result = transform.process(msg).await;

        assert!(result.is_some());
        let msg = result.expect("should have message");
        assert_eq!(msg.metadata().get("transformed"), Some(&"yes".to_string()));
    }

    #[tokio::test]
    async fn test_middleware_chain() {
        let mut chain = MiddlewareChain::new();

        // First: add metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("step1".into(), "done".into());
            msg
        }));

        // Second: filter (keep all)
        chain.add(Filter::new(|_: &Message| true));

        // Third: add more metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("step2".into(), "done".into());
            msg
        }));

        let msg = Message::new("test", "evt", Bytes::new());
        let result = chain.process(msg).await;

        assert!(result.is_some());
        let msg = result.expect("should have message");
        assert_eq!(msg.metadata().get("step1"), Some(&"done".to_string()));
        assert_eq!(msg.metadata().get("step2"), Some(&"done".to_string()));
    }

    #[tokio::test]
    async fn test_middleware_chain_filter() {
        let mut chain = MiddlewareChain::new();

        // First: add metadata
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("step1".into(), "done".into());
            msg
        }));

        // Second: filter (drop all)
        chain.add(Filter::new(|_: &Message| false));

        // Third: should never run
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("step2".into(), "done".into());
            msg
        }));

        let msg = Message::new("test", "evt", Bytes::new());
        let result = chain.process(msg).await;

        assert!(result.is_none());
    }

    // ======================================================================
    // Processing Trace Tests
    // ======================================================================

    #[tokio::test]
    async fn test_trace_not_recorded_by_default() {
        let mut chain = MiddlewareChain::new();
        chain.add(PassThrough);

        let msg = Message::new("test", "evt", Bytes::new());
        let result = chain.process(msg).await.unwrap();

        assert!(
            result
                .metadata()
                .get(polku_core::metadata_keys::TRACE_LOG)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_trace_recorded_when_enabled() {
        let mut chain = MiddlewareChain::new();
        chain.add(PassThrough);
        chain.add(Transform::new(|mut msg: Message| {
            msg.metadata_mut().insert("x".into(), "y".into());
            msg
        }));

        let msg = Message::new("test", "evt", Bytes::new())
            .with_metadata(polku_core::metadata_keys::TRACE_ENABLED, "true");

        let result = chain.process(msg).await.unwrap();
        let trace = result
            .metadata()
            .get(polku_core::metadata_keys::TRACE_LOG)
            .expect("trace should be present");

        // Parse as JSON to validate structure
        let entries: Vec<serde_json::Value> = serde_json::from_str(trace).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["mw"], "passthrough");
        assert_eq!(entries[0]["action"], "passed");
        assert!(entries[0]["us"].is_number());
        assert_eq!(entries[1]["mw"], "transform");
        assert_eq!(entries[1]["action"], "passed");
    }

    #[tokio::test]
    async fn test_trace_records_filter_action() {
        let mut chain = MiddlewareChain::new();
        chain.add(PassThrough);
        chain.add(Filter::new(|_: &Message| false)); // drops message
        chain.add(PassThrough); // should not appear

        let msg = Message::new("test", "evt", Bytes::new())
            .with_metadata(polku_core::metadata_keys::TRACE_ENABLED, "true");

        // Message gets filtered — returns None
        let result = chain.process(msg).await;
        assert!(result.is_none());
        // Trace is logged via tracing::debug (can't easily assert on logs,
        // but we verify the chain still returns None correctly)
    }

    #[tokio::test]
    async fn test_trace_not_enabled_with_wrong_value() {
        let mut chain = MiddlewareChain::new();
        chain.add(PassThrough);

        let msg = Message::new("test", "evt", Bytes::new())
            .with_metadata(polku_core::metadata_keys::TRACE_ENABLED, "false");

        let result = chain.process(msg).await.unwrap();
        assert!(
            result
                .metadata()
                .get(polku_core::metadata_keys::TRACE_LOG)
                .is_none()
        );
    }
}
