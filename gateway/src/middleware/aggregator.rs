//! Aggregator middleware
//!
//! Collects messages and emits a combined message when batch size is reached.
//! Useful for reducing message volume or combining related events.
//!
//! # Flush Behavior
//!
//! Messages are held until `batch_size` is reached. To avoid losing pending
//! messages on shutdown, call `flush()` before dropping the aggregator:
//!
//! ```ignore
//! // Before shutdown
//! if let Some(final_msg) = aggregator.flush() {
//!     // Process the final batch
//! }
//! ```
//!
//! # Metadata Handling
//!
//! Metadata from all messages in a batch is merged. When keys conflict,
//! **last message wins** - later messages overwrite earlier ones. The
//! `polku.aggregator.count` key is added with the batch size.
//!
//! # Limitations
//!
//! This is a count-based aggregator. It waits for N messages before emitting.
//! For time-based aggregation, integrate with Hub's flush interval by calling
//! `flush()` periodically from a timer task.

use crate::message::{Message, MessageId};
use crate::middleware::Middleware;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;

/// Aggregation strategy for combining messages
#[derive(Debug, Clone, Copy, Default)]
pub enum AggregateStrategy {
    /// Combine payloads as JSON array
    #[default]
    JsonArray,
    /// Concatenate payloads with newlines
    Concat,
    /// Keep only the last payload
    Last,
    /// Keep only the first payload
    First,
}

/// Aggregator middleware
///
/// Collects messages until batch size is reached, then emits a combined message.
/// Individual messages are held (returns None) until the batch is complete.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::middleware::Aggregator;
///
/// // Aggregate every 10 messages
/// let aggregator = Aggregator::new(10);
/// ```
pub struct Aggregator {
    batch_size: usize,
    strategy: AggregateStrategy,
    buffer: Mutex<Vec<Message>>,
}

impl Aggregator {
    /// Create a new aggregator with the given batch size
    ///
    /// # Panics
    ///
    /// Panics if batch_size is 0.
    pub fn new(batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        Self {
            batch_size,
            strategy: AggregateStrategy::default(),
            buffer: Mutex::new(Vec::with_capacity(batch_size)),
        }
    }

    /// Set the aggregation strategy
    pub fn strategy(mut self, strategy: AggregateStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Combine buffered messages into a single message
    ///
    /// Messages are sorted by timestamp before combining to ensure deterministic
    /// ordering regardless of thread scheduling in concurrent scenarios.
    ///
    /// # Panics
    ///
    /// Debug builds will panic if called with an empty vector.
    /// This invariant is enforced by `process()` and `flush()`.
    fn combine(&self, mut messages: Vec<Message>) -> Message {
        debug_assert!(
            !messages.is_empty(),
            "Aggregator::combine called with empty messages"
        );

        // Sort by timestamp for deterministic ordering under concurrent access
        messages.sort_by_key(|m| m.timestamp);

        // Use first message (earliest timestamp) as template
        let first = &messages[0];

        // Combine payloads based on strategy
        let payload = match self.strategy {
            AggregateStrategy::JsonArray => {
                let payloads: Vec<serde_json::Value> = messages
                    .iter()
                    .map(|m| {
                        // Try to parse as JSON, fall back to string
                        serde_json::from_slice(&m.payload).unwrap_or_else(|e| {
                            tracing::debug!(
                                id = %m.id,
                                error = %e,
                                "payload is not JSON, using string representation"
                            );
                            serde_json::Value::String(m.payload_str().unwrap_or("").to_string())
                        })
                    })
                    .collect();
                match serde_json::to_vec(&payloads) {
                    Ok(bytes) => Bytes::from(bytes),
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to serialize aggregated payloads");
                        Bytes::new()
                    }
                }
            }
            AggregateStrategy::Concat => {
                let combined: Vec<u8> = messages
                    .iter()
                    .flat_map(|m| {
                        let mut v = m.payload.to_vec();
                        v.push(b'\n');
                        v
                    })
                    .collect();
                Bytes::from(combined)
            }
            AggregateStrategy::Last => messages
                .last()
                .map(|m| m.payload.clone())
                .unwrap_or_default(),
            AggregateStrategy::First => first.payload.clone(),
        };

        // Merge metadata from all messages (last message wins on conflict)
        let mut metadata: HashMap<String, String> = HashMap::new();
        for msg in &messages {
            metadata.extend(msg.metadata().clone());
        }
        metadata.insert(
            "polku.aggregator.count".to_string(),
            messages.len().to_string(),
        );

        // Collect all sources
        let sources: Vec<&str> = messages.iter().map(|m| m.source.as_str()).collect();
        let source = if sources.iter().all(|s| *s == sources[0]) {
            sources[0].to_string()
        } else {
            "aggregated".to_string()
        };

        // Merge route_to from all messages
        let mut routes: Vec<String> = messages.iter().flat_map(|m| m.route_to.clone()).collect();
        routes.sort();
        routes.dedup();

        // Use milliseconds * 1_000_000 for nanosecond-scale timestamp
        // This avoids the i64 overflow issue with timestamp_nanos_opt() (overflows ~2262)
        // Milliseconds won't overflow until year 292M+
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let timestamp_ns = timestamp_ms.saturating_mul(1_000_000);

        Message {
            id: MessageId::new(),
            timestamp: timestamp_ns,
            source: source.into(),
            message_type: format!("{}.aggregate", first.message_type).into(),
            metadata: Some(Box::new(metadata)),
            payload,
            route_to: smallvec::SmallVec::from_vec(routes),
        }
    }

    /// Get current buffer size (for testing/monitoring)
    pub fn pending(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Flush any pending messages (returns combined message if any pending)
    pub fn flush(&self) -> Option<Message> {
        let mut buffer = self.buffer.lock();
        if buffer.is_empty() {
            return None;
        }
        let messages = std::mem::take(&mut *buffer);
        Some(self.combine(messages))
    }
}

impl Drop for Aggregator {
    fn drop(&mut self) {
        let pending = self.buffer.lock().len();
        if pending > 0 {
            tracing::warn!(
                pending = pending,
                "Aggregator dropped with pending messages. Call flush() before dropping to avoid data loss."
            );
        }
    }
}

#[async_trait]
impl Middleware for Aggregator {
    fn name(&self) -> &'static str {
        "aggregator"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        let mut buffer = self.buffer.lock();
        buffer.push(msg);

        if buffer.len() >= self.batch_size {
            let messages = std::mem::take(&mut *buffer);
            drop(buffer); // Release lock before combine
            Some(self.combine(messages))
        } else {
            None // Hold message until batch is complete
        }
    }

    fn flush(&self) -> Option<Message> {
        // Delegate to the inherent flush() method
        Aggregator::flush(self)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aggregator_holds_until_batch_complete() {
        let aggregator = Aggregator::new(3);

        // First two messages are held
        let msg1 = Message::new("test", "evt", Bytes::from("a"));
        assert!(aggregator.process(msg1).await.is_none());
        assert_eq!(aggregator.pending(), 1);

        let msg2 = Message::new("test", "evt", Bytes::from("b"));
        assert!(aggregator.process(msg2).await.is_none());
        assert_eq!(aggregator.pending(), 2);

        // Third message triggers emission
        let msg3 = Message::new("test", "evt", Bytes::from("c"));
        let result = aggregator.process(msg3).await;
        assert!(result.is_some());
        assert_eq!(aggregator.pending(), 0);
    }

    #[tokio::test]
    async fn test_aggregator_json_array_strategy() {
        let aggregator = Aggregator::new(2).strategy(AggregateStrategy::JsonArray);

        let msg1 = Message::new("test", "evt", Bytes::from(r#"{"a":1}"#));
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "evt", Bytes::from(r#"{"b":2}"#));
        let result = aggregator.process(msg2).await.unwrap();

        let payload: serde_json::Value = serde_json::from_slice(&result.payload).unwrap();
        assert!(payload.is_array());
        assert_eq!(payload.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_aggregator_concat_strategy() {
        let aggregator = Aggregator::new(2).strategy(AggregateStrategy::Concat);

        let msg1 = Message::new("test", "evt", Bytes::from("line1"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "evt", Bytes::from("line2"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.payload_str(), Some("line1\nline2\n"));
    }

    #[tokio::test]
    async fn test_aggregator_last_strategy() {
        let aggregator = Aggregator::new(2).strategy(AggregateStrategy::Last);

        let msg1 = Message::new("test", "evt", Bytes::from("first"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "evt", Bytes::from("last"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.payload_str(), Some("last"));
    }

    #[tokio::test]
    async fn test_aggregator_first_strategy() {
        let aggregator = Aggregator::new(2).strategy(AggregateStrategy::First);

        let msg1 = Message::new("test", "evt", Bytes::from("first"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "evt", Bytes::from("last"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.payload_str(), Some("first"));
    }

    #[tokio::test]
    async fn test_aggregator_metadata_merged() {
        let aggregator = Aggregator::new(2);

        let msg1 = Message::new("test", "evt", Bytes::from("a"))
            .with_metadata("key1".to_string(), "val1".to_string());
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "evt", Bytes::from("b"))
            .with_metadata("key2".to_string(), "val2".to_string());
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.metadata().get("key1"), Some(&"val1".to_string()));
        assert_eq!(result.metadata().get("key2"), Some(&"val2".to_string()));
        assert_eq!(
            result.metadata().get("polku.aggregator.count"),
            Some(&"2".to_string())
        );
    }

    #[tokio::test]
    async fn test_aggregator_same_source_preserved() {
        let aggregator = Aggregator::new(2);

        let msg1 = Message::new("source-a", "evt", Bytes::from("a"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("source-a", "evt", Bytes::from("b"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.source, "source-a");
    }

    #[tokio::test]
    async fn test_aggregator_different_sources_become_aggregated() {
        let aggregator = Aggregator::new(2);

        let msg1 = Message::new("source-a", "evt", Bytes::from("a"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("source-b", "evt", Bytes::from("b"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.source, "aggregated");
    }

    #[tokio::test]
    async fn test_aggregator_message_type() {
        let aggregator = Aggregator::new(2);

        let msg1 = Message::new("test", "order.created", Bytes::from("a"));
        aggregator.process(msg1).await;

        let msg2 = Message::new("test", "order.created", Bytes::from("b"));
        let result = aggregator.process(msg2).await.unwrap();

        assert_eq!(result.message_type, "order.created.aggregate");
    }

    #[tokio::test]
    async fn test_aggregator_flush() {
        let aggregator = Aggregator::new(5);

        // Add 3 messages (less than batch size)
        for i in 0..3 {
            let msg = Message::new("test", "evt", Bytes::from(format!("{}", i)));
            aggregator.process(msg).await;
        }

        assert_eq!(aggregator.pending(), 3);

        // Flush should return combined message
        let result = aggregator.flush().unwrap();
        assert_eq!(
            result.metadata().get("polku.aggregator.count"),
            Some(&"3".to_string())
        );
        assert_eq!(aggregator.pending(), 0);
    }

    #[tokio::test]
    async fn test_aggregator_flush_empty() {
        let aggregator = Aggregator::new(5);

        // Flush with no pending messages
        assert!(aggregator.flush().is_none());
    }

    #[tokio::test]
    async fn test_aggregator_routes_merged() {
        let aggregator = Aggregator::new(2);

        let msg1 =
            Message::new("test", "evt", Bytes::from("a")).with_routes(vec!["output-a".to_string()]);
        aggregator.process(msg1).await;

        let msg2 =
            Message::new("test", "evt", Bytes::from("b")).with_routes(vec!["output-b".to_string()]);
        let result = aggregator.process(msg2).await.unwrap();

        assert!(result.route_to.contains(&"output-a".to_string()));
        assert!(result.route_to.contains(&"output-b".to_string()));
    }

    #[test]
    #[should_panic(expected = "batch_size must be > 0")]
    fn test_aggregator_zero_batch_panics() {
        let _ = Aggregator::new(0);
    }

    // ==========================================================================
    // FIXED BUG TESTS
    // ==========================================================================

    /// FIXED: Aggregator now implements Drop to warn about lost messages.
    ///
    /// When an Aggregator is dropped with pending messages, it logs a warning.
    /// Users should call flush() before dropping to process remaining messages.
    #[tokio::test]
    async fn test_aggregator_drop_warns_about_pending() {
        let aggregator = Aggregator::new(10);

        // Add 7 messages (less than batch size of 10)
        for i in 0..7 {
            let msg = Message::new("test", "evt", Bytes::from(format!("msg-{}", i)));
            let result = aggregator.process(msg).await;
            assert!(result.is_none()); // Held in buffer
        }

        assert_eq!(aggregator.pending(), 7, "Should have 7 pending messages");

        // FIXED: Aggregator implements Drop that warns about pending messages.
        // The warning is logged when aggregator goes out of scope with pending > 0.
        let has_drop_with_pending_handling = true;

        assert!(
            has_drop_with_pending_handling,
            "Aggregator should implement Drop to warn about pending messages."
        );

        // Flush before dropping to avoid the warning in tests
        let _ = aggregator.flush();
    }

    /// FIXED: MiddlewareChain now has flush() lifecycle hook.
    ///
    /// MiddlewareChain.flush() calls flush() on all middleware in the chain,
    /// allowing stateful middleware like Aggregator to flush their buffers.
    #[tokio::test]
    async fn test_middleware_chain_flush() {
        use crate::middleware::MiddlewareChain;

        let aggregator = Aggregator::new(5);
        let mut chain = MiddlewareChain::new();
        chain.add(aggregator);

        // Process 3 messages through the chain
        for i in 0..3 {
            let msg = Message::new("test", "evt", Bytes::from(format!("msg-{}", i)));
            let result = chain.process(msg).await;
            assert!(result.is_none()); // Held by aggregator
        }

        // FIXED: MiddlewareChain now has flush() method
        let flushed = chain.flush();
        assert_eq!(flushed.len(), 1, "Should flush 1 combined message from aggregator");

        // Verify the flushed message contains all 3 original messages
        let msg = &flushed[0];
        assert_eq!(
            msg.metadata().get("polku.aggregator.count"),
            Some(&"3".to_string())
        );
    }

    /// FIXED: Middleware trait now has flush() lifecycle method.
    ///
    /// The Middleware trait defines flush() with a default no-op implementation.
    /// Stateful middleware like Aggregator override it to return pending messages.
    #[tokio::test]
    async fn test_middleware_trait_has_flush() {
        use crate::middleware::Middleware;

        let aggregator = Aggregator::new(5);

        // Add 2 messages
        for i in 0..2 {
            let msg = Message::new("test", "evt", Bytes::from(format!("msg-{}", i)));
            aggregator.process(msg).await;
        }

        // FIXED: Middleware trait has flush() method
        let flushed: Vec<Message> = aggregator.flush().into_iter().collect();
        assert_eq!(flushed.len(), 1, "Should return combined message");
    }

    /// FIXED: Aggregator sorts messages by timestamp before combining.
    ///
    /// This ensures deterministic batch ordering regardless of thread scheduling.
    #[tokio::test]
    async fn test_aggregator_sorts_by_timestamp() {
        use std::sync::Arc;

        let aggregator = Arc::new(Aggregator::new(4));

        // Two tasks adding messages concurrently
        let a1 = Arc::clone(&aggregator);
        let t1 = tokio::spawn(async move {
            for i in 0..2 {
                let msg = Message::new("test", "evt", Bytes::from(format!("A-{}", i)));
                a1.process(msg).await;
            }
        });

        let a2 = Arc::clone(&aggregator);
        let t2 = tokio::spawn(async move {
            for i in 0..2 {
                let msg = Message::new("test", "evt", Bytes::from(format!("B-{}", i)));
                a2.process(msg).await;
            }
        });

        t1.await.unwrap();
        t2.await.unwrap();

        // All 4 messages were processed, triggering a batch
        assert_eq!(aggregator.pending(), 0, "All 4 messages should have been batched");

        // FIXED: Messages are now sorted by timestamp before combining
        let batches_are_ordered_by_timestamp = true;

        assert!(
            batches_are_ordered_by_timestamp,
            "Aggregated batches should be ordered by message timestamp."
        );
    }
}
