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
    /// # Panics
    ///
    /// Debug builds will panic if called with an empty vector.
    /// This invariant is enforced by `process()` and `flush()`.
    fn combine(&self, messages: Vec<Message>) -> Message {
        debug_assert!(
            !messages.is_empty(),
            "Aggregator::combine called with empty messages"
        );

        // Use first message as template (safe due to debug_assert)
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

        Message {
            id: MessageId::new(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
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
}
