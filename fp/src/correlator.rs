//! OccurrenceCorrelator — enrich messages with correlation context
//!
//! Reads `context.correlation_keys` from the Occurrence payload and
//! tracks correlation groups. Enriches messages with metadata for
//! downstream routing and grouping.
//!
//! This is an enrichment middleware — it always passes messages through.
//! Non-Occurrence payloads pass through unchanged.

use async_trait::async_trait;
use parking_lot::Mutex;
use polku_gateway::message::Message;
use polku_gateway::middleware::Middleware;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Metadata key for the primary correlation key
pub const CORRELATION_KEY: &str = "polku.correlation_key";

/// Metadata key for the number of messages sharing the correlation key
pub const CORRELATION_COUNT: &str = "polku.correlation_count";

struct CorrelationGroup {
    count: u64,
    last_seen: Instant,
}

/// Enrichment middleware that tags messages with correlation context
///
/// Parses Occurrence JSON from `msg.payload`, reads `context.correlation_keys`,
/// and maintains correlation groups with TTL-based cleanup.
///
/// # Example
///
/// ```ignore
/// let correlator = OccurrenceCorrelator::new().ttl_secs(300);
/// ```
pub struct OccurrenceCorrelator {
    groups: Mutex<HashMap<String, CorrelationGroup>>,
    ttl: Duration,
}

impl OccurrenceCorrelator {
    /// Create a new correlator with a default TTL of 5 minutes
    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(300),
        }
    }

    /// Set the TTL for correlation groups in seconds
    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl = Duration::from_secs(secs);
        self
    }

    /// Clean up expired correlation groups
    fn cleanup(&self, now: Instant) {
        let mut groups = self.groups.lock();
        groups.retain(|_, group| now.duration_since(group.last_seen) < self.ttl);
    }

    /// Extract correlation keys from an Occurrence JSON payload
    fn extract_correlation_keys(payload: &[u8]) -> Option<Vec<String>> {
        let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
        let keys = value.get("context")?.get("correlation_keys")?.as_array()?;

        let result: Vec<String> = keys
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

impl Default for OccurrenceCorrelator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for OccurrenceCorrelator {
    fn name(&self) -> &str {
        "occurrence-correlator"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        let keys = match Self::extract_correlation_keys(&msg.payload) {
            Some(keys) => keys,
            None => return Some(msg),
        };

        let now = Instant::now();

        // Periodic cleanup
        {
            let groups = self.groups.lock();
            if groups.len() > 100 {
                drop(groups);
                self.cleanup(now);
            }
        }

        let mut groups = self.groups.lock();

        // Use the first key as the primary correlation key
        let primary_key = &keys[0];

        // Update all correlation groups
        for key in &keys {
            let group = groups
                .entry(key.clone())
                .or_insert_with(|| CorrelationGroup {
                    count: 0,
                    last_seen: now,
                });
            group.count += 1;
            group.last_seen = now;
        }

        // Enrich metadata with primary key and its count
        let count = groups.get(primary_key).map(|g| g.count).unwrap_or(1);

        drop(groups);

        msg.metadata_mut()
            .insert(CORRELATION_KEY.to_string(), primary_key.clone());
        msg.metadata_mut()
            .insert(CORRELATION_COUNT.to_string(), count.to_string());

        tracing::debug!(
            id = %msg.id,
            correlation_key = %primary_key,
            correlation_count = count,
            "message correlated"
        );

        Some(msg)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn occurrence_json(correlation_keys: &[&str]) -> Bytes {
        let keys_json: Vec<String> = correlation_keys
            .iter()
            .map(|k| format!(r#""{k}""#))
            .collect();
        let json = format!(
            r#"{{"id":"01X","timestamp":"2024-01-15T10:30:00Z","source":"test","type":"ci.test.failed","severity":"error","outcome":"failure","context":{{"correlation_keys":[{}]}}}}"#,
            keys_json.join(",")
        );
        Bytes::from(json)
    }

    fn occurrence_json_no_context() -> Bytes {
        Bytes::from(
            r#"{"id":"01X","timestamp":"2024-01-15T10:30:00Z","source":"test","type":"ci.test.failed","severity":"error","outcome":"failure"}"#,
        )
    }

    #[tokio::test]
    async fn test_message_with_correlation_keys_enriched() {
        let correlator = OccurrenceCorrelator::new();
        let msg = Message::new("test", "ci.test.failed", occurrence_json(&["build-123"]));

        let result = correlator.process(msg).await.unwrap();
        assert_eq!(
            result.metadata().get(CORRELATION_KEY),
            Some(&"build-123".to_string())
        );
        assert_eq!(
            result.metadata().get(CORRELATION_COUNT),
            Some(&"1".to_string())
        );
    }

    #[tokio::test]
    async fn test_two_messages_same_key_increments_count() {
        let correlator = OccurrenceCorrelator::new();

        let msg1 = Message::new("test", "ci.test.failed", occurrence_json(&["build-123"]));
        let result1 = correlator.process(msg1).await.unwrap();
        assert_eq!(
            result1.metadata().get(CORRELATION_COUNT),
            Some(&"1".to_string())
        );

        let msg2 = Message::new("test", "ci.build.failed", occurrence_json(&["build-123"]));
        let result2 = correlator.process(msg2).await.unwrap();
        assert_eq!(
            result2.metadata().get(CORRELATION_COUNT),
            Some(&"2".to_string())
        );
    }

    #[tokio::test]
    async fn test_message_without_correlation_keys_passes_through() {
        let correlator = OccurrenceCorrelator::new();
        let msg = Message::new("test", "ci.test.failed", occurrence_json_no_context());

        let result = correlator.process(msg).await.unwrap();
        assert!(result.metadata().get(CORRELATION_KEY).is_none());
        assert!(result.metadata().get(CORRELATION_COUNT).is_none());
    }

    #[tokio::test]
    async fn test_non_occurrence_payload_passes_through() {
        let correlator = OccurrenceCorrelator::new();
        let msg = Message::new("test", "raw.event", Bytes::from("not json at all"));

        let result = correlator.process(msg).await.unwrap();
        assert!(result.metadata().get(CORRELATION_KEY).is_none());
    }

    #[tokio::test]
    async fn test_ttl_expiry_cleans_up_groups() {
        let correlator = OccurrenceCorrelator::new().ttl_secs(0); // immediate expiry

        let msg1 = Message::new("test", "ci.test.failed", occurrence_json(&["build-123"]));
        correlator.process(msg1).await.unwrap();

        // Force cleanup
        correlator.cleanup(Instant::now() + Duration::from_secs(1));

        // New message with same key starts fresh
        let msg2 = Message::new("test", "ci.test.failed", occurrence_json(&["build-123"]));
        let result = correlator.process(msg2).await.unwrap();
        assert_eq!(
            result.metadata().get(CORRELATION_COUNT),
            Some(&"1".to_string())
        );
    }

    #[tokio::test]
    async fn test_multiple_correlation_keys_uses_first() {
        let correlator = OccurrenceCorrelator::new();
        let msg = Message::new(
            "test",
            "ci.test.failed",
            occurrence_json(&["build-123", "pipeline-456"]),
        );

        let result = correlator.process(msg).await.unwrap();
        assert_eq!(
            result.metadata().get(CORRELATION_KEY),
            Some(&"build-123".to_string())
        );
    }

    #[tokio::test]
    async fn test_empty_correlation_keys_passes_through() {
        let correlator = OccurrenceCorrelator::new();
        let json = Bytes::from(
            r#"{"id":"01X","timestamp":"t","source":"s","type":"t","severity":"info","outcome":"success","context":{"correlation_keys":[]}}"#,
        );
        let msg = Message::new("test", "ci.test.failed", json);

        let result = correlator.process(msg).await.unwrap();
        assert!(result.metadata().get(CORRELATION_KEY).is_none());
    }
}
