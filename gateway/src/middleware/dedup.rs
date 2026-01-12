//! Deduplication middleware
//!
//! Drops duplicate messages within a time window.
//! Thread-safe using parking_lot mutex.
//!
//! # Memory Behavior
//!
//! The `seen` HashMap grows as new unique IDs arrive. Cleanup only runs
//! every `cleanup_interval` operations, so memory may grow between cleanups.
//! For high-cardinality ID spaces with infrequent messages, consider using
//! a smaller cleanup interval or calling cleanup manually.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Time-windowed deduplicator
///
/// Tracks message IDs within a TTL window.
/// Duplicates (same ID within window) are dropped.
///
/// # Memory Growth
///
/// The internal HashMap grows until cleanup runs (every `cleanup_interval` ops).
/// For workloads with many unique IDs and infrequent messages, expired entries
/// may accumulate before cleanup triggers. Adjust `cleanup_interval` accordingly.
pub struct Deduplicator {
    /// ID -> last seen time
    seen: Mutex<HashMap<String, Instant>>,
    /// Time-to-live for dedup entries
    ttl: Duration,
    /// Counter for cleanup scheduling (atomic to avoid separate mutex)
    ops_since_cleanup: AtomicU32,
    /// Cleanup every N operations (minimum 1)
    cleanup_interval: u32,
    /// Count of duplicate messages dropped
    dropped: AtomicU64,
}

impl Deduplicator {
    /// Create a new deduplicator with given TTL
    ///
    /// Uses default cleanup interval of 1000 operations.
    ///
    /// # Arguments
    /// * `ttl` - How long to remember message IDs
    pub fn new(ttl: Duration) -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
            ttl,
            ops_since_cleanup: AtomicU32::new(0),
            cleanup_interval: 1000,
            dropped: AtomicU64::new(0),
        }
    }

    /// Create deduplicator with custom cleanup interval
    ///
    /// # Arguments
    /// * `ttl` - How long to remember message IDs
    /// * `cleanup_interval` - Run cleanup every N operations (minimum 1)
    ///
    /// Lower values = more frequent cleanup = lower memory, higher overhead.
    /// Higher values = less frequent cleanup = higher memory, lower overhead.
    pub fn with_cleanup_interval(ttl: Duration, cleanup_interval: u32) -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
            ttl,
            ops_since_cleanup: AtomicU32::new(0),
            cleanup_interval: cleanup_interval.max(1), // Ensure at least 1
            dropped: AtomicU64::new(0),
        }
    }

    /// Check if message is a duplicate
    ///
    /// Returns true if this is a NEW message (not seen before or TTL expired).
    /// Returns false if duplicate within TTL window.
    pub fn check(&self, id: &str) -> bool {
        let now = Instant::now();

        // Increment op counter and maybe trigger cleanup
        // Use compare_exchange to ensure exactly one thread resets and runs cleanup
        let ops = self.ops_since_cleanup.fetch_add(1, Ordering::Relaxed);
        if ops >= self.cleanup_interval {
            // Only reset if counter is still at ops+1 (we were the one who crossed threshold)
            if self
                .ops_since_cleanup
                .compare_exchange(ops + 1, 0, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.cleanup(now);
            }
        }

        let mut seen = self.seen.lock();

        // Note: clippy suggests `if let ... &&` but that's unstable (RFC 2497)
        #[allow(clippy::collapsible_if)]
        if let Some(last_seen) = seen.get(id) {
            if now.duration_since(*last_seen) < self.ttl {
                // Duplicate within TTL
                return false;
            }
        }

        // New or expired - record and allow
        seen.insert(id.to_string(), now);
        true
    }

    /// Remove expired entries
    ///
    /// Collects expired (key, timestamp) pairs first, then only removes
    /// entries if the timestamp still matches (to avoid removing fresh entries
    /// that arrived between the two passes).
    fn cleanup(&self, now: Instant) {
        // First pass: collect expired keys WITH their timestamps (short lock)
        let expired: Vec<(String, Instant)> = {
            let seen = self.seen.lock();
            seen.iter()
                .filter(|(_, last_seen)| now.duration_since(**last_seen) >= self.ttl)
                .map(|(k, ts)| (k.clone(), *ts))
                .collect()
        };

        if expired.is_empty() {
            return;
        }

        // Second pass: remove only if timestamp still matches (short lock)
        // This prevents removing a fresh entry that arrived between passes
        {
            let mut seen = self.seen.lock();
            for (key, old_ts) in expired {
                if let Some(current_ts) = seen.get(&key) {
                    if *current_ts == old_ts {
                        seen.remove(&key);
                    }
                }
            }
        }
    }

    /// Get current number of tracked IDs
    ///
    /// Returns a snapshot at the time of the call. The value may change
    /// immediately after due to concurrent access.
    pub fn len(&self) -> usize {
        self.seen.lock().len()
    }

    /// Check if tracker is currently empty
    ///
    /// Returns a snapshot at the time of the call. The value may change
    /// immediately after due to concurrent access.
    pub fn is_empty(&self) -> bool {
        self.seen.lock().is_empty()
    }

    /// Get the count of dropped duplicate messages
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Middleware for Deduplicator {
    fn name(&self) -> &'static str {
        "deduplicator"
    }

    async fn process(&self, msg: Message) -> Option<Message> {
        if self.check(&msg.id.to_string()) {
            Some(msg)
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                id = %msg.id,
                "duplicate message dropped"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_dedup_first_message_passes() {
        let dedup = Deduplicator::new(Duration::from_secs(60));
        let msg = Message::new("test", "evt", Bytes::new());

        let result = dedup.process(msg).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_dedup_duplicate_dropped() {
        let dedup = Deduplicator::new(Duration::from_secs(60));

        // Create message with known ID
        let mut msg1 = Message::new("test", "evt", Bytes::new());
        msg1.id = "fixed-id".into();

        let mut msg2 = Message::new("test", "evt", Bytes::new());
        msg2.id = "fixed-id".into();

        // First passes
        assert!(dedup.process(msg1).await.is_some());

        // Duplicate dropped
        assert!(dedup.process(msg2).await.is_none());
    }

    #[tokio::test]
    async fn test_dedup_different_ids_pass() {
        let dedup = Deduplicator::new(Duration::from_secs(60));

        let mut msg1 = Message::new("test", "evt", Bytes::new());
        msg1.id = "id-1".into();

        let mut msg2 = Message::new("test", "evt", Bytes::new());
        msg2.id = "id-2".into();

        assert!(dedup.process(msg1).await.is_some());
        assert!(dedup.process(msg2).await.is_some());
    }

    #[tokio::test]
    async fn test_dedup_expired_passes_again() {
        let dedup = Deduplicator::new(Duration::from_millis(10));

        let mut msg1 = Message::new("test", "evt", Bytes::new());
        msg1.id = "expire-test".into();

        // First passes
        assert!(dedup.process(msg1).await.is_some());

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Same ID passes again after expiry
        let mut msg2 = Message::new("test", "evt", Bytes::new());
        msg2.id = "expire-test".into();
        assert!(dedup.process(msg2).await.is_some());
    }

    #[tokio::test]
    async fn test_dedup_cleanup() {
        let dedup = Deduplicator::with_cleanup_interval(Duration::from_millis(5), 10);

        // Add some messages
        for i in 0..5 {
            let mut msg = Message::new("test", "evt", Bytes::new());
            msg.id = format!("msg-{}", i).into();
            dedup.process(msg).await;
        }

        assert_eq!(dedup.len(), 5);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger cleanup by adding more messages (need 10 to trigger)
        for i in 5..20 {
            let mut msg = Message::new("test", "evt", Bytes::new());
            msg.id = format!("msg-{}", i).into();
            dedup.process(msg).await;
        }

        // Some cleanup should have occurred; allow for timing variance
        // We added 15 new messages, cleanup removed expired ones
        assert!(
            dedup.len() < 20,
            "expected cleanup to reduce entries, got {}",
            dedup.len()
        );
    }

    #[test]
    fn test_dedup_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let dedup = Arc::new(Deduplicator::new(Duration::from_secs(60)));
        let mut handles = vec![];

        // Multiple threads trying to send same ID
        for _ in 0..10 {
            let dedup = Arc::clone(&dedup);
            handles.push(thread::spawn(move || dedup.check("same-id")));
        }

        let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Exactly one should succeed
        let passed: usize = results.iter().filter(|&&r| r).count();
        assert_eq!(passed, 1, "expected exactly 1 pass, got {}", passed);
    }

    #[test]
    fn test_check_method_directly() {
        let dedup = Deduplicator::new(Duration::from_secs(60));

        assert!(dedup.check("id-1")); // First - pass
        assert!(!dedup.check("id-1")); // Duplicate - fail
        assert!(dedup.check("id-2")); // Different - pass
        assert!(!dedup.check("id-2")); // Duplicate - fail
    }

    #[test]
    fn test_cleanup_interval_minimum() {
        // cleanup_interval of 0 should be treated as 1
        let dedup = Deduplicator::with_cleanup_interval(Duration::from_secs(1), 0);
        assert!(dedup.check("test")); // Should not panic
    }

    #[tokio::test]
    async fn test_dedup_dropped_count() {
        let dedup = Deduplicator::new(Duration::from_secs(60));

        // First message passes
        let mut msg1 = Message::new("test", "evt", Bytes::new());
        msg1.id = "dup-test".into();
        assert!(dedup.process(msg1).await.is_some());
        assert_eq!(dedup.dropped_count(), 0);

        // Duplicate is dropped
        let mut msg2 = Message::new("test", "evt", Bytes::new());
        msg2.id = "dup-test".into();
        assert!(dedup.process(msg2).await.is_none());
        assert_eq!(dedup.dropped_count(), 1);

        // Another duplicate
        let mut msg3 = Message::new("test", "evt", Bytes::new());
        msg3.id = "dup-test".into();
        assert!(dedup.process(msg3).await.is_none());
        assert_eq!(dedup.dropped_count(), 2);
    }

}
