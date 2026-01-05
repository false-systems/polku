//! Checkpoint-based acknowledgment for reliable message delivery
//!
//! Tracks delivery progress per emitter so they can resume from their
//! last known position after restarts or failures.
//!
//! # Architecture
//!
//! ```text
//! Hub ──> Emitter A ──> checkpoint(seq=42)
//!     └─> Emitter B ──> checkpoint(seq=38)
//!     └─> Emitter C ──> checkpoint(seq=42)
//! ```
//!
//! Each emitter maintains an independent sequence number. On restart,
//! the hub can query the minimum checkpoint across all emitters to
//! determine the safe point for message retention.

use parking_lot::RwLock;
use std::collections::HashMap;

/// Storage backend for checkpoint data
///
/// Implementations must be thread-safe as checkpoints may be updated
/// concurrently by multiple emitters.
pub trait CheckpointStore: Send + Sync {
    /// Get the current checkpoint for an emitter
    fn get(&self, emitter: &str) -> Option<u64>;

    /// Set/update the checkpoint for an emitter
    fn set(&self, emitter: &str, seq: u64);

    /// Get all checkpoints (for persistence/debugging)
    fn all(&self) -> HashMap<String, u64>;

    /// Get the minimum checkpoint across all emitters
    ///
    /// This is the safe retention point - messages before this
    /// sequence have been acknowledged by all emitters.
    fn min_checkpoint(&self) -> Option<u64> {
        self.all().values().copied().min()
    }

    /// Remove checkpoint for an emitter (on disconnect)
    fn remove(&self, emitter: &str);

    /// Clear all checkpoints
    fn clear(&self);
}

/// In-memory checkpoint store for testing and single-node deployments
pub struct MemoryCheckpointStore {
    checkpoints: RwLock<HashMap<String, u64>>,
}

impl MemoryCheckpointStore {
    /// Create a new empty checkpoint store
    pub fn new() -> Self {
        Self {
            checkpoints: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointStore for MemoryCheckpointStore {
    fn get(&self, emitter: &str) -> Option<u64> {
        self.checkpoints.read().get(emitter).copied()
    }

    fn set(&self, emitter: &str, seq: u64) {
        self.checkpoints.write().insert(emitter.to_string(), seq);
    }

    fn all(&self) -> HashMap<String, u64> {
        self.checkpoints.read().clone()
    }

    fn remove(&self, emitter: &str) {
        self.checkpoints.write().remove(emitter);
    }

    fn clear(&self) {
        self.checkpoints.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store_is_empty() {
        let store = MemoryCheckpointStore::new();
        assert!(store.all().is_empty());
        assert_eq!(store.min_checkpoint(), None);
    }

    #[test]
    fn test_set_and_get_checkpoint() {
        let store = MemoryCheckpointStore::new();

        store.set("emitter-a", 42);
        assert_eq!(store.get("emitter-a"), Some(42));

        // Non-existent emitter
        assert_eq!(store.get("emitter-b"), None);
    }

    #[test]
    fn test_update_checkpoint() {
        let store = MemoryCheckpointStore::new();

        store.set("emitter-a", 10);
        assert_eq!(store.get("emitter-a"), Some(10));

        store.set("emitter-a", 20);
        assert_eq!(store.get("emitter-a"), Some(20));
    }

    #[test]
    fn test_multiple_emitters() {
        let store = MemoryCheckpointStore::new();

        store.set("emitter-a", 42);
        store.set("emitter-b", 38);
        store.set("emitter-c", 50);

        assert_eq!(store.get("emitter-a"), Some(42));
        assert_eq!(store.get("emitter-b"), Some(38));
        assert_eq!(store.get("emitter-c"), Some(50));

        let all = store.all();
        assert_eq!(all.len(), 3);
        assert_eq!(all.get("emitter-a"), Some(&42));
        assert_eq!(all.get("emitter-b"), Some(&38));
        assert_eq!(all.get("emitter-c"), Some(&50));
    }

    #[test]
    fn test_min_checkpoint() {
        let store = MemoryCheckpointStore::new();

        // Empty store has no minimum
        assert_eq!(store.min_checkpoint(), None);

        store.set("emitter-a", 42);
        assert_eq!(store.min_checkpoint(), Some(42));

        store.set("emitter-b", 38);
        assert_eq!(store.min_checkpoint(), Some(38));

        store.set("emitter-c", 50);
        assert_eq!(store.min_checkpoint(), Some(38)); // Still 38

        // Update the lagging emitter
        store.set("emitter-b", 45);
        assert_eq!(store.min_checkpoint(), Some(42)); // Now emitter-a is minimum
    }

    #[test]
    fn test_remove_emitter() {
        let store = MemoryCheckpointStore::new();

        store.set("emitter-a", 42);
        store.set("emitter-b", 38);

        store.remove("emitter-a");

        assert_eq!(store.get("emitter-a"), None);
        assert_eq!(store.get("emitter-b"), Some(38));
        assert_eq!(store.all().len(), 1);
    }

    #[test]
    fn test_clear_all() {
        let store = MemoryCheckpointStore::new();

        store.set("emitter-a", 42);
        store.set("emitter-b", 38);

        store.clear();

        assert!(store.all().is_empty());
        assert_eq!(store.get("emitter-a"), None);
        assert_eq!(store.get("emitter-b"), None);
    }

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(MemoryCheckpointStore::new());
        let mut handles = vec![];

        // Spawn 10 threads, each updating its own emitter 100 times
        for i in 0..10 {
            let store = Arc::clone(&store);
            let emitter = format!("emitter-{i}");
            handles.push(thread::spawn(move || {
                for seq in 0..100 {
                    store.set(&emitter, seq);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Each emitter should end at 99
        let all = store.all();
        assert_eq!(all.len(), 10);
        for i in 0..10 {
            assert_eq!(all.get(&format!("emitter-{i}")), Some(&99));
        }
    }
}
