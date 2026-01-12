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
        let mut checkpoints = self.checkpoints.write();
        // Only update if new seq is greater than current (monotonic guarantee)
        let should_update = checkpoints
            .get(emitter)
            .map(|&current| seq > current)
            .unwrap_or(true);
        if should_update {
            checkpoints.insert(emitter.to_string(), seq);
        }
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

    // ==========================================================================
    // BUG-EXPOSING TESTS
    // ==========================================================================

    /// BUG: Checkpoint can move backwards - violates monotonic guarantee.
    ///
    /// For reliable delivery, checkpoints should ONLY move forward.
    /// A late-arriving ack with an older sequence number should NOT
    /// overwrite a newer checkpoint.
    ///
    /// This test demonstrates the bug: set() blindly overwrites.
    #[test]
    fn test_bug_checkpoint_can_move_backwards() {
        let store = MemoryCheckpointStore::new();

        // Emitter processes batch 1 (seq 0-9), checkpoints at 9
        store.set("emitter-a", 9);
        assert_eq!(store.get("emitter-a"), Some(9));

        // Emitter processes batch 2 (seq 10-19), checkpoints at 19
        store.set("emitter-a", 19);
        assert_eq!(store.get("emitter-a"), Some(19));

        // BUG: A late callback from batch 1 arrives (network delay, retry, etc.)
        // It tries to set checkpoint to 9 (an OLD value)
        store.set("emitter-a", 9);

        // BUG EXPOSED: Checkpoint moved BACKWARDS from 19 to 9!
        // This violates the fundamental guarantee of checkpoint-based delivery.
        // Messages 10-19 are now "forgotten" and could be re-sent on restart.
        //
        // The fix would be: set() should only update if new_seq > current_seq
        assert_eq!(
            store.get("emitter-a"),
            Some(19),
            "BUG: Checkpoint moved backwards from 19 to 9! Should remain at 19."
        );
    }

    /// BUG: Concurrent out-of-order updates can regress checkpoint.
    ///
    /// In a multi-threaded environment, if batch completions arrive out of order,
    /// the checkpoint can regress to an earlier value.
    #[test]
    fn test_bug_concurrent_backward_regression() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(MemoryCheckpointStore::new());

        // Simulate: batch 1 (seq=10) and batch 2 (seq=20) complete concurrently
        // but batch 2's ack arrives first, then batch 1's ack arrives late

        // Thread 1: "late" ack for older batch
        let store1 = Arc::clone(&store);
        let t1 = thread::spawn(move || {
            // Simulate some delay
            thread::sleep(std::time::Duration::from_millis(5));
            store1.set("emitter", 10); // Old checkpoint
        });

        // Thread 2: ack for newer batch (arrives first despite being later)
        let store2 = Arc::clone(&store);
        let t2 = thread::spawn(move || {
            store2.set("emitter", 20); // New checkpoint
        });

        t2.join().unwrap();
        t1.join().unwrap();

        // BUG EXPOSED: Final value depends on thread timing.
        // It could be 10 (wrong!) if the late ack overwrote the newer checkpoint.
        // With proper monotonic logic, it should always be 20.
        let final_checkpoint = store.get("emitter").unwrap();
        assert_eq!(
            final_checkpoint, 20,
            "BUG: Checkpoint regressed to {} instead of staying at 20",
            final_checkpoint
        );
    }

    /// BUG: min_checkpoint() can return a regressed value.
    ///
    /// If one emitter's checkpoint regresses, min_checkpoint() will return
    /// the wrong safe retention point, potentially causing data loss.
    #[test]
    fn test_bug_min_checkpoint_with_regression() {
        let store = MemoryCheckpointStore::new();

        // Two emitters at different checkpoints
        store.set("emitter-a", 100);
        store.set("emitter-b", 50);

        // Safe retention point is 50 (the minimum)
        assert_eq!(store.min_checkpoint(), Some(50));

        // Emitter B advances to 150
        store.set("emitter-b", 150);
        assert_eq!(store.min_checkpoint(), Some(100)); // Now A is the minimum

        // BUG: Late ack regresses emitter-b back to 50
        store.set("emitter-b", 50);

        // BUG EXPOSED: min_checkpoint is now 50 again!
        // If we had already deleted messages 50-99 (because min was 100),
        // we now have a checkpoint pointing to deleted data.
        //
        // This assertion will FAIL, proving the bug:
        assert_eq!(
            store.min_checkpoint(),
            Some(100),
            "BUG: min_checkpoint regressed from 100 to {} due to backward set()",
            store.min_checkpoint().unwrap()
        );
    }
}
