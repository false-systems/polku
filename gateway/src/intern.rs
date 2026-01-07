//! String interning for zero-cost cloning of repeated strings
//!
//! In a message pipeline, source and message_type strings are often repeated
//! across thousands of messages. Interning stores each unique string once
//! and uses small integer keys for references.
//!
//! # Performance
//!
//! ```text
//! Without interning:
//!   10K messages from "my-service" = 10K string allocations
//!   Message::clone() = copy all strings (~100 bytes)
//!
//! With interning:
//!   10K messages from "my-service" = 1 string allocation
//!   Message::clone() = copy 2 u32 keys (8 bytes)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use polku_gateway::intern::InternedStr;
//!
//! let source = InternedStr::new("my-service");
//! let cloned = source.clone(); // Just copies a u32
//! assert_eq!(source.as_str(), "my-service");
//! ```

use lasso::{Key, Spur, ThreadedRodeo};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::OnceLock;

/// Global string interner
///
/// Uses OnceLock for lazy initialization. Thread-safe via lasso's ThreadedRodeo.
static INTERNER: OnceLock<ThreadedRodeo> = OnceLock::new();

/// Get or initialize the global interner
fn interner() -> &'static ThreadedRodeo {
    INTERNER.get_or_init(ThreadedRodeo::new)
}

/// An interned string reference
///
/// Stores a small integer key (4 bytes) instead of a full String.
/// Cloning is just copying the key - no string allocation.
///
/// # Thread Safety
///
/// InternedStr is Send + Sync. The underlying interner is thread-safe.
#[derive(Clone, Copy)]
pub struct InternedStr {
    key: Spur,
}

impl InternedStr {
    /// Create a new interned string
    ///
    /// If this string was previously interned, returns the existing key.
    /// Otherwise, stores the string and returns a new key.
    #[inline]
    pub fn new(s: &str) -> Self {
        Self {
            key: interner().get_or_intern(s),
        }
    }

    /// Create from an owned String
    ///
    /// Slightly more efficient than `new()` when you have an owned String
    /// that would otherwise be dropped.
    #[inline]
    pub fn from_string(s: String) -> Self {
        Self {
            key: interner().get_or_intern(s),
        }
    }

    /// Get the string slice
    #[inline]
    pub fn as_str(&self) -> &'static str {
        // SAFETY: The interner lives for 'static, and keys are never removed
        interner().resolve(&self.key)
    }

    /// Get the raw key (for debugging/metrics)
    #[inline]
    pub fn key(&self) -> u32 {
        self.key.into_usize() as u32
    }

    /// Get the number of unique strings interned
    pub fn interned_count() -> usize {
        interner().len()
    }

    /// Get approximate memory used by the interner
    pub fn interner_memory() -> usize {
        // Rough estimate: key overhead + string storage
        // lasso doesn't expose exact memory, so we approximate
        let count = Self::interned_count();
        count * 32 // ~32 bytes average per interned string
    }
}

impl Deref for InternedStr {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for InternedStr {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for InternedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl fmt::Display for InternedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for InternedStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Fast path: same key = same string
        self.key == other.key
    }
}

impl Eq for InternedStr {}

impl PartialEq<str> for InternedStr {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for InternedStr {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for InternedStr {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl Hash for InternedStr {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the key, not the string - much faster
        self.key.hash(state);
    }
}

impl From<&str> for InternedStr {
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for InternedStr {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<InternedStr> for String {
    #[inline]
    fn from(s: InternedStr) -> Self {
        s.as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_and_resolve() {
        let s1 = InternedStr::new("hello");
        assert_eq!(s1.as_str(), "hello");
        assert_eq!(&*s1, "hello");
    }

    #[test]
    fn test_same_string_same_key() {
        let s1 = InternedStr::new("same-string");
        let s2 = InternedStr::new("same-string");
        assert_eq!(s1.key(), s2.key());
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_different_strings_different_keys() {
        let s1 = InternedStr::new("first");
        let s2 = InternedStr::new("second");
        assert_ne!(s1.key(), s2.key());
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_clone_is_copy() {
        let s1 = InternedStr::new("copyable");
        let s2 = s1; // Copy, not move
        let s3 = s1; // Also Copy
        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
    }

    #[test]
    fn test_display_and_debug() {
        let s = InternedStr::new("test");
        assert_eq!(format!("{}", s), "test");
        assert_eq!(format!("{:?}", s), "\"test\"");
    }

    #[test]
    fn test_eq_with_str() {
        let s = InternedStr::new("compare");
        assert!(s == "compare");
        // Also test owned String comparison
        let owned = String::from("compare");
        assert!(s == owned);
    }

    #[test]
    fn test_hash_uses_key() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        let key = InternedStr::new("map-key");
        map.insert(key, 42);
        assert_eq!(map.get(&key), Some(&42));
    }

    #[test]
    fn test_from_string() {
        let owned = String::from("owned-string");
        let interned = InternedStr::from_string(owned);
        assert_eq!(interned.as_str(), "owned-string");
    }

    #[test]
    fn test_interned_count() {
        let before = InternedStr::interned_count();
        let _ = InternedStr::new("unique-test-string-12345");
        let after = InternedStr::interned_count();
        assert!(after >= before); // May be equal if string was already interned
    }
}
