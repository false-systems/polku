//! Stdout emitter for debugging
//!
//! Prints events to stdout in a human-readable format.
//! Useful for development and debugging.

use crate::emit::Emitter;
use crate::error::PluginError;
use polku_core::Event;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

/// Stdout emitter - prints events for debugging
pub struct StdoutEmitter {
    /// Pretty print events as JSON
    pretty: bool,
    /// Count of events emitted
    emitted_count: AtomicU64,
}

impl StdoutEmitter {
    /// Create a new StdoutEmitter
    pub fn new() -> Self {
        Self {
            pretty: false,
            emitted_count: AtomicU64::new(0),
        }
    }

    /// Create a new StdoutEmitter with pretty printing
    pub fn pretty() -> Self {
        Self {
            pretty: true,
            emitted_count: AtomicU64::new(0),
        }
    }

    /// Get total events emitted
    pub fn emitted_count(&self) -> u64 {
        self.emitted_count.load(Ordering::Relaxed)
    }
}

impl Default for StdoutEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Emitter for StdoutEmitter {
    fn name(&self) -> &'static str {
        "stdout"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        use std::io::Write;

        let mut stdout = std::io::stdout().lock();

        for event in events {
            if self.pretty {
                writeln!(
                    stdout,
                    "┌─ Event ─────────────────────────────────────────────",
                )
                .ok();
                writeln!(stdout, "│ ID:        {}", event.id).ok();
                writeln!(stdout, "│ Source:    {}", event.source).ok();
                writeln!(stdout, "│ Type:      {}", event.event_type).ok();
                writeln!(stdout, "│ Timestamp: {} ns", event.timestamp_unix_ns).ok();
                if !event.metadata.is_empty() {
                    writeln!(stdout, "│ Metadata:  {:?}", event.metadata).ok();
                }
                writeln!(stdout, "│ Payload:   {} bytes", event.payload.len()).ok();
                if !event.route_to.is_empty() {
                    writeln!(stdout, "│ Route to:  {:?}", event.route_to).ok();
                }
                writeln!(
                    stdout,
                    "└─────────────────────────────────────────────────────",
                )
                .ok();
            } else {
                writeln!(
                    stdout,
                    "[{}] {}:{} ({} bytes)",
                    event.source,
                    event.event_type,
                    event.id,
                    event.payload.len()
                )
                .ok();
            }
        }

        self.emitted_count
            .fetch_add(events.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_event(id: &str) -> Event {
        Event {
            id: id.to_string(),
            timestamp_unix_ns: 1234567890,
            source: "test-source".to_string(),
            event_type: "test".to_string(),
            metadata: Default::default(),
            payload: vec![1, 2, 3],
            route_to: vec![],
            severity: 0,
            outcome: 0,
            data: None,
        }
    }

    #[tokio::test]
    async fn test_emit_events() {
        let emitter = StdoutEmitter::new();
        let events = vec![make_event("e1"), make_event("e2")];

        emitter.emit(&events).await.unwrap();

        assert_eq!(emitter.emitted_count(), 2);
    }

    #[tokio::test]
    async fn test_health() {
        let emitter = StdoutEmitter::new();
        assert!(emitter.health().await);
    }
}
