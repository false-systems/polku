//! Stdout emitter for debugging
//!
//! Prints events to stdout in a human-readable format.
//! Useful for development and debugging.

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
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

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        use std::io::Write;

        let mut stdout = std::io::stdout().lock();
        let mut emitted = 0u64;

        for msg in messages {
            let metadata = msg.metadata();
            // Write message and propagate I/O errors
            let result = if self.pretty {
                writeln!(
                    stdout,
                    "┌─ Message ───────────────────────────────────────────",
                )
                .and_then(|_| writeln!(stdout, "│ ID:        {}", msg.id))
                .and_then(|_| writeln!(stdout, "│ Source:    {}", msg.source))
                .and_then(|_| writeln!(stdout, "│ Type:      {}", msg.message_type))
                .and_then(|_| writeln!(stdout, "│ Timestamp: {} ns", msg.timestamp))
                .and_then(|_| {
                    if !metadata.is_empty() {
                        writeln!(stdout, "│ Metadata:  {:?}", metadata)
                    } else {
                        Ok(())
                    }
                })
                .and_then(|_| writeln!(stdout, "│ Payload:   {} bytes", msg.payload.len()))
                .and_then(|_| {
                    if !msg.route_to.is_empty() {
                        writeln!(stdout, "│ Route to:  {:?}", msg.route_to)
                    } else {
                        Ok(())
                    }
                })
                .and_then(|_| {
                    writeln!(
                        stdout,
                        "└─────────────────────────────────────────────────────",
                    )
                })
            } else {
                writeln!(
                    stdout,
                    "[{}] {}:{} ({} bytes)",
                    msg.source,
                    msg.message_type,
                    msg.id,
                    msg.payload.len()
                )
            };

            // Only count successfully written events
            match result {
                Ok(()) => emitted += 1,
                Err(e) => {
                    // Update metric with what we've written so far, then return error
                    self.emitted_count.fetch_add(emitted, Ordering::Relaxed);
                    return Err(PluginError::Send(format!("stdout write failed: {}", e)));
                }
            }
        }

        self.emitted_count.fetch_add(emitted, Ordering::Relaxed);
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

    fn make_message(id: &str) -> Message {
        Message::with_id(
            id,
            1234567890,
            "test-source",
            "test",
            bytes::Bytes::from_static(&[1, 2, 3]),
        )
    }

    #[tokio::test]
    async fn test_emit_events() {
        let emitter = StdoutEmitter::new();
        let messages = vec![make_message("e1"), make_message("e2")];

        emitter.emit(&messages).await.unwrap();

        assert_eq!(emitter.emitted_count(), 2);
    }

    #[tokio::test]
    async fn test_health() {
        let emitter = StdoutEmitter::new();
        assert!(emitter.health().await);
    }
}
