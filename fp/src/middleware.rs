//! OccurrenceRouter — route Messages by Occurrence type prefix
//!
//! A convenience middleware for routing FALSE Protocol Occurrences.
//! Uses prefix matching on `msg.message_type` (set by `OccurrenceIngestor`).

use async_trait::async_trait;
use polku_gateway::message::Message;
use polku_gateway::middleware::Middleware;

/// Routes messages by Occurrence type prefix
///
/// Builder-style API for declaring routing rules. First match wins.
/// If no rules match, the message passes through unchanged.
///
/// # Example
///
/// ```ignore
/// let router = OccurrenceRouter::new()
///     .route("ci.", "sykli")
///     .route("kernel.", "tapio")
///     .route("correlation.", "ahti");
/// ```
pub struct OccurrenceRouter {
    rules: Vec<(String, Vec<String>)>,
}

impl OccurrenceRouter {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a routing rule: messages whose `message_type` starts with `prefix`
    /// are routed to `target`.
    pub fn route(mut self, prefix: &str, target: &str) -> Self {
        // Check if we already have a rule for this prefix to allow multi-target
        if let Some(existing) = self.rules.iter_mut().find(|(p, _)| p == prefix) {
            existing.1.push(target.to_string());
        } else {
            self.rules
                .push((prefix.to_string(), vec![target.to_string()]));
        }
        self
    }
}

impl Default for OccurrenceRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for OccurrenceRouter {
    fn name(&self) -> &str {
        "occurrence-router"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        for (prefix, targets) in &self.rules {
            if msg.message_type.starts_with(prefix) {
                msg.route_to = smallvec::SmallVec::from_vec(targets.clone());
                tracing::debug!(
                    id = %msg.id,
                    message_type = %msg.message_type,
                    routes = ?msg.route_to,
                    "occurrence routed"
                );
                return Some(msg);
            }
        }
        // No match — pass through unchanged
        Some(msg)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_route_ci_to_sykli() {
        let router = OccurrenceRouter::new().route("ci.", "sykli");

        let msg = Message::new("sykli", "ci.test.failed", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["sykli".to_string()]);
    }

    #[tokio::test]
    async fn test_route_kernel_to_tapio() {
        let router = OccurrenceRouter::new()
            .route("ci.", "sykli")
            .route("kernel.", "tapio");

        let msg = Message::new("tapio", "kernel.syscall.blocked", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["tapio".to_string()]);
    }

    #[tokio::test]
    async fn test_no_match_passes_through() {
        let router = OccurrenceRouter::new()
            .route("ci.", "sykli")
            .route("kernel.", "tapio");

        let msg = Message::new("unknown", "billing.invoice.created", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert!(result.route_to.is_empty());
    }

    #[tokio::test]
    async fn test_first_match_wins() {
        let router = OccurrenceRouter::new()
            .route("ci.", "first")
            .route("ci.test.", "second");

        let msg = Message::new("sykli", "ci.test.failed", Bytes::new());
        let result = router.process(msg).await.unwrap();
        // "ci." matches before "ci.test."
        assert_eq!(result.route_to.as_slice(), &["first".to_string()]);
    }

    #[tokio::test]
    async fn test_multi_target_same_prefix() {
        let router = OccurrenceRouter::new()
            .route("ci.", "sykli")
            .route("ci.", "ahti");

        let msg = Message::new("sykli", "ci.test.failed", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(
            result.route_to.as_slice(),
            &["sykli".to_string(), "ahti".to_string()]
        );
    }
}
