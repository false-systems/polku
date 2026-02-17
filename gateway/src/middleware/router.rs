//! Content-based router middleware
//!
//! Routes messages to targets based on matching rules.
//! First matching rule sets the `route_to` field.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use std::sync::Arc;

/// Type alias for routing rule function.
/// Returns owned targets when matched (required by Message.route_to: Vec<String>).
type RuleFn = Box<dyn Fn(&Message) -> Option<Vec<String>> + Send + Sync>;

/// Content-based router
///
/// Applies routing rules in order. First match sets `route_to`.
/// If no rules match, message passes through with original routes.
pub struct Router {
    rules: Vec<RuleFn>,
}

impl Router {
    /// Create a new router
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a routing rule
    ///
    /// Rules are evaluated in order. First match wins.
    ///
    /// Note: targets are cloned on each match because Message.route_to requires Vec<String>.
    /// Arc is used internally to enable sharing if same targets used across rules.
    pub fn rule<F>(mut self, matcher: F, targets: Vec<String>) -> Self
    where
        F: Fn(&Message) -> bool + Send + Sync + 'static,
    {
        // Wrap in Arc for potential sharing; still need to clone Vec on match
        // because Message.route_to is Vec<String>, not Arc<Vec<String>>
        let targets = Arc::new(targets);
        self.rules.push(Box::new(move |msg| {
            if matcher(msg) {
                Some((*targets).clone())
            } else {
                None
            }
        }));
        self
    }

    /// Add a rule that matches by message type prefix
    pub fn route_type_prefix(self, prefix: &str, targets: Vec<String>) -> Self {
        let prefix = prefix.to_string();
        self.rule(move |msg| msg.message_type.starts_with(&prefix), targets)
    }

    /// Add a rule that matches by source
    pub fn route_source(self, source: &str, targets: Vec<String>) -> Self {
        let source = source.to_string();
        self.rule(move |msg| msg.source == source, targets)
    }

    /// Add a default route (matches everything)
    pub fn default_route(self, targets: Vec<String>) -> Self {
        self.rule(|_| true, targets)
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for Router {
    fn name(&self) -> &str {
        "router"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        for rule in &self.rules {
            if let Some(targets) = rule(&msg) {
                msg.route_to = smallvec::SmallVec::from_vec(targets);
                tracing::debug!(
                    id = %msg.id,
                    routes = ?msg.route_to,
                    "routed message"
                );
                return Some(msg);
            }
        }
        // No match - pass through unchanged
        Some(msg)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_router_no_rules_passthrough() {
        let router = Router::new();
        let msg = Message::new("src", "evt", Bytes::new());
        let original_routes = msg.route_to.clone();

        let result = router.process(msg).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().route_to, original_routes);
    }

    #[tokio::test]
    async fn test_router_first_match_wins() {
        let router = Router::new()
            .rule(|_| true, vec!["first".into()])
            .rule(|_| true, vec!["second".into()]);

        let msg = Message::new("src", "evt", Bytes::new());
        let result = router.process(msg).await.unwrap();

        assert_eq!(result.route_to.as_slice(), &["first".to_string()]);
    }

    #[tokio::test]
    async fn test_router_by_type_prefix() {
        let router = Router::new()
            .route_type_prefix("error.", vec!["alerts".into()])
            .route_type_prefix("metric.", vec!["metrics".into()])
            .default_route(vec!["default".into()]);

        // Error goes to alerts
        let msg = Message::new("src", "error.critical", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["alerts".to_string()]);

        // Metric goes to metrics
        let msg = Message::new("src", "metric.cpu", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["metrics".to_string()]);

        // Unknown goes to default
        let msg = Message::new("src", "other.event", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["default".to_string()]);
    }

    #[tokio::test]
    async fn test_router_by_source() {
        let router = Router::new()
            .route_source("tapio", vec!["ebpf-sink".into()])
            .route_source("elava", vec!["aws-sink".into()]);

        let msg = Message::new("tapio", "evt", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["ebpf-sink".to_string()]);

        let msg = Message::new("elava", "evt", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["aws-sink".to_string()]);
    }

    #[tokio::test]
    async fn test_router_custom_rule() {
        let router = Router::new().rule(
            |msg| msg.metadata().get("priority") == Some(&"high".to_string()),
            vec!["priority-queue".into()],
        );

        // Without priority - no match
        let msg = Message::new("src", "evt", Bytes::new());
        let result = router.process(msg).await.unwrap();
        assert!(result.route_to.is_empty());

        // With priority - matches
        let msg = Message::new("src", "evt", Bytes::new()).with_metadata("priority", "high");
        let result = router.process(msg).await.unwrap();
        assert_eq!(result.route_to.as_slice(), &["priority-queue".to_string()]);
    }
}
