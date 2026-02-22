//! Validator middleware
//!
//! Validates messages against a schema or custom validation function.
//! Invalid messages can be dropped or tagged with an error marker.

use crate::message::Message;
use crate::middleware::Middleware;
use async_trait::async_trait;
use std::sync::Arc;

/// Validation result
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Message is valid
    Valid,
    /// Message is invalid with reason
    Invalid(String),
}

/// Action to take on invalid messages
#[derive(Debug, Clone, Copy, Default)]
pub enum InvalidAction {
    /// Drop invalid messages (default)
    #[default]
    Drop,
    /// Tag with error metadata but pass through
    Tag,
}

/// Type alias for the validation function
pub type ValidateFn = Arc<dyn Fn(&Message) -> ValidationResult + Send + Sync>;

/// Validator middleware
///
/// Validates messages using a provided validation function.
/// Invalid messages can be dropped or tagged based on configuration.
///
/// # Example
///
/// ```ignore
/// use polku_gateway::middleware::{Validator, ValidationResult};
///
/// // JSON payload validator
/// let validator = Validator::new(|msg| {
///     match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
///         Ok(_) => ValidationResult::Valid,
///         Err(e) => ValidationResult::Invalid(e.to_string()),
///     }
/// });
/// ```
pub struct Validator {
    validate_fn: ValidateFn,
    on_invalid: InvalidAction,
}

impl Validator {
    /// Create a new validator with the given validation function
    ///
    /// By default, invalid messages are dropped.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&Message) -> ValidationResult + Send + Sync + 'static,
    {
        Self {
            validate_fn: Arc::new(f),
            on_invalid: InvalidAction::Drop,
        }
    }

    /// Set the action to take on invalid messages
    pub fn on_invalid(mut self, action: InvalidAction) -> Self {
        self.on_invalid = action;
        self
    }

    /// Create a validator that checks if payload is valid JSON
    pub fn json() -> Self {
        Self::new(|msg| {
            if msg.payload.is_empty() {
                return ValidationResult::Invalid("empty payload".to_string());
            }
            match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                Ok(_) => ValidationResult::Valid,
                Err(e) => ValidationResult::Invalid(format!("invalid JSON: {}", e)),
            }
        })
    }

    /// Create a validator that checks if payload is non-empty
    pub fn non_empty() -> Self {
        Self::new(|msg| {
            if msg.payload.is_empty() {
                ValidationResult::Invalid("empty payload".to_string())
            } else {
                ValidationResult::Valid
            }
        })
    }

    /// Create a validator that checks payload size
    pub fn max_size(max_bytes: usize) -> Self {
        Self::new(move |msg| {
            if msg.payload.len() > max_bytes {
                ValidationResult::Invalid(format!(
                    "payload too large: {} > {} bytes",
                    msg.payload.len(),
                    max_bytes
                ))
            } else {
                ValidationResult::Valid
            }
        })
    }
}

#[async_trait]
impl Middleware for Validator {
    fn name(&self) -> &str {
        "validator"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        match (self.validate_fn)(&msg) {
            ValidationResult::Valid => Some(msg),
            ValidationResult::Invalid(reason) => {
                tracing::debug!(
                    id = %msg.id,
                    source = %msg.source,
                    reason = %reason,
                    "validation failed"
                );

                match self.on_invalid {
                    InvalidAction::Drop => None,
                    InvalidAction::Tag => {
                        msg.metadata_mut()
                            .insert("polku.validator.error".to_string(), reason);
                        msg.metadata_mut()
                            .insert("polku.validator.valid".to_string(), "false".to_string());
                        Some(msg)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_validator_passes_valid() {
        let validator = Validator::new(|_| ValidationResult::Valid);

        let msg = Message::new("test", "evt", Bytes::from("data"));
        let result = validator.process(msg).await;

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_validator_drops_invalid() {
        let validator = Validator::new(|_| ValidationResult::Invalid("bad".to_string()));

        let msg = Message::new("test", "evt", Bytes::from("data"));
        let result = validator.process(msg).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validator_tags_invalid() {
        let validator = Validator::new(|_| ValidationResult::Invalid("bad data".to_string()))
            .on_invalid(InvalidAction::Tag);

        let msg = Message::new("test", "evt", Bytes::from("data"));
        let result = validator.process(msg).await.unwrap();

        assert_eq!(
            result.metadata().get("polku.validator.error"),
            Some(&"bad data".to_string())
        );
        assert_eq!(
            result.metadata().get("polku.validator.valid"),
            Some(&"false".to_string())
        );
    }

    #[tokio::test]
    async fn test_validator_json_valid() {
        let validator = Validator::json();

        let msg = Message::new("test", "evt", Bytes::from(r#"{"key": "value"}"#));
        let result = validator.process(msg).await;

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_validator_json_invalid() {
        let validator = Validator::json();

        let msg = Message::new("test", "evt", Bytes::from("not json"));
        let result = validator.process(msg).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validator_json_empty() {
        let validator = Validator::json();

        let msg = Message::new("test", "evt", Bytes::new());
        let result = validator.process(msg).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validator_non_empty_valid() {
        let validator = Validator::non_empty();

        let msg = Message::new("test", "evt", Bytes::from("data"));
        let result = validator.process(msg).await;

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_validator_non_empty_invalid() {
        let validator = Validator::non_empty();

        let msg = Message::new("test", "evt", Bytes::new());
        let result = validator.process(msg).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validator_max_size_valid() {
        let validator = Validator::max_size(100);

        let msg = Message::new("test", "evt", Bytes::from("small"));
        let result = validator.process(msg).await;

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_validator_max_size_invalid() {
        let validator = Validator::max_size(5);

        let msg = Message::new("test", "evt", Bytes::from("too long"));
        let result = validator.process(msg).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validator_custom_logic() {
        // Only accept messages with "valid" in metadata
        let validator = Validator::new(|msg| {
            if msg.metadata().contains_key("valid") {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("missing 'valid' metadata".to_string())
            }
        });

        let msg_invalid = Message::new("test", "evt", Bytes::new());
        assert!(validator.process(msg_invalid).await.is_none());

        let msg_valid = Message::new("test", "evt", Bytes::new())
            .with_metadata("valid".to_string(), "yes".to_string());
        assert!(validator.process(msg_valid).await.is_some());
    }
}
