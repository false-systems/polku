//! Minimal FALSE Protocol types for POLKU ingestion and probe output
//!
//! These are local types matching the FALSE Protocol JSON schema.
//! Replace with the `false-protocol` crate when published.

use serde::{Deserialize, Serialize};

/// A FALSE Protocol Occurrence — the atomic unit of observable truth
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Occurrence {
    /// ULID identifier
    pub id: String,

    /// ISO 8601 timestamp
    pub timestamp: String,

    /// Origin system (e.g., "sykli", "tapio")
    pub source: String,

    /// Hierarchical type (e.g., "ci.test.failed", "kernel.syscall.blocked")
    #[serde(rename = "type")]
    pub occurrence_type: String,

    /// Severity level
    pub severity: Severity,

    /// Outcome of the observed event
    pub outcome: Outcome,

    /// Contextual information
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// Error details (when severity >= Warning)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,

    /// AI reasoning about the occurrence
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,

    /// Arbitrary structured data
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Severity levels matching FALSE Protocol spec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl Severity {
    /// Map to polku.severity metadata value (i32 string)
    pub fn as_polku_value(&self) -> &'static str {
        match self {
            Self::Debug => "1",
            Self::Info => "2",
            Self::Warning => "3",
            Self::Error => "4",
            Self::Critical => "5",
        }
    }
}

/// Outcome of an observed event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success,
    Failure,
    Timeout,
    InProgress,
    Unknown,
}

impl Outcome {
    /// Map to polku.outcome metadata value (i32 string)
    pub fn as_polku_value(&self) -> &'static str {
        match self {
            Self::Success => "1",
            Self::Failure => "2",
            Self::Timeout => "3",
            Self::InProgress => "4",
            Self::Unknown => "0",
        }
    }
}

/// Contextual information about where the occurrence happened
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Context {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub correlation_keys: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<Entity>,
}

/// Error block — what went wrong and why it matters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    /// Machine-readable error code (e.g., "TEST_FAILURE", "SYSCALL_DENIED")
    pub code: String,

    /// Human-readable description of what failed
    pub what_failed: String,

    /// Why this failure matters
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why_it_matters: Option<String>,

    /// Possible root causes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub possible_causes: Vec<String>,

    /// Recommended fix
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_fix: Option<String>,
}

/// AI reasoning about the occurrence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reasoning {
    /// One-line summary
    pub summary: String,

    /// Detailed explanation
    pub explanation: String,

    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,

    /// Chain of causal reasoning
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub causal_chain: Vec<String>,

    /// Patterns that matched to produce this reasoning
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patterns_matched: Vec<String>,
}

/// An observed entity (service, pod, node, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Entity type (e.g., "service", "pod", "node")
    #[serde(rename = "type")]
    pub entity_type: String,

    /// Unique identifier
    pub id: String,

    /// Human-readable name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Version string
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// When this entity was observed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,

    /// Whether this entity is the source of truth
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_of_truth: Option<bool>,

    /// Namespace / scope
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_occurrence_serialization_roundtrip() {
        let occ = Occurrence {
            id: "01HWXYZ".into(),
            timestamp: "2024-01-15T10:30:00Z".into(),
            source: "sykli".into(),
            occurrence_type: "ci.test.failed".into(),
            severity: Severity::Error,
            outcome: Outcome::Failure,
            context: Some(Context {
                cluster: Some("prod".into()),
                namespace: None,
                correlation_keys: vec!["build-123".into()],
                entities: vec![],
            }),
            error: Some(Error {
                code: "TEST_FAILURE".into(),
                what_failed: "Unit tests in auth module".into(),
                why_it_matters: Some("Blocks release pipeline".into()),
                possible_causes: vec!["Timeout in network mock".into()],
                suggested_fix: Some("Increase mock timeout".into()),
            }),
            reasoning: None,
            data: None,
        };

        let json = serde_json::to_string(&occ).unwrap();
        let parsed: Occurrence = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "01HWXYZ");
        assert_eq!(parsed.occurrence_type, "ci.test.failed");
        assert_eq!(parsed.severity, Severity::Error);
        assert_eq!(parsed.outcome, Outcome::Failure);
        assert_eq!(parsed.error.unwrap().code, "TEST_FAILURE");
    }

    #[test]
    fn test_minimal_occurrence() {
        let json = r#"{
            "id": "01ABC",
            "timestamp": "2024-01-15T10:30:00Z",
            "source": "tapio",
            "type": "kernel.syscall.blocked",
            "severity": "warning",
            "outcome": "unknown"
        }"#;

        let occ: Occurrence = serde_json::from_str(json).unwrap();
        assert_eq!(occ.source, "tapio");
        assert_eq!(occ.occurrence_type, "kernel.syscall.blocked");
        assert!(occ.context.is_none());
        assert!(occ.error.is_none());
        assert!(occ.reasoning.is_none());
        assert!(occ.data.is_none());
    }

    #[test]
    fn test_severity_polku_mapping() {
        assert_eq!(Severity::Debug.as_polku_value(), "1");
        assert_eq!(Severity::Info.as_polku_value(), "2");
        assert_eq!(Severity::Warning.as_polku_value(), "3");
        assert_eq!(Severity::Error.as_polku_value(), "4");
        assert_eq!(Severity::Critical.as_polku_value(), "5");
    }

    #[test]
    fn test_outcome_polku_mapping() {
        assert_eq!(Outcome::Success.as_polku_value(), "1");
        assert_eq!(Outcome::Failure.as_polku_value(), "2");
        assert_eq!(Outcome::Timeout.as_polku_value(), "3");
        assert_eq!(Outcome::InProgress.as_polku_value(), "4");
        assert_eq!(Outcome::Unknown.as_polku_value(), "0");
    }

    #[test]
    fn test_type_field_renamed() {
        let json = r#"{"id":"x","timestamp":"t","source":"s","type":"ci.test.failed","severity":"info","outcome":"success"}"#;
        let occ: Occurrence = serde_json::from_str(json).unwrap();
        assert_eq!(occ.occurrence_type, "ci.test.failed");

        // Serialized back should use "type" not "occurrence_type"
        let serialized = serde_json::to_string(&occ).unwrap();
        assert!(serialized.contains(r#""type":"ci.test.failed""#));
        assert!(!serialized.contains("occurrence_type"));
    }

    #[test]
    fn test_optional_fields_skipped_in_serialization() {
        let occ = Occurrence {
            id: "x".into(),
            timestamp: "t".into(),
            source: "s".into(),
            occurrence_type: "test".into(),
            severity: Severity::Info,
            outcome: Outcome::Success,
            context: None,
            error: None,
            reasoning: None,
            data: None,
        };

        let json = serde_json::to_string(&occ).unwrap();
        assert!(!json.contains("context"));
        assert!(!json.contains("error"));
        assert!(!json.contains("reasoning"));
        assert!(!json.contains("data"));
    }
}
