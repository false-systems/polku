//! Reserved metadata key constants for POLKU messages
//!
//! These keys are used by convention to carry structured data through
//! the generic Message pipeline. They allow protocol-specific data
//! (e.g., FALSE Protocol severity/outcome) to be preserved in metadata
//! without coupling the pipeline to any specific protocol.

/// Event severity level (numeric string, maps to proto Severity enum)
pub const SEVERITY: &str = "polku.severity";

/// Event outcome (numeric string, maps to proto Outcome enum)
pub const OUTCOME: &str = "polku.outcome";

/// Typed data discriminator: "network", "kernel", "container", "k8s", "process", "resource"
pub const DATA_TYPE: &str = "polku.data_type";

/// Content type of the payload: "application/protobuf", "application/json", etc.
pub const CONTENT_TYPE: &str = "polku.content_type";

/// Distributed trace ID
pub const TRACE_ID: &str = "polku.trace_id";

/// Distributed span ID
pub const SPAN_ID: &str = "polku.span_id";
