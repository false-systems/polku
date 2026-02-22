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

/// Content type of the payload: "application/protobuf", "application/json", etc.
pub const CONTENT_TYPE: &str = "polku.content_type";

/// Distributed trace ID
pub const TRACE_ID: &str = "polku.trace_id";

/// Distributed span ID
pub const SPAN_ID: &str = "polku.span_id";

/// Set to "true" to enable per-message processing trace through middleware
pub const TRACE_ENABLED: &str = "polku.trace";

/// JSON array of trace entries recorded by the middleware chain (read-only)
///
/// Each entry: `{"mw":"<name>","action":"passed"|"filtered","us":<microseconds>}`
pub const TRACE_LOG: &str = "_polku.trace";
