//! Pipeline Manifest - self-describing topology for AI-native observability
//!
//! The manifest captures the pipeline's component topology at build time,
//! enabling AI agents to query what's running without reading source code.

use serde::Serialize;

/// Self-describing pipeline topology
///
/// Generated at `Hub::build()` time. Exposes the full component graph:
/// ingestors, middleware chain, emitters, and buffer configuration.
///
/// # AI-Native Design
///
/// AI agents operating POLKU can query the manifest to understand:
/// - What middleware is active and in what order
/// - Which emitters are configured (fan-out targets)
/// - Buffer strategy and capacity (backpressure characteristics)
/// - Pipeline version for compatibility checks
#[derive(Debug, Clone, Serialize)]
pub struct PipelineManifest {
    /// Manifest schema version
    pub version: String,
    /// Middleware in processing order
    pub middleware: Vec<ComponentDesc>,
    /// Registered emitters (fan-out targets)
    pub emitters: Vec<ComponentDesc>,
    /// Buffer configuration
    pub buffer: BufferDesc,
    /// Pipeline tuning parameters
    pub tuning: TuningDesc,
}

/// Description of a pipeline component
#[derive(Debug, Clone, Serialize)]
pub struct ComponentDesc {
    /// Component name (from `name()` trait method)
    pub name: String,
    /// Component kind (e.g., "middleware", "emitter")
    pub kind: String,
    /// Position in the processing chain (0-indexed)
    pub position: usize,
}

/// Buffer configuration description
#[derive(Debug, Clone, Serialize)]
pub struct BufferDesc {
    /// Buffer strategy name (e.g., "lock-free", "tiered")
    pub strategy: String,
    /// Total buffer capacity
    pub capacity: usize,
}

/// Pipeline tuning parameters
#[derive(Debug, Clone, Serialize)]
pub struct TuningDesc {
    /// Flush batch size
    pub batch_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Input channel capacity
    pub channel_capacity: usize,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_serializes_to_json() {
        let manifest = PipelineManifest {
            version: "1".to_string(),
            middleware: vec![
                ComponentDesc {
                    name: "filter".to_string(),
                    kind: "middleware".to_string(),
                    position: 0,
                },
                ComponentDesc {
                    name: "transform".to_string(),
                    kind: "middleware".to_string(),
                    position: 1,
                },
            ],
            emitters: vec![ComponentDesc {
                name: "stdout".to_string(),
                kind: "emitter".to_string(),
                position: 0,
            }],
            buffer: BufferDesc {
                strategy: "lock-free".to_string(),
                capacity: 10_000,
            },
            tuning: TuningDesc {
                batch_size: 100,
                flush_interval_ms: 10,
                channel_capacity: 8192,
            },
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("\"filter\""));
        assert!(json.contains("\"stdout\""));
        assert!(json.contains("\"lock-free\""));
        assert!(json.contains("10000"));
    }
}
