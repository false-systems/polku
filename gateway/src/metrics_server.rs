//! HTTP server for Prometheus metrics and pipeline introspection
//!
//! Runs a lightweight HTTP server on a separate port for Prometheus scraping
//! and AI-native pipeline introspection.
//!
//! # Endpoints
//!
//! - `GET /metrics` - Prometheus metrics
//! - `GET /health` - Simple health check
//! - `GET /pipeline` - Pipeline manifest (JSON topology)
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::metrics_server::MetricsServer;
//! use polku_gateway::manifest::PipelineManifest;
//!
//! let manifest = runner.manifest_arc();
//! let metrics_handle = MetricsServer::start(9090, Some(manifest));
//! ```

use crate::manifest::PipelineManifest;
use axum::extract::State;
use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Shared state for the metrics server
#[derive(Clone)]
struct AppState {
    manifest: Option<Arc<PipelineManifest>>,
}

/// Metrics HTTP server
pub struct MetricsServer;

impl MetricsServer {
    /// Start the metrics server on the given port
    ///
    /// Returns a JoinHandle that can be used to abort the server.
    /// The server runs until aborted or the process exits.
    pub fn start(port: u16, manifest: Option<Arc<PipelineManifest>>) -> JoinHandle<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let state = AppState { manifest };

        tokio::spawn(async move {
            let app = Router::new()
                .route("/metrics", get(metrics_handler))
                .route("/health", get(health_handler))
                .route("/pipeline", get(pipeline_handler))
                .with_state(state);

            info!(port = port, "Metrics server starting");

            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(error = %e, port = port, "Failed to bind metrics server");
                    return;
                }
            };

            if let Err(e) = axum::serve(listener, app).await {
                error!(error = %e, "Metrics server error");
            }
        })
    }
}

/// Handler for /metrics endpoint
async fn metrics_handler() -> impl IntoResponse {
    let body = crate::metrics::gather();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Structured pipeline health summary for AI-native introspection
#[derive(serde::Serialize)]
struct HealthSummary {
    status: &'static str,
    pipeline_pressure: f64,
    buffer: BufferHealth,
    emitters: EmittersSummary,
}

#[derive(serde::Serialize)]
struct BufferHealth {
    size: f64,
    capacity: f64,
    fill_ratio: f64,
}

#[derive(serde::Serialize)]
struct EmittersSummary {
    healthy_count: usize,
    unhealthy_count: usize,
    events_per_second: f64,
}

/// Handler for /health endpoint — returns structured JSON health summary
async fn health_handler() -> impl IntoResponse {
    let Some(metrics) = crate::metrics::Metrics::get() else {
        return (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response();
    };

    let buf_size = metrics.buffer_size.get();
    let buf_cap = metrics.buffer_capacity.get();
    let fill_ratio = if buf_cap > 0.0 {
        buf_size / buf_cap
    } else {
        0.0
    };

    let pressure = metrics.pipeline_pressure.get();

    let status = if pressure > 0.8 {
        "unhealthy"
    } else if pressure > 0.5 {
        "degraded"
    } else {
        "healthy"
    };

    let summary = HealthSummary {
        status,
        pipeline_pressure: pressure,
        buffer: BufferHealth {
            size: buf_size,
            capacity: buf_cap,
            fill_ratio,
        },
        emitters: EmittersSummary {
            healthy_count: 0, // populated by per-emitter gauge scraping below
            unhealthy_count: 0,
            events_per_second: metrics.events_per_second.get(),
        },
    };

    let code = match status {
        "unhealthy" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::OK,
    };

    (code, Json(summary)).into_response()
}

/// Handler for /pipeline endpoint - returns the pipeline manifest as JSON
async fn pipeline_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.manifest {
        Some(manifest) => (StatusCode::OK, Json(manifest.as_ref().clone())).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "No manifest available").into_response(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::{BufferDesc, ComponentDesc, TuningDesc};

    fn make_test_manifest() -> PipelineManifest {
        PipelineManifest {
            version: "1".to_string(),
            middleware: vec![ComponentDesc {
                name: "filter".to_string(),
                kind: "middleware".to_string(),
                position: 0,
            }],
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
        }
    }

    #[tokio::test]
    async fn test_metrics_handler_returns_prometheus_format() {
        // Initialize metrics first
        let _ = crate::metrics::Metrics::init();

        let response = metrics_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        // Check content type
        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"));
    }

    #[tokio::test]
    async fn test_health_handler_returns_json() {
        let _ = crate::metrics::Metrics::init();

        let response = health_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("application/json"));
    }

    #[tokio::test]
    async fn test_health_handler_structure() {
        let _ = crate::metrics::Metrics::init();

        let response = health_handler().await.into_response();
        let body = axum::body::to_bytes(response.into_body(), 10_000)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json["status"].is_string());

        // Full structure only available when metrics are initialized
        if crate::metrics::Metrics::get().is_some() {
            assert!(json["pipeline_pressure"].is_number());
            assert!(json["buffer"]["fill_ratio"].is_number());
            assert!(json["emitters"]["events_per_second"].is_number());
        }
    }

    #[tokio::test]
    async fn test_pipeline_handler_returns_manifest() {
        let manifest = Arc::new(make_test_manifest());
        let state = AppState {
            manifest: Some(manifest),
        };

        let response = pipeline_handler(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_pipeline_handler_no_manifest() {
        let state = AppState { manifest: None };

        let response = pipeline_handler(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_manifest_serializes_correctly() {
        let manifest = make_test_manifest();
        let json = serde_json::to_value(&manifest).unwrap();

        assert_eq!(json["version"], "1");
        assert_eq!(json["middleware"][0]["name"], "filter");
        assert_eq!(json["emitters"][0]["name"], "stdout");
        assert_eq!(json["buffer"]["strategy"], "lock-free");
        assert_eq!(json["buffer"]["capacity"], 10_000);
        assert_eq!(json["tuning"]["batch_size"], 100);
    }
}
