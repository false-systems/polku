//! POLKU Gateway - Programmatic Protocol Hub
//!
//! Infrastructure library for internal service communication.
//!
//! ## Usage
//!
//! ```bash
//! # Run with default settings
//! cargo run
//!
//! # Run with debug output to stdout
//! POLKU_LOG_LEVEL=debug cargo run
//! ```
//!
//! ## Environment Variables
//!
//! - `POLKU_GRPC_ADDR`: gRPC server address (default: "[::1]:50051")
//! - `POLKU_METRICS_ADDR`: Metrics server address (default: "127.0.0.1:9090")
//! - `POLKU_BUFFER_CAPACITY`: Event buffer capacity (default: 10000)
//! - `POLKU_LOG_LEVEL`: Log level (default: "info")
//! - `POLKU_EMIT_GRPC_ENDPOINTS`: Comma-separated gRPC endpoints for router mode
//! - `POLKU_EMIT_GRPC_LAZY`: Use lazy connections (true/false)
//! - `POLKU_EMIT_AHTI_ENDPOINT`: AHTI endpoint (requires --features ahti)

use polku_gateway::buffer::RingBuffer;
use polku_gateway::config::Config;
#[cfg(feature = "ahti")]
use polku_gateway::emit::AhtiEmitter;
use polku_gateway::emit::{GrpcEmitter, StdoutEmitter};
use polku_gateway::hub::Hub;
use polku_gateway::metrics::Metrics;
use polku_gateway::metrics_server::MetricsServer;
use polku_gateway::registry::PluginRegistry;
use polku_gateway::server::GatewayService;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = Config::from_env()?;
    info!(
        grpc_addr = %config.grpc_addr,
        metrics_addr = %config.metrics_addr,
        buffer_capacity = config.buffer_capacity,
        "Starting POLKU Gateway"
    );

    // Initialize metrics and start metrics server
    Metrics::init()?;
    let _metrics_handle = MetricsServer::start(config.metrics_addr.port());
    info!(addr = %config.metrics_addr, "Metrics server started");

    // Create shared buffer (used for backpressure signaling)
    let buffer = Arc::new(RingBuffer::new(config.buffer_capacity));

    // Build Hub with appropriate emitter
    #[cfg(feature = "ahti")]
    let hub = if let Some(ref ahti_endpoint) = config.emit_ahti_endpoint {
        // AHTI mode: forward to AHTI for analysis
        let ahti_emitter = AhtiEmitter::new(ahti_endpoint).await?;
        info!(endpoint = %ahti_endpoint, "AHTI mode: forwarding to AHTI");
        Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms)
            .emitter(ahti_emitter)
    } else if !config.emit_grpc_endpoints.is_empty() {
        // Router mode: forward to downstream POLKU instances
        let grpc_emitter = if config.emit_grpc_lazy {
            GrpcEmitter::with_endpoints_lazy(config.emit_grpc_endpoints.clone()).await?
        } else {
            GrpcEmitter::with_endpoints(config.emit_grpc_endpoints.clone()).await?
        };
        info!(
            endpoints = ?config.emit_grpc_endpoints,
            lazy = config.emit_grpc_lazy,
            "Router mode: forwarding to gRPC endpoints"
        );
        Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms)
            .emitter(grpc_emitter)
    } else {
        // Debug mode: print to stdout
        info!("Debug mode: emitting to stdout");
        Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms)
            .emitter(StdoutEmitter::pretty())
    };

    #[cfg(not(feature = "ahti"))]
    let hub = if !config.emit_grpc_endpoints.is_empty() {
        // Router mode: forward to downstream POLKU instances
        let grpc_emitter = if config.emit_grpc_lazy {
            GrpcEmitter::with_endpoints_lazy(config.emit_grpc_endpoints.clone()).await?
        } else {
            GrpcEmitter::with_endpoints(config.emit_grpc_endpoints.clone()).await?
        };
        info!(
            endpoints = ?config.emit_grpc_endpoints,
            lazy = config.emit_grpc_lazy,
            "Router mode: forwarding to gRPC endpoints"
        );
        Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms)
            .emitter(grpc_emitter)
    } else {
        // Debug mode: print to stdout
        info!("Debug mode: emitting to stdout");
        Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms)
            .emitter(StdoutEmitter::pretty())
    };

    // Build hub and get senders + runner
    let (_raw_sender, hub_sender, hub_runner) = hub.build();

    // Spawn Hub runner in background (processes events and calls emit)
    let hub_handle = tokio::spawn(async move {
        if let Err(e) = hub_runner.run().await {
            tracing::error!(error = %e, "Hub runner error");
        }
    });

    // Create plugin registry (for health checks)
    let registry = Arc::new(PluginRegistry::new());

    // Create gRPC service with Hub sender
    let service = GatewayService::with_hub(
        Arc::clone(&buffer),
        Arc::clone(&registry),
        hub_sender,
    );

    // Start gRPC server
    let addr = config.grpc_addr;
    info!(%addr, "gRPC server listening");

    Server::builder()
        .add_service(service.into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    // Wait for Hub to finish processing
    hub_handle.abort();

    info!("POLKU Gateway shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = ?e, "Failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!(error = ?e, "Failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down"),
        _ = terminate => info!("Received SIGTERM, shutting down"),
    }
}
