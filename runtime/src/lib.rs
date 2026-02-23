//! POLKU Runtime — injectable pipeline interface
//!
//! Provides [`run()`] for zero-boilerplate pipeline startup, and
//! [`RuntimeBuilder`] for users who need control over ports, gRPC, etc.
//!
//! # Quick start
//!
//! ```ignore
//! use polku_runtime::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     polku_runtime::run(|hub| async move {
//!         Ok(hub.emitter(StdoutEmitter::pretty()))
//!     }).await
//! }
//! ```

#![deny(unsafe_code)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![warn(clippy::panic)]

pub mod prelude;

use polku_gateway::buffer::RingBuffer;
use polku_gateway::config::{Config, LogFormat};
use polku_gateway::hub::Hub;
use polku_gateway::metrics::Metrics;
use polku_gateway::metrics_server::MetricsServer;
use polku_gateway::registry::PluginRegistry;
use polku_gateway::server::GatewayService;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Run a POLKU pipeline with default settings.
///
/// Loads configuration from environment variables, initialises tracing and
/// metrics, calls your closure to wire up the pipeline, then runs the gRPC
/// server with graceful shutdown.
///
/// # Example
///
/// ```ignore
/// polku_runtime::run(|hub| async move {
///     Ok(hub
///         .ingestor(JsonIngestor::new())
///         .middleware(Filter::new(|msg: &Message| {
///             msg.message_type.starts_with("important.")
///         }))
///         .emitter(StdoutEmitter::pretty()))
/// }).await
/// ```
pub async fn run<F, Fut>(configure: F) -> anyhow::Result<()>
where
    F: FnOnce(Hub) -> Fut,
    Fut: Future<Output = anyhow::Result<Hub>>,
{
    RuntimeBuilder::new().configure(configure).await
}

/// Power-user builder for controlling runtime behaviour.
///
/// Use this when you need to override the gRPC address, metrics port,
/// or disable the gRPC server entirely.
///
/// # Example
///
/// ```ignore
/// RuntimeBuilder::new()
///     .grpc_addr("0.0.0.0:50052".parse()?)
///     .metrics_port(9091)
///     .configure(|hub| async move {
///         Ok(hub.emitter(StdoutEmitter::pretty()))
///     })
///     .await
/// ```
pub struct RuntimeBuilder {
    grpc_addr: Option<SocketAddr>,
    metrics_port: Option<u16>,
    grpc_enabled: bool,
}

impl RuntimeBuilder {
    /// Create a new builder with defaults from environment variables.
    pub fn new() -> Self {
        Self {
            grpc_addr: None,
            metrics_port: None,
            grpc_enabled: true,
        }
    }

    /// Override the gRPC server listen address.
    ///
    /// Default: loaded from `POLKU_GRPC_ADDR` env var, or `0.0.0.0:50051`.
    pub fn grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.grpc_addr = Some(addr);
        self
    }

    /// Override the metrics HTTP server port.
    ///
    /// Default: loaded from `POLKU_METRICS_ADDR` env var, or `9090`.
    pub fn metrics_port(mut self, port: u16) -> Self {
        self.metrics_port = Some(port);
        self
    }

    /// Disable the gRPC ingest server.
    ///
    /// Useful for pipelines that only use `RawSender` / `MessageSender`
    /// and don't need gRPC ingest.
    pub fn disable_grpc(mut self) -> Self {
        self.grpc_enabled = false;
        self
    }

    /// Configure the pipeline and run it to completion.
    ///
    /// This is the terminal method — it blocks until shutdown.
    pub async fn configure<F, Fut>(self, configure: F) -> anyhow::Result<()>
    where
        F: FnOnce(Hub) -> Fut,
        Fut: Future<Output = anyhow::Result<Hub>>,
    {
        // ── 1. Load config from env ──────────────────────────────
        let config = Config::from_env()?;

        // ── 2. Init tracing ──────────────────────────────────────
        init_tracing(&config);

        info!(
            grpc_addr = %self.grpc_addr.unwrap_or(config.grpc_addr),
            metrics_port = self.metrics_port.unwrap_or(config.metrics_addr.port()),
            buffer_capacity = config.buffer_capacity,
            "Starting POLKU"
        );

        // ── 3. Init metrics + HTTP server ────────────────────────
        Metrics::init()?;
        let metrics_port = self.metrics_port.unwrap_or(config.metrics_addr.port());
        let _metrics_handle = MetricsServer::start(metrics_port, None);
        info!(port = metrics_port, "Metrics server started");

        // ── 4. Pre-configure Hub from env ────────────────────────
        let hub = Hub::new()
            .buffer_capacity(config.buffer_capacity)
            .batch_size(config.batch_size)
            .flush_interval_ms(config.flush_interval_ms);

        // ── 5. User configures the Hub ───────────────────────────
        let hub = configure(hub).await?;

        // ── 6. Build and spawn HubRunner ─────────────────────────
        let (_raw_sender, hub_sender, hub_runner) = hub.build();

        let hub_handle = tokio::spawn(async move {
            if let Err(e) = hub_runner.run().await {
                tracing::error!(error = %e, "Hub runner error");
            }
        });

        // ── 7. Start gRPC server (if enabled) ───────────────────
        if self.grpc_enabled {
            let buffer = Arc::new(RingBuffer::new(config.buffer_capacity));
            let registry = Arc::new(PluginRegistry::new());
            let service =
                GatewayService::with_hub(Arc::clone(&buffer), Arc::clone(&registry), hub_sender);

            let addr = self.grpc_addr.unwrap_or(config.grpc_addr);
            info!(%addr, "gRPC server listening");

            Server::builder()
                .add_service(service.into_server())
                .serve_with_shutdown(addr, shutdown_signal())
                .await?;
        } else {
            info!("gRPC server disabled, waiting for shutdown signal");
            shutdown_signal().await;
        }

        // ── 8. Shutdown ──────────────────────────────────────────
        hub_handle.abort();
        info!("POLKU shutdown complete");

        Ok(())
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialise the tracing subscriber based on config.
fn init_tracing(config: &Config) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| config.log_level.clone().into());

    let registry = tracing_subscriber::registry().with(env_filter);

    match config.log_format {
        LogFormat::Json => {
            registry
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        LogFormat::Pretty => {
            registry.with(tracing_subscriber::fmt::layer()).init();
        }
    }
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM.
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
