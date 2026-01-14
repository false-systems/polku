//! Plugin Discovery for POLKU
//!
//! Allows external plugins to self-register with POLKU at runtime.
//! POLKU exposes a `PluginRegistry` gRPC service that plugins call on startup.
//!
//! # Architecture
//!
//! ```text
//! Plugin (Go/Python/etc) ──► Register() ──► POLKU creates ExternalIngestor/Emitter
//!                         ◄── plugin_id ──
//!
//! Plugin ──► Heartbeat(plugin_id) ──► POLKU updates last_seen
//!
//! Plugin ──► Unregister(plugin_id) ──► POLKU removes from registry
//! ```
//!
//! # Example
//!
//! ```ignore
//! use polku_gateway::discovery::DiscoveryServer;
//!
//! // Start discovery server alongside main gateway
//! let discovery = DiscoveryServer::new(registry.clone());
//!
//! // In the gRPC server setup:
//! tonic::transport::Server::builder()
//!     .add_service(PluginRegistryServer::new(discovery))
//!     .serve(addr)
//!     .await?;
//! ```

mod server;

pub use server::DiscoveryServer;
