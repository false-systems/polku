//! Test Receiver Plugin
//!
//! A simple emitter plugin that stores all received events for verification.
//! Used for black-box testing of POLKU.
//!
//! Features:
//! - Stores all received events in memory
//! - Tracks success/failure counts
//! - Can be configured to reject certain events (for error testing)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tracing::info;

pub mod proto {
    include!("proto/polku.v1.rs");
}

use proto::emitter_plugin_server::{EmitterPlugin, EmitterPluginServer};
use proto::{EmitError, EmitRequest, EmitResponse, PluginHealthResponse, PluginInfo, PluginType, ShutdownResponse};

/// Storage for received events - uses polku_core::Event directly
#[derive(Default)]
pub struct ReceivedEvents {
    /// All received events, keyed by event ID
    events: RwLock<HashMap<String, polku_core::Event>>,
    /// Count of successful emissions
    success_count: AtomicU64,
    /// Count of failed emissions (rejected)
    failure_count: AtomicU64,
    /// Event IDs to reject (for testing error paths)
    reject_ids: RwLock<Vec<String>>,
    /// Reject events with empty ID
    reject_empty_id: RwLock<bool>,
    /// Reject events with empty event_type
    reject_empty_type: RwLock<bool>,
}

impl ReceivedEvents {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Get count of successfully received events
    pub fn success_count(&self) -> u64 {
        self.success_count.load(Ordering::SeqCst)
    }

    /// Get count of rejected events
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::SeqCst)
    }

    /// Get all received events
    pub fn get_all(&self) -> Vec<polku_core::Event> {
        self.events.read().values().cloned().collect()
    }

    /// Get a specific event by ID
    pub fn get(&self, id: &str) -> Option<polku_core::Event> {
        self.events.read().get(id).cloned()
    }

    /// Clear all received events
    pub fn clear(&self) {
        self.events.write().clear();
        self.success_count.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
    }

    /// Configure to reject specific event IDs
    pub fn reject_ids(&self, ids: Vec<String>) {
        *self.reject_ids.write() = ids;
    }

    /// Configure to reject events with empty ID
    pub fn set_reject_empty_id(&self, reject: bool) {
        *self.reject_empty_id.write() = reject;
    }

    /// Configure to reject events with empty event_type
    pub fn set_reject_empty_type(&self, reject: bool) {
        *self.reject_empty_type.write() = reject;
    }

    /// Check if an event should be rejected
    fn should_reject(&self, event: &polku_core::Event) -> Option<String> {
        // Check empty ID
        if *self.reject_empty_id.read() && event.id.is_empty() {
            return Some("Event ID is empty".to_string());
        }

        // Check empty event_type
        if *self.reject_empty_type.read() && event.event_type.is_empty() {
            return Some("Event type is empty".to_string());
        }

        // Check reject list
        if self.reject_ids.read().contains(&event.id) {
            return Some(format!("Event ID {} is in reject list", event.id));
        }

        None
    }

    /// Store an event (returns error reason if rejected)
    fn store(&self, event: polku_core::Event) -> Result<(), (String, String)> {
        if let Some(reason) = self.should_reject(&event) {
            self.failure_count.fetch_add(1, Ordering::SeqCst);
            return Err((event.id.clone(), reason));
        }

        let id = event.id.clone();
        self.events.write().insert(id, event);
        self.success_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The receiver plugin service
pub struct ReceiverPlugin {
    storage: Arc<ReceivedEvents>,
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ReceiverPlugin {
    pub fn new(storage: Arc<ReceivedEvents>) -> Self {
        Self {
            storage,
            shutdown_tx: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_shutdown(mut self, tx: oneshot::Sender<()>) -> Self {
        self.shutdown_tx = Some(tx);
        self
    }
}

#[tonic::async_trait]
impl EmitterPlugin for ReceiverPlugin {
    async fn info(&self, _: Request<()>) -> Result<Response<PluginInfo>, Status> {
        Ok(Response::new(PluginInfo {
            name: "test-receiver".to_string(),
            version: "1.0.0".to_string(),
            r#type: PluginType::Emitter as i32,
            description: "Test receiver that stores events for verification".to_string(),
            sources: vec![],
            emitter_name: "test-receiver".to_string(),
            capabilities: vec!["store".to_string(), "query".to_string()],
        }))
    }

    async fn health(&self, _: Request<()>) -> Result<Response<PluginHealthResponse>, Status> {
        Ok(Response::new(PluginHealthResponse {
            healthy: true,
            message: format!(
                "Received {} events ({} rejected)",
                self.storage.success_count(),
                self.storage.failure_count()
            ),
            components: Default::default(),
        }))
    }

    async fn emit(&self, request: Request<EmitRequest>) -> Result<Response<EmitResponse>, Status> {
        let req = request.into_inner();
        let mut failed_ids = Vec::new();
        let mut errors = Vec::new();
        let mut success_count = 0i64;

        for proto_event in req.events {
            // polku_core::Event IS the proto event (same type via extern_path)
            // Just store it directly
            let event = proto_event;

            match self.storage.store(event) {
                Ok(()) => success_count += 1,
                Err((event_id, reason)) => {
                    failed_ids.push(event_id.clone());
                    errors.push(EmitError {
                        event_id,
                        message: reason,
                        retryable: false,
                    });
                }
            }
        }

        info!(
            success = success_count,
            failed = failed_ids.len(),
            "Processed batch"
        );

        Ok(Response::new(EmitResponse {
            success_count,
            failed_event_ids: failed_ids,
            errors,
        }))
    }

    async fn shutdown(&self, _: Request<()>) -> Result<Response<ShutdownResponse>, Status> {
        info!("Shutdown requested");
        Ok(Response::new(ShutdownResponse {
            success: true,
            message: "Shutting down".to_string(),
        }))
    }
}

/// Start the receiver plugin server
pub async fn start_server(
    addr: SocketAddr,
    storage: Arc<ReceivedEvents>,
) -> Result<(), Box<dyn std::error::Error>> {
    let plugin = ReceiverPlugin::new(storage);

    info!(%addr, "Starting test receiver plugin");

    tonic::transport::Server::builder()
        .add_service(EmitterPluginServer::new(plugin))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr: SocketAddr = std::env::var("RECEIVER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9001".to_string())
        .parse()?;

    let storage = ReceivedEvents::new();
    start_server(addr, storage).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_basic() {
        let storage = ReceivedEvents::new();

        let event = polku_core::Event {
            id: "test-1".to_string(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ..Default::default()
        };

        assert!(storage.store(event).is_ok());
        assert_eq!(storage.success_count(), 1);
        assert_eq!(storage.failure_count(), 0);

        let retrieved = storage.get("test-1").unwrap();
        assert_eq!(retrieved.source, "test");
    }

    #[test]
    fn test_storage_reject_empty_id() {
        let storage = ReceivedEvents::new();
        storage.set_reject_empty_id(true);

        let event = polku_core::Event {
            id: "".to_string(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ..Default::default()
        };

        assert!(storage.store(event).is_err());
        assert_eq!(storage.success_count(), 0);
        assert_eq!(storage.failure_count(), 1);
    }

    #[test]
    fn test_storage_reject_specific_ids() {
        let storage = ReceivedEvents::new();
        storage.reject_ids(vec!["bad-1".to_string(), "bad-2".to_string()]);

        let good = polku_core::Event {
            id: "good-1".to_string(),
            ..Default::default()
        };
        assert!(storage.store(good).is_ok());

        let bad = polku_core::Event {
            id: "bad-1".to_string(),
            ..Default::default()
        };
        assert!(storage.store(bad).is_err());

        assert_eq!(storage.success_count(), 1);
        assert_eq!(storage.failure_count(), 1);
    }
}
