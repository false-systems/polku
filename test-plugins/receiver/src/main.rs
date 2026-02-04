//! Test Receiver Plugin
//!
//! A simple gateway receiver that stores all received events for verification.
//! Used for black-box testing of POLKU.
//!
//! Implements the Gateway service to receive events from POLKU's gRPC emitter.
//!
//! Features:
//! - Stores all received events in memory
//! - Tracks success/failure counts
//! - Can be configured to reject certain events (for error testing)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

#[allow(clippy::large_enum_variant)]
pub mod proto {
    include!("proto/polku.v1.rs");
}

use proto::gateway_server::{Gateway, GatewayServer};
use proto::{
    Ack, AckError, ComponentHealth, HealthRequest, HealthResponse, IngestBatch, IngestEvent,
    ingest_batch, ingest_event,
};

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
    #[allow(dead_code)]
    pub fn success_count(&self) -> u64 {
        self.success_count.load(Ordering::SeqCst)
    }

    /// Get count of rejected events
    #[allow(dead_code)]
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::SeqCst)
    }

    /// Get all received events
    #[allow(dead_code)]
    pub fn get_all(&self) -> Vec<polku_core::Event> {
        self.events.read().values().cloned().collect()
    }

    /// Get a specific event by ID
    #[allow(dead_code)]
    pub fn get(&self, id: &str) -> Option<polku_core::Event> {
        self.events.read().get(id).cloned()
    }

    /// Clear all received events
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.events.write().clear();
        self.success_count.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
    }

    /// Configure to reject specific event IDs
    #[allow(dead_code)]
    pub fn reject_ids(&self, ids: Vec<String>) {
        *self.reject_ids.write() = ids;
    }

    /// Configure to reject events with empty ID
    #[allow(dead_code)]
    pub fn set_reject_empty_id(&self, reject: bool) {
        *self.reject_empty_id.write() = reject;
    }

    /// Configure to reject events with empty event_type
    #[allow(dead_code)]
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

/// The receiver gateway service - implements Gateway to receive from POLKU's gRPC emitter
pub struct ReceiverGateway {
    storage: Arc<ReceivedEvents>,
}

impl ReceiverGateway {
    pub fn new(storage: Arc<ReceivedEvents>) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl Gateway for ReceiverGateway {
    type StreamEventsStream = Pin<Box<dyn Stream<Item = Result<Ack, Status>> + Send>>;

    async fn stream_events(
        &self,
        request: Request<Streaming<IngestBatch>>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let mut stream = request.into_inner();
        let storage = self.storage.clone();

        let output = async_stream::try_stream! {
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;

                let mut event_ids = Vec::new();
                let mut errors = Vec::new();

                // Extract events from the batch payload
                if let Some(payload) = batch.payload {
                    match payload {
                        ingest_batch::Payload::Events(event_payload) => {
                            for event in event_payload.events {
                                let event_id = event.id.clone();
                                match storage.store(event) {
                                    Ok(()) => event_ids.push(event_id),
                                    Err((id, reason)) => {
                                        errors.push(AckError {
                                            event_id: id,
                                            code: "REJECTED".to_string(),
                                            message: reason,
                                        });
                                    }
                                }
                            }
                        }
                        ingest_batch::Payload::Raw(_) => {
                            // We don't support raw payloads in the test receiver
                            errors.push(AckError {
                                event_id: String::new(),
                                code: "UNSUPPORTED".to_string(),
                                message: "Raw payloads not supported".to_string(),
                            });
                        }
                    }
                }

                info!(
                    success = event_ids.len(),
                    failed = errors.len(),
                    "Processed batch"
                );

                yield Ack {
                    event_ids,
                    errors,
                    buffer_size: 0,
                    buffer_capacity: 100000,
                    middleware_stats: None,
                };
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn send_event(&self, request: Request<IngestEvent>) -> Result<Response<Ack>, Status> {
        let ingest = request.into_inner();
        let mut event_ids = Vec::new();
        let mut errors = Vec::new();

        if let Some(payload) = ingest.payload {
            match payload {
                ingest_event::Payload::Event(event) => {
                    let event_id = event.id.clone();
                    match self.storage.store(event) {
                        Ok(()) => event_ids.push(event_id),
                        Err((id, reason)) => {
                            errors.push(AckError {
                                event_id: id,
                                code: "REJECTED".to_string(),
                                message: reason,
                            });
                        }
                    }
                }
                ingest_event::Payload::Raw(_) => {
                    errors.push(AckError {
                        event_id: String::new(),
                        code: "UNSUPPORTED".to_string(),
                        message: "Raw payloads not supported".to_string(),
                    });
                }
            }
        }

        Ok(Response::new(Ack {
            event_ids,
            errors,
            buffer_size: 0,
            buffer_capacity: 100000,
            middleware_stats: None,
        }))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let mut components = HashMap::new();
        components.insert(
            "storage".to_string(),
            ComponentHealth {
                healthy: true,
                message: format!(
                    "Received {} events ({} rejected)",
                    self.storage.success_count.load(Ordering::SeqCst),
                    self.storage.failure_count.load(Ordering::SeqCst)
                ),
            },
        );

        Ok(Response::new(HealthResponse {
            healthy: true,
            components,
            uptime_seconds: 0,
            events_processed: self.storage.success_count.load(Ordering::SeqCst),
        }))
    }
}

/// Start the receiver gateway server
pub async fn start_server(
    addr: SocketAddr,
    storage: Arc<ReceivedEvents>,
) -> Result<(), Box<dyn std::error::Error>> {
    let gateway = ReceiverGateway::new(storage);

    info!(%addr, "Starting test receiver plugin");

    tonic::transport::Server::builder()
        .add_service(GatewayServer::new(gateway))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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
