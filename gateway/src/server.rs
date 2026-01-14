//! gRPC server for POLKU Gateway
//!
//! Implements the Gateway service defined in proto/v1/gateway.proto
//!
//! Supports two modes:
//! - Pass-through: Clients send pre-formatted Events, no transformation needed
//! - Transform: Clients send raw bytes, InputPlugins transform them to Events

use crate::buffer::RingBuffer;
use crate::emit::Event;
use crate::hub::MessageSender;
use crate::ingest::IngestContext;
use crate::message::Message;
use crate::proto::{
    Ack, ComponentHealth, HealthRequest, HealthResponse, IngestBatch, IngestEvent,
    gateway_server::{Gateway, GatewayServer},
    ingest_batch, ingest_event,
};
use crate::registry::PluginRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Gateway gRPC service implementation
pub struct GatewayService {
    buffer: Arc<RingBuffer>,
    registry: Arc<PluginRegistry>,
    start_time: Instant,
    events_processed: Arc<AtomicU64>,
    /// Optional Hub sender for forwarding events to the processing pipeline
    hub_sender: Option<MessageSender>,
}

impl GatewayService {
    /// Create a new GatewayService with buffer only (no plugins)
    pub fn new(buffer: Arc<RingBuffer>) -> Self {
        Self {
            buffer,
            registry: Arc::new(PluginRegistry::new()),
            start_time: Instant::now(),
            events_processed: Arc::new(AtomicU64::new(0)),
            hub_sender: None,
        }
    }

    /// Create a new GatewayService with buffer and plugin registry
    pub fn with_registry(buffer: Arc<RingBuffer>, registry: Arc<PluginRegistry>) -> Self {
        Self {
            buffer,
            registry,
            start_time: Instant::now(),
            events_processed: Arc::new(AtomicU64::new(0)),
            hub_sender: None,
        }
    }

    /// Create a new GatewayService with Hub sender for event forwarding
    ///
    /// When a Hub sender is provided, events are sent to the Hub pipeline
    /// for processing and emission to configured outputs. The buffer is
    /// still used for backpressure signaling in Ack responses.
    pub fn with_hub(
        buffer: Arc<RingBuffer>,
        registry: Arc<PluginRegistry>,
        hub_sender: MessageSender,
    ) -> Self {
        Self {
            buffer,
            registry,
            start_time: Instant::now(),
            events_processed: Arc::new(AtomicU64::new(0)),
            hub_sender: Some(hub_sender),
        }
    }

    /// Create a tonic server from this service
    pub fn into_server(self) -> GatewayServer<Self> {
        GatewayServer::new(self)
    }

    /// Process a single IngestEvent and return an Event
    #[allow(clippy::result_large_err)]
    fn process_event(&self, ingest: &IngestEvent) -> Result<Event, Status> {
        let source = &ingest.source;
        let cluster = &ingest.cluster;

        match &ingest.payload {
            Some(ingest_event::Payload::Event(event)) => {
                debug!(source = %source, id = %event.id, "Pass-through event");
                Ok(event.clone())
            }
            Some(ingest_event::Payload::Raw(data)) => {
                let ctx = IngestContext {
                    source,
                    cluster,
                    format: &ingest.format,
                };

                match self.registry.transform(&ctx, data) {
                    Ok(mut events) => {
                        if events.is_empty() {
                            return Err(Status::invalid_argument("Transform produced no events"));
                        }
                        // Return first event for unary RPC
                        if events.len() > 1 {
                            warn!(
                                source = %source,
                                count = events.len(),
                                "Transform produced multiple events, returning first only"
                            );
                        }
                        Ok(events.remove(0))
                    }
                    Err(e) => {
                        error!(source = %source, error = %e, "Transform failed");
                        Err(Status::unimplemented(format!(
                            "Transform failed for source '{}': {}",
                            source, e
                        )))
                    }
                }
            }
            None => Err(Status::invalid_argument("Event payload is empty")),
        }
    }
}

#[tonic::async_trait]
impl Gateway for GatewayService {
    type StreamEventsStream = ReceiverStream<Result<Ack, Status>>;

    async fn stream_events(
        &self,
        request: Request<Streaming<IngestBatch>>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let mut stream = request.into_inner();
        let buffer = Arc::clone(&self.buffer);
        let registry = Arc::clone(&self.registry);
        let events_processed = Arc::clone(&self.events_processed);
        let hub_sender = self.hub_sender.clone();

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Ok(Some(batch)) = stream.message().await {
                let source = batch.source.clone();

                // Process the batch
                let events = match process_batch_with_registry(&batch, &registry) {
                    Ok(events) => events,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        continue;
                    }
                };

                if events.is_empty() {
                    let ack = Ack {
                        event_ids: vec![],
                        errors: vec![],
                        buffer_size: buffer.len() as i64,
                        buffer_capacity: buffer.capacity() as i64,
                        middleware_stats: None,
                    };
                    if tx.send(Ok(ack)).await.is_err() {
                        break;
                    }
                    continue;
                }

                let event_ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
                let event_count = events.len();

                // Convert proto Events to internal Messages
                let messages: Vec<Message> = events.into_iter().map(Message::from).collect();

                // Send to Hub if configured (for processing and emission)
                // Otherwise just buffer (legacy behavior)
                if let Some(ref sender) = hub_sender {
                    let mut send_errors = 0;
                    for msg in messages {
                        if sender.try_send(msg).is_err() {
                            send_errors += 1;
                        }
                    }
                    if send_errors > 0 {
                        info!(dropped = send_errors, source = %source, "Hub channel full, events dropped");
                    }
                } else {
                    let dropped = buffer.push(messages);
                    if dropped > 0 {
                        info!(dropped = dropped, source = %source, "Buffer overflow, events dropped");
                    }
                }

                events_processed.fetch_add(event_count as u64, Ordering::Relaxed);

                let ack = Ack {
                    event_ids,
                    errors: vec![],
                    buffer_size: buffer.len() as i64,
                    buffer_capacity: buffer.capacity() as i64,
                    middleware_stats: None,
                };

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn send_event(&self, request: Request<IngestEvent>) -> Result<Response<Ack>, Status> {
        let ingest = request.into_inner();
        let event = self.process_event(&ingest)?;

        let event_id = event.id.clone();
        // Convert proto Event to internal Message
        let message: Message = event.into();

        // Send to Hub if configured, otherwise buffer
        if let Some(ref sender) = self.hub_sender {
            if sender.try_send(message).is_err() {
                return Err(Status::resource_exhausted(
                    "Hub channel full, event dropped",
                ));
            }
        } else {
            let dropped = self.buffer.push(vec![message]);
            if dropped > 0 {
                return Err(Status::resource_exhausted("Buffer full, event dropped"));
            }
        }

        self.events_processed.fetch_add(1, Ordering::Relaxed);

        Ok(Response::new(Ack {
            event_ids: vec![event_id],
            errors: vec![],
            buffer_size: self.buffer.len() as i64,
            buffer_capacity: self.buffer.capacity() as i64,
            middleware_stats: None,
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let buffer_health = ComponentHealth {
            healthy: !self.buffer.is_full(),
            message: format!("Buffer: {}/{}", self.buffer.len(), self.buffer.capacity()),
        };

        let mut components = HashMap::new();
        components.insert("buffer".to_string(), buffer_health);

        // Add output plugin health
        let output_health = self.registry.output_health().await;
        for (name, healthy) in output_health {
            components.insert(
                format!("output:{}", name),
                ComponentHealth {
                    healthy,
                    message: if healthy {
                        "OK".to_string()
                    } else {
                        "Unhealthy".to_string()
                    },
                },
            );
        }

        Ok(Response::new(HealthResponse {
            healthy: true,
            components,
            uptime_seconds: self.start_time.elapsed().as_secs() as i64,
            events_processed: self.events_processed.load(Ordering::Relaxed),
        }))
    }
}

/// Helper to process a batch with registry (used in spawned task)
#[allow(clippy::result_large_err)]
fn process_batch_with_registry(
    batch: &IngestBatch,
    registry: &PluginRegistry,
) -> Result<Vec<Event>, Status> {
    let source = &batch.source;
    let cluster = &batch.cluster;

    match &batch.payload {
        Some(ingest_batch::Payload::Events(payload)) => Ok(payload.events.clone()),
        Some(ingest_batch::Payload::Raw(payload)) => {
            let ctx = IngestContext {
                source,
                cluster,
                format: &payload.format,
            };

            registry.transform(&ctx, &payload.data).map_err(|e| {
                Status::unimplemented(format!("Transform failed for source '{}': {}", source, e))
            })
        }
        None => Ok(vec![]),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::proto::{EventPayload, RawPayload};

    fn make_service() -> GatewayService {
        let buffer = Arc::new(RingBuffer::new(100));
        GatewayService::new(buffer)
    }

    #[test]
    fn test_process_event_passthrough() {
        let svc = make_service();

        let event = Event {
            id: "test-id".into(),
            timestamp_unix_ns: 123,
            source: "test".into(),
            event_type: "test.event".into(),
            metadata: HashMap::new(),
            payload: vec![1, 2, 3],
            route_to: vec![],
            severity: 0,
            outcome: 0,
            data: None,
        };

        let ingest = IngestEvent {
            source: "client".into(),
            cluster: "prod".into(),
            format: String::new(),
            payload: Some(ingest_event::Payload::Event(event.clone())),
        };

        let result = svc.process_event(&ingest);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "test-id");
    }

    #[test]
    fn test_process_event_empty_payload() {
        let svc = make_service();

        let ingest = IngestEvent {
            source: "client".into(),
            cluster: "prod".into(),
            format: String::new(),
            payload: None,
        };

        let result = svc.process_event(&ingest);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_batch_passthrough() {
        let registry = PluginRegistry::new();

        let events = vec![
            Event {
                id: "e1".into(),
                timestamp_unix_ns: 1,
                source: "test".into(),
                event_type: "test".into(),
                metadata: HashMap::new(),
                payload: vec![],
                route_to: vec![],
                severity: 0,
                outcome: 0,
                data: None,
            },
            Event {
                id: "e2".into(),
                timestamp_unix_ns: 2,
                source: "test".into(),
                event_type: "test".into(),
                metadata: HashMap::new(),
                payload: vec![],
                route_to: vec![],
                severity: 0,
                outcome: 0,
                data: None,
            },
        ];

        let batch = IngestBatch {
            source: "client".into(),
            cluster: "prod".into(),
            payload: Some(ingest_batch::Payload::Events(EventPayload {
                events: events.clone(),
            })),
        };

        let result = process_batch_with_registry(&batch, &registry);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_process_batch_empty() {
        let registry = PluginRegistry::new();

        let batch = IngestBatch {
            source: "client".into(),
            cluster: "prod".into(),
            payload: None,
        };

        let result = process_batch_with_registry(&batch, &registry);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_process_batch_raw_no_plugin() {
        let registry = PluginRegistry::new();

        let batch = IngestBatch {
            source: "unknown-source".into(),
            cluster: "prod".into(),
            payload: Some(ingest_batch::Payload::Raw(RawPayload {
                format: "json".into(),
                data: b"{}".to_vec(),
            })),
        };

        let result = process_batch_with_registry(&batch, &registry);
        assert!(result.is_err()); // No plugin registered
    }
}
