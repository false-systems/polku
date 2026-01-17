//! POLKU gRPC Client for testing

use crate::proto::gateway_client::GatewayClient;
use crate::proto::{Ack, IngestEvent};
use polku_core::Event;
use tonic::transport::Channel;

/// Client for sending events to POLKU
pub struct PolkuClient {
    client: GatewayClient<Channel>,
    source: String,
    cluster: String,
}

impl PolkuClient {
    /// Connect to POLKU at the given address
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let client = GatewayClient::connect(addr.to_string()).await?;
        Ok(Self {
            client,
            source: "e2e-test".to_string(),
            cluster: "test-cluster".to_string(),
        })
    }

    /// Send a single event using unary RPC
    pub async fn send_event(&mut self, event: Event) -> Result<Ack, tonic::Status> {
        let request = IngestEvent {
            source: self.source.clone(),
            cluster: self.cluster.clone(),
            payload: Some(crate::proto::ingest_event::Payload::Event(event)),
            format: String::new(),
        };

        let response = self.client.send_event(request).await?;
        Ok(response.into_inner())
    }

    /// Send multiple events one by one (TODO: use streaming API)
    pub async fn send_events(&mut self, events: Vec<Event>) -> Result<Ack, tonic::Status> {
        let mut total_ids = Vec::new();
        let mut total_errors = Vec::new();

        for event in events {
            match self.send_event(event).await {
                Ok(ack) => {
                    total_ids.extend(ack.event_ids);
                    total_errors.extend(ack.errors);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(Ack {
            event_ids: total_ids,
            errors: total_errors,
            buffer_size: 0,
            buffer_capacity: 0,
            middleware_stats: None,
        })
    }

    /// Create a test event with random ID
    pub fn make_event(event_type: &str) -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            source: "e2e-test".to_string(),
            event_type: event_type.to_string(),
            timestamp_unix_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            ..Default::default()
        }
    }

    /// Create a broken event (empty ID - should be rejected)
    pub fn make_broken_event_empty_id() -> Event {
        Event {
            id: String::new(),
            source: "e2e-test".to_string(),
            event_type: "test.broken".to_string(),
            ..Default::default()
        }
    }

    /// Create a broken event (empty event_type - should be rejected)
    pub fn make_broken_event_empty_type() -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            source: "e2e-test".to_string(),
            event_type: String::new(),
            ..Default::default()
        }
    }

    /// Create N valid events
    pub fn make_events(n: usize, event_type: &str) -> Vec<Event> {
        (0..n).map(|_| Self::make_event(event_type)).collect()
    }

    /// Create a batch with N% broken events
    pub fn make_mixed_batch(total: usize, broken_percent: f32) -> Vec<Event> {
        let broken_count = (total as f32 * broken_percent / 100.0).round() as usize;
        let valid_count = total - broken_count;

        let mut events = Vec::with_capacity(total);

        for _ in 0..valid_count {
            events.push(Self::make_event("test.valid"));
        }

        for i in 0..broken_count {
            if i % 2 == 0 {
                events.push(Self::make_broken_event_empty_id());
            } else {
                events.push(Self::make_broken_event_empty_type());
            }
        }

        events
    }
}
