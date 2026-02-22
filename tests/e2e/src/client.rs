//! POLKU gRPC Client for testing

use crate::proto::gateway_client::GatewayClient;
use crate::proto::ingest_batch;
use crate::proto::{Ack, EventPayload, IngestBatch, IngestEvent};
use polku_core::{Event, Outcome, Severity};
use tokio_stream::StreamExt;
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

    /// Send multiple events one by one (slow - use stream_events for throughput)
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

    /// Stream events using bidirectional streaming RPC (high throughput)
    /// Sends events in batches over a single stream connection
    pub async fn stream_events(
        &mut self,
        events: Vec<Event>,
        batch_size: usize,
    ) -> Result<Ack, tonic::Status> {
        assert!(batch_size > 0, "batch_size must be greater than 0");

        let source = self.source.clone();
        let cluster = self.cluster.clone();

        // NOTE: We collect batches into a Vec here intentionally for performance measurement.
        // This isolates the streaming RPC performance from event generation overhead.
        // In production code, consider using an iterator-based approach for memory efficiency.
        let batches: Vec<IngestBatch> = events
            .chunks(batch_size)
            .map(|chunk| IngestBatch {
                source: source.clone(),
                cluster: cluster.clone(),
                payload: Some(ingest_batch::Payload::Events(EventPayload {
                    events: chunk.to_vec(),
                })),
            })
            .collect();

        // Stream the batches
        let stream = tokio_stream::iter(batches);
        let mut response_stream = self.client.stream_events(stream).await?.into_inner();

        // Collect all acks
        let mut total_ids = Vec::new();
        let mut total_errors = Vec::new();

        while let Some(ack_result) = response_stream.next().await {
            let ack = ack_result?;
            total_ids.extend(ack.event_ids);
            total_errors.extend(ack.errors);
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
                .as_nanos()
                .try_into()
                .unwrap_or(i64::MAX),
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

    // ========================================================================
    // REAL PROTOBUF EVENTS - typed eBPF/kernel events
    // ========================================================================

    /// Create a kernel syscall event with payload data
    pub fn make_kernel_syscall_event(syscall: &str, pid: u32, retval: i64) -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            source: "ebpf.raw".to_string(),
            event_type: format!("kernel.syscall.{}", syscall),
            timestamp_unix_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .try_into()
                .unwrap_or(i64::MAX),
            severity: Severity::Info as i32,
            outcome: if retval >= 0 {
                Outcome::Success as i32
            } else {
                Outcome::Failure as i32
            },
            payload: format!(
                r#"{{"syscall":"{}","pid":{},"retval":{}}}"#,
                syscall, pid, retval
            ).into_bytes(),
            ..Default::default()
        }
    }

    /// Create a network event with payload data
    pub fn make_network_event(protocol: &str, src_ip: &str, dst_ip: &str, dst_port: u32) -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            source: "tapio".to_string(),
            event_type: format!("network.{}.connect", protocol.to_lowercase()),
            timestamp_unix_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .try_into()
                .unwrap_or(i64::MAX),
            severity: Severity::Info as i32,
            outcome: Outcome::Success as i32,
            payload: format!(
                r#"{{"protocol":"{}","src":"{}","dst":"{}","port":{}}}"#,
                protocol, src_ip, dst_ip, dst_port
            ).into_bytes(),
            ..Default::default()
        }
    }

    /// Create N real eBPF kernel events with typed data
    pub fn make_kernel_events(n: usize) -> Vec<Event> {
        let syscalls = ["read", "write", "open", "close", "execve", "mmap", "stat"];
        (0..n)
            .map(|i| {
                let syscall = syscalls[i % syscalls.len()];
                let pid = (1000 + i % 100) as u32;
                let retval = if i % 10 == 9 { -1 } else { (i % 1000) as i64 };
                Self::make_kernel_syscall_event(syscall, pid, retval)
            })
            .collect()
    }

    /// Create N network events with typed data
    pub fn make_network_events(n: usize) -> Vec<Event> {
        (0..n)
            .map(|i| {
                let protocol = if i % 2 == 0 { "TCP" } else { "UDP" };
                let dst_port = (80 + i % 10) as u32;
                Self::make_network_event(
                    protocol,
                    &format!("10.0.0.{}", i % 256),
                    &format!("192.168.1.{}", i % 256),
                    dst_port,
                )
            })
            .collect()
    }

    /// Create a mixed batch of real eBPF events (kernel + network)
    pub fn make_real_ebpf_batch(total: usize) -> Vec<Event> {
        let mut events = Vec::with_capacity(total);
        let kernel_count = total * 7 / 10; // 70% kernel events
        let network_count = total - kernel_count; // 30% network events

        events.extend(Self::make_kernel_events(kernel_count));
        events.extend(Self::make_network_events(network_count));

        events
    }
}
