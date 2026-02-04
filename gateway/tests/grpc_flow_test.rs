//! Real gRPC flow tests
//!
//! These tests start an actual gRPC server, send events through it,
//! and verify they come out the other end correctly.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
use polku_gateway::{
    Emitter, Event, PluginError,
    buffer::RingBuffer,
    hub::Hub,
    proto::{IngestEvent, gateway_client::GatewayClient, ingest_event},
    registry::PluginRegistry,
    server::GatewayService,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;

// ============================================================================
// Collecting Emitter - captures all events for verification
// ============================================================================

#[derive(Clone)]
struct CollectingEmitter {
    events: Arc<Mutex<Vec<Event>>>,
    count: Arc<AtomicU64>,
}

impl CollectingEmitter {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn collected(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Emitter for CollectingEmitter {
    fn name(&self) -> &'static str {
        "collector"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        let mut collected = self.events.lock().unwrap();
        for event in events {
            collected.push(event.clone());
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

async fn start_server_with_collector()
-> (SocketAddr, CollectingEmitter, tokio::task::JoinHandle<()>) {
    let collector = CollectingEmitter::new();

    // Create Hub with collector
    let hub = Hub::new()
        .buffer_capacity(1000)
        .flush_interval_ms(10)
        .emitter(collector.clone());

    let (_, sender, runner) = hub.build();

    // Find available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Create gateway service with hub
    let buffer = Arc::new(RingBuffer::new(1000));
    let registry = Arc::new(PluginRegistry::new());
    let service = GatewayService::with_hub(buffer, registry, sender);

    // Start server
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_server())
            .serve(addr)
            .await
            .ok();
    });

    // Start hub runner
    tokio::spawn(async move {
        runner.run().await;
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, collector, server_handle)
}

fn make_event(id: &str, source: &str, event_type: &str, payload: &str) -> Event {
    Event {
        id: id.to_string(),
        source: source.to_string(),
        event_type: event_type.to_string(),
        timestamp_unix_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        payload: payload.as_bytes().to_vec(),
        metadata: HashMap::new(),
        route_to: vec![],
        severity: 0,
        outcome: 0,
        data: None,
    }
}

fn make_ebpf_event(id: usize, syscall: &str) -> Event {
    let payload = format!(
        r#"{{"syscall":"{}","pid":{},"tid":{},"retval":0}}"#,
        syscall,
        id % 10000,
        id % 10000 + 1
    );
    make_event(
        &format!("ebpf-{}", id),
        "ebpf.raw",
        &format!("syscall.{}", syscall),
        &payload,
    )
}

fn make_broken_event(id: usize) -> Event {
    match id % 4 {
        0 => Event {
            id: String::new(), // empty ID
            source: "broken".into(),
            event_type: "broken".into(),
            ..Default::default()
        },
        1 => Event {
            id: format!("broken-{}", id),
            source: String::new(), // empty source
            event_type: "broken".into(),
            ..Default::default()
        },
        2 => Event {
            id: format!("broken-{}", id),
            source: "broken".into(),
            event_type: String::new(), // empty type
            ..Default::default()
        },
        _ => Event {
            id: format!("broken-{}", id),
            source: "broken".into(),
            event_type: "broken".into(),
            payload: vec![0xFF, 0xFE, 0x00], // binary garbage
            ..Default::default()
        },
    }
}

// ============================================================================
// REAL FLOW TESTS
// ============================================================================

/// Send 100 events through real gRPC, verify they arrive
#[tokio::test]
async fn test_grpc_100_events_flow_through() {
    let (addr, collector, _handle) = start_server_with_collector().await;

    let mut client = GatewayClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect");

    // Send 100 events
    for i in 0..100 {
        let event = make_ebpf_event(i, "read");
        let request = IngestEvent {
            source: "test".into(),
            cluster: "test".into(),
            payload: Some(ingest_event::Payload::Event(event)),
            format: String::new(),
        };

        let response = client.send_event(request).await;
        assert!(response.is_ok(), "Event {} failed: {:?}", i, response);
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    let collected = collector.collected();
    eprintln!("Sent 100, received {}", collected.len());

    assert_eq!(
        collected.len(),
        100,
        "Expected 100 events, got {}",
        collected.len()
    );

    // Verify content
    for (i, event) in collected.iter().enumerate() {
        assert_eq!(event.source, "ebpf.raw", "Event {} wrong source", i);
        assert!(
            event.event_type.starts_with("syscall."),
            "Event {} wrong type",
            i
        );
    }
}

/// Send 20,000 events (19,000 valid + 1,000 broken)
#[tokio::test]
async fn test_grpc_20k_events_with_broken() {
    let (addr, collector, _handle) = start_server_with_collector().await;

    let mut client = GatewayClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect");

    let total = 20_000usize;
    let broken_count = 1_000usize;
    let syscalls = ["read", "write", "open", "close", "stat", "mmap", "execve"];

    eprintln!(
        "Sending {} events ({} valid, {} broken)...",
        total,
        total - broken_count,
        broken_count
    );

    let mut sent = 0;
    let mut errors = 0;

    for i in 0..total {
        let event = if i % 20 == 19 {
            // Every 20th is broken
            make_broken_event(i)
        } else {
            make_ebpf_event(i, syscalls[i % syscalls.len()])
        };

        let request = IngestEvent {
            source: "test".into(),
            cluster: "test".into(),
            payload: Some(ingest_event::Payload::Event(event)),
            format: String::new(),
        };

        match client.send_event(request).await {
            Ok(_) => sent += 1,
            Err(e) => {
                errors += 1;
                if errors < 5 {
                    eprintln!("Error on event {}: {:?}", i, e);
                }
            }
        }
    }

    eprintln!("Sent: {}, Errors: {}", sent, errors);

    // Wait for all flushes
    tokio::time::sleep(Duration::from_millis(500)).await;

    let collected = collector.collected();
    eprintln!("Collected: {}", collected.len());

    // Count by source
    let valid_count = collected.iter().filter(|e| e.source == "ebpf.raw").count();
    let broken_count = collected.iter().filter(|e| e.source == "broken").count();

    eprintln!("Valid: {}, Broken: {}", valid_count, broken_count);

    // All sent events should arrive (gateway accepts all valid protobuf)
    assert_eq!(
        collected.len(),
        sent,
        "Lost events! sent={} collected={}",
        sent,
        collected.len()
    );
}

/// High throughput: 50k events as fast as possible
#[tokio::test]
async fn test_grpc_50k_throughput() {
    let (addr, collector, _handle) = start_server_with_collector().await;

    let mut client = GatewayClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect");

    let total = 50_000usize;

    eprintln!("Sending {} events...", total);
    let start = std::time::Instant::now();

    for i in 0..total {
        let event = make_ebpf_event(i, "write");
        let request = IngestEvent {
            source: "test".into(),
            cluster: "test".into(),
            payload: Some(ingest_event::Payload::Event(event)),
            format: String::new(),
        };

        client.send_event(request).await.ok();
    }

    let send_time = start.elapsed();
    let send_rate = total as f64 / send_time.as_secs_f64();
    eprintln!(
        "Sent {} in {:?} ({:.0} events/sec)",
        total, send_time, send_rate
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let collected = collector.count() as usize;
    eprintln!("Collected: {}", collected);

    // Verify throughput is reasonable (at least 1k/sec - debug builds are slower)
    assert!(send_rate > 1_000.0, "Too slow: {:.0} events/sec", send_rate);

    // Verify we got most events
    let received_pct = (collected as f64 / total as f64) * 100.0;
    eprintln!("Received {:.1}% of events", received_pct);
    assert!(
        received_pct > 95.0,
        "Lost too many events: only {:.1}% received",
        received_pct
    );
}

/// Concurrent clients sending events
#[tokio::test]
async fn test_grpc_concurrent_clients() {
    let (addr, collector, _handle) = start_server_with_collector().await;

    let num_clients = 4;
    let events_per_client = 1000;

    eprintln!(
        "Starting {} concurrent clients, {} events each",
        num_clients, events_per_client
    );

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = GatewayClient::connect(format!("http://{}", addr))
                .await
                .expect("Failed to connect");

            let mut sent = 0;
            for i in 0..events_per_client {
                let event = make_event(
                    &format!("c{}-{}", client_id, i),
                    &format!("client-{}", client_id),
                    "test.concurrent",
                    &format!("payload-{}", i),
                );

                let request = IngestEvent {
                    source: format!("client-{}", client_id),
                    cluster: "test".into(),
                    payload: Some(ingest_event::Payload::Event(event)),
                    format: String::new(),
                };

                if client.send_event(request).await.is_ok() {
                    sent += 1;
                }
            }
            sent
        }));
    }

    let mut total_sent = 0;
    for handle in handles {
        total_sent += handle.await.unwrap();
    }

    eprintln!("Total sent: {}", total_sent);

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    let collected = collector.collected();
    eprintln!("Collected: {}", collected.len());

    // Count by client
    for client_id in 0..num_clients {
        let count = collected
            .iter()
            .filter(|e| e.source == format!("client-{}", client_id))
            .count();
        eprintln!("  Client {}: {} events", client_id, count);
    }

    assert_eq!(collected.len(), total_sent, "Lost events!");
}

/// Verify event content survives the journey
#[tokio::test]
async fn test_grpc_content_integrity() {
    let (addr, collector, _handle) = start_server_with_collector().await;

    let mut client = GatewayClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect");

    // Create events with specific content
    let test_cases = vec![
        ("id-1", "source-alpha", "type.one", r#"{"key":"value"}"#),
        ("id-2", "source-beta", "type.two", "plain text payload"),
        ("id-3", "服务", "事件.中文", "unicode 🚀 payload"),
        ("id-4", "binary-source", "binary.type", "\x00\x01\x02\x03"),
    ];

    for (id, source, event_type, payload) in &test_cases {
        let event = make_event(id, source, event_type, payload);
        let request = IngestEvent {
            source: source.to_string(),
            cluster: "test".into(),
            payload: Some(ingest_event::Payload::Event(event)),
            format: String::new(),
        };

        client.send_event(request).await.expect("Failed to send");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let collected = collector.collected();

    // Debug: print what we actually received
    eprintln!("Collected {} events:", collected.len());
    for (i, e) in collected.iter().enumerate() {
        eprintln!(
            "  [{}] id='{}' source='{}' type='{}'",
            i, e.id, e.source, e.event_type
        );
    }

    assert_eq!(collected.len(), 4, "Expected 4 events");

    // Verify event IDs are preserved (fixed in MessageId)
    for (id, source, event_type, payload) in &test_cases {
        let found = collected.iter().find(|e| e.id == *id);
        assert!(
            found.is_some(),
            "Event with id='{}' not found! IDs in collection: {:?}",
            id,
            collected.iter().map(|e| &e.id).collect::<Vec<_>>()
        );

        let event = found.unwrap();
        assert_eq!(event.source, *source, "Wrong source for id={}", id);
        assert_eq!(event.event_type, *event_type, "Wrong type for id={}", id);
        assert_eq!(
            String::from_utf8_lossy(&event.payload),
            *payload,
            "Wrong payload for id={}",
            id
        );
    }

    eprintln!("Content integrity checks passed - IDs preserved!");
}
