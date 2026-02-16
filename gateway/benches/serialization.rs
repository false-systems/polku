//! Boundary conversion benchmarks
//!
//! Measures Message <-> Event conversion at gRPC boundaries.
//! After the Message-first refactor, these conversions only happen at
//! protocol boundaries (gRPC ingestion/emission), NOT in the hot path.

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use polku_core::Event;
use polku_gateway::Message;
use std::collections::HashMap;

fn make_message() -> Message {
    Message::new(
        "benchmark-service",
        "user.created",
        Bytes::from(r#"{"user_id": 12345, "email": "test@example.com", "name": "Test User"}"#),
    )
    .with_metadata("key1", "value1")
    .with_metadata("key2", "value2")
    .with_metadata("trace_id", "abc123def456")
    .with_routes(vec!["output-1".into(), "output-2".into()])
}

fn make_event() -> Event {
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());
    metadata.insert("trace_id".to_string(), "abc123def456".to_string());

    Event {
        id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
        timestamp_unix_ns: 1704067200000000000,
        source: "benchmark-service".to_string(),
        event_type: "user.created".to_string(),
        metadata,
        payload: r#"{"user_id": 12345, "email": "test@example.com", "name": "Test User"}"#
            .as_bytes()
            .to_vec(),
        route_to: vec!["output-1".to_string(), "output-2".to_string()],
        severity: 0,
        outcome: 0,
        data: None,
    }
}

fn bench_boundary_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("boundary_conversion");
    group.throughput(Throughput::Elements(1000));

    // Message -> Event at gRPC emission boundary
    group.bench_function("message_to_event", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let msg = make_message();
                let _event: Event = msg.into();
            }
        })
    });

    // Event -> Message at gRPC ingestion boundary
    group.bench_function("event_to_message", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let event = make_event();
                let _msg: Message = event.into();
            }
        })
    });

    group.finish();
}

fn bench_message_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_clone");
    group.throughput(Throughput::Elements(1000));

    // Demonstrate zero-copy: cloning Message should be cheap (Arc-based Bytes)
    group.bench_function("clone_message", |b| {
        let msg = make_message();
        b.iter(|| {
            for _ in 0..1000 {
                let _cloned = msg.clone();
            }
        })
    });

    // Compare with Event clone (Vec<u8> payload = full copy)
    group.bench_function("clone_event", |b| {
        let event = make_event();
        b.iter(|| {
            for _ in 0..1000 {
                let _cloned = event.clone();
            }
        })
    });

    group.finish();
}

fn bench_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_sizes");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Bytes(size as u64 * 100)); // 100 messages

        let payload = vec![b'x'; size];
        let msg = Message::new("bench", "test", Bytes::from(payload));

        group.bench_function(format!("clone_{}b_payload", size), |b| {
            b.iter(|| {
                for _ in 0..100 {
                    let _cloned = msg.clone();
                }
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_boundary_conversion,
    bench_message_clone,
    bench_payload_sizes
);
criterion_main!(benches);
