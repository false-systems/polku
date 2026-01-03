//! Serialization benchmarks
//!
//! Measures Message <-> Event conversion overhead.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use polku_gateway::{Event, Message};
use std::collections::HashMap;

fn make_message() -> Message {
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());
    metadata.insert("trace_id".to_string(), "abc123def456".to_string());

    Message {
        id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
        timestamp: 1704067200000000000, // 2024-01-01 00:00:00 UTC
        source: "benchmark-service".to_string(),
        message_type: "user.created".to_string(),
        metadata,
        payload: Bytes::from(r#"{"user_id": 12345, "email": "test@example.com", "name": "Test User"}"#),
        route_to: vec!["output-1".to_string(), "output-2".to_string()],
    }
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
    }
}

fn bench_message_to_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");
    group.throughput(Throughput::Elements(1000));

    // Message -> Event (used when sending to emitters)
    group.bench_function("message_to_event", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let msg = make_message();
                let _event: Event = msg.into();
            }
        })
    });

    // Event -> Message (used when receiving from gRPC)
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

    // Demonstrate zero-copy: cloning Message should be cheap
    group.bench_function("clone_message", |b| {
        let msg = make_message();
        b.iter(|| {
            for _ in 0..1000 {
                let _cloned = msg.clone();
            }
        })
    });

    // Compare with Event clone (not zero-copy)
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
        let msg = Message {
            id: "test".to_string(),
            timestamp: 0,
            source: "bench".to_string(),
            message_type: "test".to_string(),
            metadata: HashMap::new(),
            payload: Bytes::from(payload),
            route_to: vec![],
        };

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
    bench_message_to_event,
    bench_message_clone,
    bench_payload_sizes
);
criterion_main!(benches);
