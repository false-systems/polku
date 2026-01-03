//! Buffer operations benchmarks
//!
//! Measures push/drain performance of the ring buffer.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use polku_gateway::buffer::RingBuffer;
use polku_gateway::Message;

fn make_message(i: usize) -> Message {
    Message::new("bench", format!("event-{}", i), Bytes::from("benchmark payload data"))
}

fn make_messages(count: usize) -> Vec<Message> {
    (0..count).map(make_message).collect()
}

fn bench_buffer_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_push");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("batch_{}", batch_size), |b| {
            let buffer = RingBuffer::new(100_000);
            let messages = make_messages(batch_size);

            b.iter(|| {
                buffer.push(messages.clone());
            })
        });
    }

    group.finish();
}

fn bench_buffer_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_drain");

    for drain_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(drain_size as u64));
        group.bench_function(format!("drain_{}", drain_size), |b| {
            b.iter_batched(
                || {
                    // Setup: create buffer and fill it
                    let buffer = RingBuffer::new(100_000);
                    buffer.push(make_messages(10_000));
                    buffer
                },
                |buffer| {
                    // Benchmark: drain
                    buffer.drain(drain_size)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_buffer_push_drain_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_cycle");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("push_100_drain_100_x10", |b| {
        let buffer = RingBuffer::new(10_000);

        b.iter(|| {
            for _ in 0..10 {
                buffer.push(make_messages(100));
                let _ = buffer.drain(100);
            }
        })
    });

    group.finish();
}

fn bench_buffer_concurrent(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("buffer_concurrent");
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("4_writers_1_reader", |b| {
        b.iter(|| {
            let buffer = Arc::new(RingBuffer::new(100_000));
            let mut handles = vec![];

            // 4 writer threads
            for _ in 0..4 {
                let buf = Arc::clone(&buffer);
                handles.push(thread::spawn(move || {
                    for i in 0..2500 {
                        buf.push(vec![make_message(i)]);
                    }
                }));
            }

            // 1 reader thread
            let buf = Arc::clone(&buffer);
            handles.push(thread::spawn(move || {
                let mut total = 0;
                while total < 10_000 {
                    let drained = buf.drain(100);
                    total += drained.len();
                    if drained.is_empty() {
                        std::hint::spin_loop();
                    }
                }
            }));

            for h in handles {
                h.join().unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_buffer_push,
    bench_buffer_drain,
    bench_buffer_push_drain_cycle,
    bench_buffer_concurrent
);
criterion_main!(benches);
