//! Buffer operations benchmarks
//!
//! Measures push/drain performance of both ring buffer implementations:
//! - RingBuffer: Mutex-based (original)
//! - LockFreeBuffer: crossbeam-based (Monster optimization)

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use polku_gateway::Message;
use polku_gateway::buffer::RingBuffer;
use polku_gateway::buffer_lockfree::LockFreeBuffer;

fn make_message(i: usize) -> Message {
    Message::new(
        "bench",
        format!("event-{}", i),
        Bytes::from("benchmark payload data"),
    )
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

/// Benchmark: lock-free buffer push performance
fn bench_lockfree_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("lockfree_push");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("batch_{}", batch_size), |b| {
            let buffer = LockFreeBuffer::new(100_000);
            let messages = make_messages(batch_size);

            b.iter(|| {
                buffer.push_batch(messages.clone());
            })
        });
    }

    group.finish();
}

/// Benchmark: lock-free concurrent (MPSC pattern)
fn bench_lockfree_concurrent(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("lockfree_concurrent");
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("4_writers_1_reader", |b| {
        b.iter(|| {
            let buffer = LockFreeBuffer::new(100_000);
            let mut handles = vec![];

            // 4 writer threads
            for _ in 0..4 {
                let buf = buffer.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..2500 {
                        buf.push(make_message(i));
                    }
                }));
            }

            // 1 reader thread
            let buf = buffer.clone();
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

/// Head-to-head comparison: Mutex vs Lock-Free
fn bench_comparison(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("mutex_vs_lockfree");
    group.throughput(Throughput::Elements(100_000));

    // Mutex-based buffer
    group.bench_function("mutex_100k_push", |b| {
        b.iter(|| {
            let buffer = RingBuffer::new(100_000);
            for i in 0..100_000 {
                buffer.push(vec![make_message(i)]);
            }
        })
    });

    // Lock-free buffer
    group.bench_function("lockfree_100k_push", |b| {
        b.iter(|| {
            let buffer = LockFreeBuffer::new(100_000);
            for i in 0..100_000 {
                buffer.push(make_message(i));
            }
        })
    });

    // Concurrent scenario: 4 producers, 1 consumer
    group.bench_function("mutex_concurrent_4p1c", |b| {
        b.iter(|| {
            let buffer = Arc::new(RingBuffer::new(100_000));
            let mut handles = vec![];

            for _ in 0..4 {
                let buf = Arc::clone(&buffer);
                handles.push(thread::spawn(move || {
                    for i in 0..25_000 {
                        buf.push(vec![make_message(i)]);
                    }
                }));
            }

            let buf = Arc::clone(&buffer);
            handles.push(thread::spawn(move || {
                let mut total = 0;
                while total < 100_000 {
                    let drained = buf.drain(1000);
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

    group.bench_function("lockfree_concurrent_4p1c", |b| {
        b.iter(|| {
            let buffer = LockFreeBuffer::new(100_000);
            let mut handles = vec![];

            for _ in 0..4 {
                let buf = buffer.clone();
                handles.push(thread::spawn(move || {
                    for i in 0..25_000 {
                        buf.push(make_message(i));
                    }
                }));
            }

            let buf = buffer.clone();
            handles.push(thread::spawn(move || {
                let mut total = 0;
                while total < 100_000 {
                    let drained = buf.drain(1000);
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
    bench_buffer_concurrent,
    bench_lockfree_push,
    bench_lockfree_concurrent,
    bench_comparison
);
criterion_main!(benches);
