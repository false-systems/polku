//! Hub throughput benchmarks
//!
//! Measures messages/second through the full pipeline.

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use polku_gateway::{Emitter, Event, Hub, Message, PluginError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// No-op emitter that just counts events
struct NullEmitter {
    count: AtomicU64,
}

impl NullEmitter {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
        }
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Emitter for NullEmitter {
    fn name(&self) -> &'static str {
        "null"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        self.count.fetch_add(events.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

fn make_message(i: usize) -> Message {
    Message::new(
        "bench",
        format!("event-{}", i),
        Bytes::from("benchmark payload"),
    )
}

fn bench_hub_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("hub_throughput");

    for batch_size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("messages_{}", batch_size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let emitter = Arc::new(NullEmitter::new());
                    let (sender, runner) = Hub::new()
                        .buffer_capacity(batch_size * 2)
                        .emitter_arc(emitter.clone())
                        .build();

                    // Spawn runner
                    let runner_handle = tokio::spawn(async move { runner.run().await });

                    // Send messages
                    for i in 0..batch_size {
                        sender.send(make_message(i)).await.unwrap();
                    }

                    // Drop sender to trigger shutdown
                    drop(sender);

                    // Wait for completion
                    let _ = runner_handle.await;

                    assert_eq!(emitter.count(), batch_size as u64);
                })
            })
        });
    }

    group.finish();
}

fn bench_hub_with_middleware(c: &mut Criterion) {
    use polku_gateway::{Filter, Transform};

    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("hub_with_middleware");
    group.throughput(Throughput::Elements(1000));

    // Baseline: no middleware
    group.bench_function("no_middleware", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(NullEmitter::new());
                let (sender, runner) = Hub::new()
                    .buffer_capacity(2000)
                    .emitter_arc(emitter.clone())
                    .build();

                let runner_handle = tokio::spawn(async move { runner.run().await });

                for i in 0..1000 {
                    sender.send(make_message(i)).await.unwrap();
                }
                drop(sender);
                let _ = runner_handle.await;
            })
        })
    });

    // With filter
    group.bench_function("with_filter", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(NullEmitter::new());
                let (sender, runner) = Hub::new()
                    .buffer_capacity(2000)
                    .middleware(Filter::new(|_| true)) // pass all
                    .emitter_arc(emitter.clone())
                    .build();

                let runner_handle = tokio::spawn(async move { runner.run().await });

                for i in 0..1000 {
                    sender.send(make_message(i)).await.unwrap();
                }
                drop(sender);
                let _ = runner_handle.await;
            })
        })
    });

    // With transform
    group.bench_function("with_transform", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(NullEmitter::new());
                let (sender, runner) = Hub::new()
                    .buffer_capacity(2000)
                    .middleware(Transform::new(|mut msg| {
                        msg.metadata_mut().insert("processed".into(), "true".into());
                        msg
                    }))
                    .emitter_arc(emitter.clone())
                    .build();

                let runner_handle = tokio::spawn(async move { runner.run().await });

                for i in 0..1000 {
                    sender.send(make_message(i)).await.unwrap();
                }
                drop(sender);
                let _ = runner_handle.await;
            })
        })
    });

    // With 5 middleware
    group.bench_function("with_5_middleware", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(NullEmitter::new());
                let (sender, runner) = Hub::new()
                    .buffer_capacity(2000)
                    .middleware(Filter::new(|_| true))
                    .middleware(Transform::new(|msg| msg))
                    .middleware(Filter::new(|_| true))
                    .middleware(Transform::new(|msg| msg))
                    .middleware(Filter::new(|_| true))
                    .emitter_arc(emitter.clone())
                    .build();

                let runner_handle = tokio::spawn(async move { runner.run().await });

                for i in 0..1000 {
                    sender.send(make_message(i)).await.unwrap();
                }
                drop(sender);
                let _ = runner_handle.await;
            })
        })
    });

    group.finish();
}

/// Hot path benchmark - measures pure send throughput on a pre-warmed Hub
///
/// This avoids including Hub creation/teardown in measurements.
/// Uses fast flush interval (1ms) to reduce flush loop bottleneck.
fn bench_hot_path(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("hot_path");
    group.throughput(Throughput::Elements(10_000));

    // Pre-warmed Hub with fast flush
    group.bench_function("prewarmed_10k", |b| {
        // Setup once outside iterations
        let emitter = Arc::new(NullEmitter::new());
        let (sender, runner) = Hub::new()
            .buffer_capacity(50_000)
            .batch_size(1000) // Larger batches
            .flush_interval_ms(1) // Fast flush - 1ms instead of 10ms
            .emitter_arc(emitter.clone())
            .build();

        // Start runner
        let runner_handle = rt.spawn(async move { runner.run().await });

        // Warm up the channel
        rt.block_on(async {
            for i in 0..100 {
                let _ = sender.send(make_message(i)).await;
            }
            // Give flush loop time to process
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        });

        // Benchmark just the send operations
        b.iter(|| {
            rt.block_on(async {
                for i in 0..10_000 {
                    sender.send(make_message(i)).await.unwrap();
                }
            })
        });

        // Cleanup
        drop(sender);
        let _ = rt.block_on(async { runner_handle.await });
    });

    // Compare with try_send (non-blocking)
    group.bench_function("try_send_10k", |b| {
        let emitter = Arc::new(NullEmitter::new());
        let (sender, runner) = Hub::new()
            .buffer_capacity(100_000) // Larger buffer for try_send
            .batch_size(1000)
            .flush_interval_ms(1)
            .emitter_arc(emitter.clone())
            .build();

        let runner_handle = rt.spawn(async move { runner.run().await });

        // Warm up
        rt.block_on(async {
            for i in 0..100 {
                let _ = sender.try_send(make_message(i));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        });

        b.iter(|| {
            for i in 0..10_000 {
                let _ = sender.try_send(make_message(i));
            }
        });

        drop(sender);
        let _ = rt.block_on(async { runner_handle.await });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_hub_throughput,
    bench_hub_with_middleware,
    bench_hot_path
);
criterion_main!(benches);
