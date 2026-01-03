//! Hub throughput benchmarks
//!
//! Measures messages/second through the full pipeline.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use polku_gateway::{Emitter, Event, Hub, Message, PluginError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    Message::new("bench", format!("event-{}", i), Bytes::from("benchmark payload"))
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
                    let runner_handle = tokio::spawn(async move {
                        runner.run().await
                    });

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
                        msg.metadata.insert("processed".into(), "true".into());
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

criterion_group!(benches, bench_hub_throughput, bench_hub_with_middleware);
criterion_main!(benches);
