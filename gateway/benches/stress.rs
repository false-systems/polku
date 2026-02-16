//! Stress test - find the breaking point
//!
//! Pushes until buffer overflows or system chokes.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use polku_gateway::{Emitter, Hub, Message, PluginError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Slow emitter - simulates a backend that can't keep up
struct SlowEmitter {
    delay_ms: u64,
    count: AtomicU64,
}

impl SlowEmitter {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            count: AtomicU64::new(0),
        }
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Emitter for SlowEmitter {
    fn name(&self) -> &'static str {
        "slow"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        self.count
            .fetch_add(messages.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

/// Fast null emitter
struct NullEmitter(AtomicU64);

#[async_trait::async_trait]
impl Emitter for NullEmitter {
    fn name(&self) -> &'static str {
        "null"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        self.0.fetch_add(messages.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn health(&self) -> bool {
        true
    }
}

fn make_message(i: usize) -> Message {
    Message::new(
        "stress",
        format!("event-{}", i),
        Bytes::from("stress test payload with some realistic size data here"),
    )
}

/// Sustained throughput - measure real-world msg/sec
fn bench_sustained_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sustained");
    group.sample_size(10); // Fewer samples, longer runs
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("100k_messages", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(NullEmitter(AtomicU64::new(0)));
                let (_, sender, runner) = Hub::new()
                    .buffer_capacity(50_000)
                    .emitter_arc(emitter.clone())
                    .build();

                let start = Instant::now();

                // Spawn runner
                let runner_handle = tokio::spawn(async move { runner.run().await });

                // Blast 100k messages as fast as possible
                for i in 0..100_000 {
                    if sender.send(make_message(i)).await.is_err() {
                        break;
                    }
                }

                drop(sender);
                let _ = runner_handle.await;

                let elapsed = start.elapsed();
                let count = emitter.0.load(Ordering::SeqCst);
                let rate = count as f64 / elapsed.as_secs_f64();

                println!(
                    "\n  → {} msgs in {:?} = {:.0} msg/sec",
                    count, elapsed, rate
                );
            })
        })
    });

    group.finish();
}

/// Backpressure test - what happens when emitter is slower than input?
fn bench_backpressure(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("backpressure");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    // Slow emitter (10ms delay) with fast input
    group.bench_function("slow_emitter_10ms", |b| {
        b.iter(|| {
            rt.block_on(async {
                let emitter = Arc::new(SlowEmitter::new(10));
                let (_, sender, runner) = Hub::new()
                    .buffer_capacity(1000) // Small buffer
                    .emitter_arc(emitter.clone())
                    .build();

                let runner_handle = tokio::spawn(async move { runner.run().await });

                let start = Instant::now();
                let mut sent = 0;
                let mut failed = 0;

                // Try to send 5000 messages in 500ms
                let deadline = start + Duration::from_millis(500);
                while Instant::now() < deadline {
                    match sender.try_send(make_message(sent)) {
                        Ok(_) => sent += 1,
                        Err(_) => failed += 1,
                    }
                    // Small yield to let flush happen
                    if sent % 100 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                drop(sender);
                let _ = runner_handle.await;

                let emitted = emitter.count();
                println!(
                    "\n  → sent: {}, failed: {}, emitted: {}",
                    sent, failed, emitted
                );
            })
        })
    });

    group.finish();
}

/// Buffer overflow test - what happens when buffer is full?
fn bench_buffer_overflow(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("overflow");
    group.sample_size(10);

    for buffer_size in [100, 1000, 10000] {
        group.bench_function(format!("buffer_{}", buffer_size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    // Emitter that never emits (blocks forever until shutdown)
                    struct BlockingEmitter;

                    #[async_trait::async_trait]
                    impl Emitter for BlockingEmitter {
                        fn name(&self) -> &'static str {
                            "blocking"
                        }
                        async fn emit(&self, _: &[Message]) -> Result<(), PluginError> {
                            // Sleep long enough that buffer fills up
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            Ok(())
                        }
                        async fn health(&self) -> bool {
                            true
                        }
                    }

                    let (_, sender, runner) = Hub::new()
                        .buffer_capacity(buffer_size)
                        .emitter(BlockingEmitter)
                        .build();

                    let runner_handle = tokio::spawn(async move {
                        tokio::time::timeout(Duration::from_millis(100), runner.run()).await
                    });

                    // Try to overfill the buffer
                    let mut sent = 0;
                    for i in 0..buffer_size * 3 {
                        if sender.try_send(make_message(i)).is_ok() {
                            sent += 1;
                        }
                    }

                    drop(sender);
                    let _ = runner_handle.await;

                    // Should have sent approximately buffer_size + channel capacity
                    println!("\n  → buffer: {}, sent: {}", buffer_size, sent);
                })
            })
        });
    }

    group.finish();
}

/// Memory pressure test - large payloads
fn bench_large_payloads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("large_payloads");
    group.sample_size(10);

    for payload_kb in [1, 10, 100, 1000] {
        let payload = Bytes::from(vec![b'x'; payload_kb * 1024]);
        group.throughput(Throughput::Bytes((payload_kb * 1024 * 1000) as u64));

        group.bench_function(format!("{}kb_x1000", payload_kb), |b| {
            let payload = payload.clone();
            b.iter(|| {
                rt.block_on(async {
                    let emitter = Arc::new(NullEmitter(AtomicU64::new(0)));
                    let (_, sender, runner) = Hub::new()
                        .buffer_capacity(2000)
                        .emitter_arc(emitter.clone())
                        .build();

                    let runner_handle = tokio::spawn(async move { runner.run().await });

                    for i in 0..1000 {
                        let msg = Message::new("stress", format!("evt-{}", i), payload.clone());
                        let _ = sender.send(msg).await;
                    }

                    drop(sender);
                    let _ = runner_handle.await;
                })
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sustained_throughput,
    bench_backpressure,
    bench_buffer_overflow,
    bench_large_payloads
);
criterion_main!(benches);
