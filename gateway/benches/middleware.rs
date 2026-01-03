//! Middleware overhead benchmarks
//!
//! Measures cost of each middleware type per message.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use polku_gateway::{
    Aggregator, Deduplicator, Enricher, Filter, Message, Middleware, RateLimiter, Sampler,
    Throttle, Transform, Validator,
};
use std::collections::HashMap;
use std::time::Duration;

fn make_message() -> Message {
    Message::new("bench-source", "bench.event", Bytes::from(r#"{"key": "value"}"#))
}

fn bench_individual_middleware(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("middleware_overhead");
    group.throughput(Throughput::Elements(1000));

    // Filter
    group.bench_function("filter", |b| {
        let filter = Filter::new(|msg: &Message| msg.source.starts_with("bench"));
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = filter.process(make_message()).await;
                }
            })
        })
    });

    // Transform
    group.bench_function("transform", |b| {
        let transform = Transform::new(|mut msg: Message| {
            msg.metadata.insert("processed".into(), "true".into());
            msg
        });
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = transform.process(make_message()).await;
                }
            })
        })
    });

    // RateLimiter (high limit so it doesn't block)
    group.bench_function("rate_limiter", |b| {
        let limiter = RateLimiter::new(1_000_000, 1_000_000);
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = limiter.process(make_message()).await;
                }
            })
        })
    });

    // Throttle
    group.bench_function("throttle", |b| {
        let throttle = Throttle::new(1_000_000, 1_000_000);
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = throttle.process(make_message()).await;
                }
            })
        })
    });

    // Deduplicator
    group.bench_function("deduplicator", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Fresh dedup each iteration (no duplicates)
                let dedup = Deduplicator::new(Duration::from_secs(60));
                for _ in 0..1000 {
                    let _ = dedup.process(make_message()).await;
                }
            })
        })
    });

    // Sampler (100% pass rate)
    group.bench_function("sampler", |b| {
        let sampler = Sampler::new(1.0);
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = sampler.process(make_message()).await;
                }
            })
        })
    });

    // Enricher (static)
    group.bench_function("enricher_static", |b| {
        let mut meta = HashMap::new();
        meta.insert("env".to_string(), "bench".to_string());
        let enricher = Enricher::with_static(meta);
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = enricher.process(make_message()).await;
                }
            })
        })
    });

    // Validator (JSON)
    group.bench_function("validator_json", |b| {
        let validator = Validator::json();
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = validator.process(make_message()).await;
                }
            })
        })
    });

    // Validator (non-empty, fast path)
    group.bench_function("validator_non_empty", |b| {
        let validator = Validator::non_empty();
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let _ = validator.process(make_message()).await;
                }
            })
        })
    });

    group.finish();
}

fn bench_aggregator(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregator");
    group.throughput(Throughput::Elements(1000));

    // Aggregator (batch size 10)
    group.bench_function("batch_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                let aggregator = Aggregator::new(10);
                for _ in 0..1000 {
                    let _ = aggregator.process(make_message()).await;
                }
            })
        })
    });

    // Aggregator (batch size 100)
    group.bench_function("batch_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let aggregator = Aggregator::new(100);
                for _ in 0..1000 {
                    let _ = aggregator.process(make_message()).await;
                }
            })
        })
    });

    group.finish();
}

criterion_group!(benches, bench_individual_middleware, bench_aggregator);
criterion_main!(benches);
