# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-02-12

Initial pre-release of POLKU as an open-source programmable protocol hub.

### Added

- **Message-first pipeline**: `Message` is the sole type flowing through the pipeline. No more `Event<->Message` conversions on the hot path (#46)
- **AI-native introspection**: Pipeline manifest (`/pipeline`), per-message processing trace (`polku.trace`), structured error context with `ErrorContext` + `PipelineStage` (#46)
- **Pipeline health endpoint**: `/health` returns structured JSON with pipeline pressure metric (0.0-1.0), K8s liveness/readiness compatible (#46)
- **Middleware suite**: Router, RateLimiter, Deduplicator, Sampler, Throttle, Enricher, Validator, Aggregator
- **Resilience patterns**: RetryEmitter (exponential backoff + jitter), CircuitBreakerEmitter, FailureCaptureEmitter
- **GrpcEmitter**: Least-loaded endpoint selection, automatic failover, per-endpoint health tracking
- **TieredBuffer**: Primary ring buffer with zstd-compressed secondary tier for overflow
- **LockFreeBuffer**: Crossbeam-backed buffer for high-throughput scenarios
- **Per-middleware Prometheus metrics**: `polku_middleware_duration_seconds`, `polku_middleware_messages_total` (#53)
- **Bidirectional streaming**: High-throughput gRPC streaming for ingestors and emitters (#31)
- **External plugin system**: gRPC-based ingestor and emitter plugins with discovery service
- **Batch compression**: TieredBuffer secondary tier uses zstd compression (#26)
- **40+ Prometheus metrics**: Events, throughput, buffer, emitter health, circuit breaker, pipeline pressure
- **Example plugins**: Python Slack emitter, Go CSV ingestor

### Fixed

- AHTI emitter: 5 conversion bugs (timestamp nanos, severity mapping, entity extraction, duration precedence, empty-field handling) (#49)
- Preserve original event IDs through pipeline instead of generating new ULIDs (#30)
- Extracted shared `emit_to_destinations` to eliminate ~90 lines of duplicated flush logic (#53)
- Shared `TokenBucket` implementation replacing 3 independent copies (#51)
- Shared endpoint health tracking replacing duplicated health logic (#52)

### Architecture

- `polku-core`: Emitter trait, Message, PluginError, Event proto, metadata key constants
- `polku-gateway`: Hub, Ingestors, Middleware, Emitters, Buffer strategies
- Zero-copy `Bytes` payloads flow end-to-end (no `.to_vec()` on hot path)
- `InternedStr` for source/message_type fields (O(1) comparison, deduped storage)

### Infrastructure

- Multi-stage Dockerfile (non-root, stripped release binary)
- SYKLI CI integration (fmt, clippy, test, build)
- E2E black-box test infrastructure with Kind cluster support
