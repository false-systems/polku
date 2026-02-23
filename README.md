# POLKU

**The path your events take.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

```
Messages in  ───>  POLKU  ───>  Messages out
                (transform)
                (filter)
                (route)
```

---

## What is POLKU?

POLKU is a **programmable protocol hub**. Messages come in, get transformed, and go out to multiple destinations. Routing logic is Rust code, not config files.

```
                     ┌────────────────────────────────────────────────────┐
                     │                      POLKU                         │
                     │                                                    │
   Your App ────────>│  Ingestors ──> Middleware ──> Buffer ──> Emitters  │────────> gRPC
                     │      │              │                       │      │
   Agents ──────────>│      │         [transform]                  │      │────────> Webhook
                     │      │         [filter]                     │      │
   Webhooks ────────>│      │         [route]                      │      │────────> Stdout
                     │      │         [validate]                   │      │
                     │      v         [aggregate]                  v      │
                     │  ┌────────┐                           ┌────────┐  │
                     │  │ Plugin │  <── gRPC ──>             │ Plugin │  │
                     │  │ (any   │                           │ (any   │  │
                     │  │  lang) │                           │  lang) │  │
                     │  └────────┘                           └────────┘  │
                     └────────────────────────────────────────────────────┘
```

**Why POLKU?**

- **No YAML** - Routing logic is code, not config files
- **Any language** - Write plugins in Python, Go, Rust, whatever (gRPC interface)
- **Tiny** - 10-20MB memory, no external dependencies
- **Fast** - 178k+ events/sec streaming, ~1.3k/sec unary
- **Observable** - 40+ Prometheus metrics, pipeline pressure gauge, health endpoints

---

## Installation

### From Source

```bash
git clone https://github.com/false-systems/polku
cd polku

# Build the gateway binary
cargo build --release

# Binary at ./target/release/polku-gateway
```

### Docker

```bash
docker build -t polku-gateway .
docker run -p 50051:50051 -p 9090:9090 polku-gateway
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
polku-runtime = { git = "https://github.com/false-systems/polku" }
```

---

## Usage

### Standalone Gateway

Run the binary — it listens for gRPC on port 50051 and serves Prometheus metrics on port 9090:

```bash
# Debug mode: prints messages to stdout
./polku-gateway

# Router mode: forward to downstream gRPC endpoints
POLKU_EMIT_GRPC_ENDPOINTS=host1:50051,host2:50051 ./polku-gateway

# With JSON structured logging
POLKU_LOG_FORMAT=json POLKU_LOG_LEVEL=debug ./polku-gateway
```

Send a message:

```bash
grpcurl -plaintext -d '{
  "source": "my-app",
  "cluster": "dev",
  "payload": {
    "events": {
      "events": [{
        "id": "1",
        "timestamp_unix_ns": 0,
        "source": "my-app",
        "event_type": "user.signup",
        "payload": "aGVsbG8="
      }]
    }
  }
}' localhost:50051 polku.v1.Gateway/SendEvent
```

### As a Rust Library

Define your pipeline in your own crate — POLKU handles tracing, metrics, gRPC, and shutdown:

```rust
use polku_runtime::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    polku_runtime::run(|hub| async move {
        Ok(hub
            .ingestor(JsonIngestor::new())
            .middleware(Filter::new(|msg: &Message| {
                msg.message_type.starts_with("user.")
            }))
            .middleware(Validator::json())
            .emitter(StdoutEmitter::pretty()))
    }).await
}
```

Your closure receives a `Hub` pre-configured from environment variables (buffer capacity, batch size, flush interval). You add your ingestors, middleware, and emitters — the runtime does the rest.

For more control over ports or to disable gRPC:

```rust
use polku_runtime::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    RuntimeBuilder::new()
        .grpc_addr("0.0.0.0:50052".parse()?)
        .metrics_port(9091)
        .configure(|hub| async move {
            let grpc = GrpcEmitter::with_endpoints(vec![
                "downstream:50051".into(),
            ]).await?;

            Ok(hub
                .ingestor(JsonIngestor::new())
                .emitter(grpc))
        }).await
}
```

<details>
<summary>Advanced: direct Hub wiring without the runtime</summary>

If you need full control and want to manage tracing, metrics, and the gRPC server yourself:

```rust
use polku_gateway::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (_, hub_sender, runner) = Hub::new()
        .buffer_capacity(50_000)
        .batch_size(500)
        .flush_interval_ms(50)
        .middleware(Filter::new(|msg: &Message| msg.message_type.starts_with("user.")))
        .middleware(Validator::json())
        .emitter(StdoutEmitter::pretty())
        .build();

    tokio::spawn(runner.run());

    let buffer = Arc::new(polku_gateway::buffer::RingBuffer::new(10_000));
    let registry = Arc::new(PluginRegistry::new());
    let service = polku_gateway::server::GatewayService::with_hub(
        buffer, registry, hub_sender,
    );

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve("[::1]:50051".parse()?)
        .await?;

    Ok(())
}
```

</details>

### Kubernetes

POLKU runs as a single pod. Minimal deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: polku
spec:
  replicas: 1
  selector:
    matchLabels:
      app: polku
  template:
    metadata:
      labels:
        app: polku
    spec:
      containers:
        - name: polku
          image: polku-gateway:latest
          ports:
            - containerPort: 50051  # gRPC
            - containerPort: 9090   # Metrics
          env:
            - name: POLKU_GRPC_ADDR
              value: "0.0.0.0:50051"
            - name: POLKU_METRICS_ADDR
              value: "0.0.0.0:9090"
---
apiVersion: v1
kind: Service
metadata:
  name: polku
spec:
  selector:
    app: polku
  ports:
    - name: grpc
      port: 50051
    - name: metrics
      port: 9090
```

---

## The Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                          MESSAGE FLOW                                │
│                                                                      │
│   INGEST            MIDDLEWARE            BUFFER            EMIT     │
│  ┌───────┐        ┌───────────┐        ┌───────────┐     ┌───────┐ │
│  │ bytes │───────>│  Filter   │───────>│   Ring    │────>│ Fan   │ │
│  │   |   │        │  Enrich   │        │  Buffer   │     │ Out   │ │
│  │Message│        │  Route    │        │  (async)  │     │       │ │
│  └───────┘        │  Validate │        │  Tiered   │     └───────┘ │
│                   │  Aggregate│        └───────────┘               │
│                   └───────────┘                                     │
└─────────────────────────────────────────────────────────────────────┘
```

1. **Ingestors** - Parse bytes into Messages (protobuf, JSON, NDJSON, custom via gRPC plugin)
2. **Middleware** - Transform, filter, enrich, validate, route, aggregate, sample, rate-limit, deduplicate
3. **Buffer** - Decouple ingestion from emission (backpressure). Standard ring buffer or tiered with zstd compression.
4. **Emitters** - Send to destinations (gRPC, webhooks, stdout, custom via gRPC plugin). Optional resilience wrappers: retry, circuit breaker, failure capture.

The core type flowing through the pipeline is `Message` — protocol-agnostic, zero-copy:

```rust
pub struct Message {
    pub id: MessageId,                        // 16-byte ULID (Copy)
    pub timestamp: i64,                       // Unix nanos
    pub source: InternedStr,                  // Interned, O(1) clone
    pub message_type: InternedStr,            // Interned, O(1) clone
    pub metadata: Option<Box<HashMap<...>>>,  // Lazy-allocated
    pub payload: Bytes,                       // Zero-copy (Arc-based)
    pub route_to: SmallVec<[String; 2]>,      // Inline for 0-2 targets
}
```

The proto `Event` type only exists at gRPC boundaries (wire format). Inside the pipeline, everything is `Message`.

---

## Built-in Components

### Ingestors
| Name | Sources | Description |
|------|---------|-------------|
| `PassthroughIngestor` | `passthrough`, `polku` | Decodes protobuf `Event` into `Message` |
| `JsonIngestor` | `json`, `json-lines`, `ndjson` | JSON objects, arrays, or newline-delimited |
| `ExternalIngestor` | configurable | Delegates to any gRPC plugin |

### Middleware
| Name | What it does |
|------|--------------|
| `Filter` | Drop messages that don't match a predicate |
| `Transform` | Apply a function to each message |
| `Router` | Content-based routing (sets `route_to` targets) |
| `RateLimiter` | Global token-bucket rate limiting |
| `Throttle` | Per-source rate limiting with LRU eviction |
| `Sampler` | Probabilistic sampling (lock-free xorshift64) |
| `Deduplicator` | Drop duplicate IDs within a TTL window |
| `Enricher` | Add metadata from async lookups (DB, API) |
| `Validator` | Validate messages (built-ins: JSON, non-empty, max-size) |
| `Aggregator` | Batch N messages into 1 (strategies: JSON array, concat, first, last) |

### Emitters
| Name | Description |
|------|-------------|
| `StdoutEmitter` | Print to console (compact or pretty format) |
| `GrpcEmitter` | Send to downstream gRPC endpoints with load balancing |
| `WebhookEmitter` | HTTP POST JSON to any URL |
| `ExternalEmitter` | Delegate to any gRPC plugin |

### Resilience Wrappers
Composable layers that wrap any emitter:

| Wrapper | What it does |
|---------|--------------|
| `RetryEmitter` | Exponential backoff with jitter |
| `CircuitBreakerEmitter` | Closed/Open/Half-Open state machine |
| `FailureCaptureEmitter` | Buffer failed messages for debugging |

Compose with the builder:
```rust
let emitter = ResilientEmitter::wrap(my_emitter)
    .with_default_retry()
    .with_default_circuit_breaker()
    .with_failure_capture(buffer)
    .build();
```

---

## Write a Plugin

Plugins communicate over gRPC. Write them in any language.

### Python: Slack Emitter

```python
# Receives Message data as Event over gRPC
class SlackEmitter:
    def Info(self, request, context):
        return PluginInfo(
            name="slack-emitter",
            type=EMITTER,
            emitter_name="slack",
        )

    def Emit(self, request, context):
        for event in request.events:
            message = f"*{event.event_type}* from `{event.source}`"
            requests.post(SLACK_WEBHOOK, json={"text": message})
        return EmitResponse(success_count=len(request.events))

server = grpc.server(ThreadPoolExecutor())
server.add_insecure_port('[::]:9001')
server.start()
```

### Go: CSV Ingestor

```go
func (p *CSVIngestor) Ingest(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
    reader := csv.NewReader(bytes.NewReader(req.Data))
    headers, _ := reader.Read()

    var events []*Event
    for {
        row, err := reader.Read()
        if err != nil { break }

        metadata := make(map[string]string)
        for i, h := range headers {
            metadata[h] = row[i]
        }

        events = append(events, &Event{
            Id:        uuid.New().String(),
            EventType: "csv.row",
            Metadata:  metadata,
        })
    }
    return &IngestResponse{Events: events}, nil
}
```

Wire up in Rust:
```rust
Hub::new()
    .ingestor(ExternalIngestor::new("csv", "http://localhost:9002"))
    .emitter(Arc::new(ExternalEmitter::new("http://localhost:9001")))
    .build();
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLKU_GRPC_ADDR` | `[::1]:50051` | gRPC listen address |
| `POLKU_METRICS_ADDR` | `127.0.0.1:9090` | Prometheus metrics address |
| `POLKU_BUFFER_CAPACITY` | `100000` | Ring buffer capacity |
| `POLKU_BATCH_SIZE` | `1000` | Batch size for emitters |
| `POLKU_FLUSH_INTERVAL_MS` | `100` | Flush interval (ms) |
| `POLKU_LOG_LEVEL` | `info` | Log level |
| `POLKU_LOG_FORMAT` | `pretty` | Log format (`pretty` or `json`) |
| `POLKU_EMIT_GRPC_ENDPOINTS` | - | Comma-separated gRPC downstream endpoints |
| `POLKU_EMIT_GRPC_LAZY` | `false` | Lazy gRPC connections |

See [Usage](#usage) for programmatic Rust examples.

---

## Observability

### Prometheus Metrics (port 9090)

POLKU exposes 40+ metrics at `GET /metrics`:

| Category | Key Metrics |
|----------|-------------|
| **Throughput** | `polku_events_received_total`, `polku_events_forwarded_total`, `polku_events_per_second` |
| **Buffer** | `polku_buffer_size`, `polku_buffer_capacity`, `polku_buffer_overflow_total` |
| **Latency** | `polku_processing_latency`, `polku_flush_duration_seconds`, `polku_middleware_duration_seconds` |
| **Emitter health** | `polku_emitter_health`, `polku_circuit_breaker_state`, `polku_emitter_throughput` |
| **Load balancing** | `polku_grpc_endpoint_fill_ratio`, `polku_grpc_endpoint_health`, `polku_grpc_failover_total` |
| **Pipeline** | `polku_pipeline_pressure` (composite: 0.0 idle → 1.0 overloaded) |

### Health Endpoints

- `GET /health` - JSON health summary with pipeline pressure and per-component status
- `GET /pipeline` - JSON pipeline manifest (topology, components, configuration)

### Processing Trace

Set `metadata["polku.trace"] = "true"` on a message to get per-middleware timing:

```json
[{"mw":"filter","action":"passed","us":12},{"mw":"router","action":"routed","targets":["kafka"]}]
```

Stored in `metadata["_polku.trace"]` after pipeline processing.

---

## Performance

| Metric | Value |
|--------|-------|
| Memory | 10-20 MB |
| Streaming throughput | 178k+ events/sec |
| Unary throughput | ~1.3k events/sec |
| Plugin overhead | ~100-500us per gRPC call |

Streaming (`StreamEvents`) is the high-throughput path. For bulk ingestion, use batched streaming instead of unary `SendEvent`.

The pipeline is zero-copy end-to-end: `Bytes` payloads flow without allocation, `InternedStr` fields clone in O(1), and `MessageId` is `Copy`.

---

## Project Structure

```
polku/
├── core/                    # Shared types crate
│   └── src/
│       ├── message.rs       # Message, MessageId, Routes, Metadata
│       ├── emit.rs          # Emitter trait
│       ├── error.rs         # PluginError
│       └── intern.rs        # InternedStr
│
├── gateway/                 # Gateway library + binary
│   └── src/
│       ├── main.rs          # Standalone entry point
│       ├── config.rs        # Env var configuration
│       ├── server.rs        # gRPC server (polku.v1.Gateway)
│       ├── hub/             # Hub builder + runner
│       ├── ingest/          # Ingestors (3 built-in)
│       ├── middleware/       # Middleware (10 types)
│       ├── emit/            # Emitters (4 + resilience wrappers)
│       ├── metrics.rs       # Prometheus metric definitions
│       ├── metrics_server.rs# /metrics, /health, /pipeline
│       ├── buffer.rs        # RingBuffer
│       ├── buffer_tiered.rs # TieredBuffer (zstd compression)
│       └── manifest.rs      # Pipeline self-description
│
├── runtime/                 # Injectable pipeline interface
│   └── src/
│       ├── lib.rs           # run(), RuntimeBuilder
│       └── prelude.rs       # Re-exports for pipeline authors
│
├── ci/                      # CI utilities
├── test-plugins/            # Test plugin implementations
│   └── receiver/            # gRPC receiver for testing
├── tests/e2e/               # End-to-end tests (Kind k8s)
└── docs/                    # Documentation
```

Proto definitions live in a [separate repository](https://github.com/false-systems/proto) at `proto/polku/v1/`:
- `gateway.proto` - Client-facing gRPC service (`SendEvent`, `StreamEvents`, `Health`)
- `plugin.proto` - Plugin gRPC interface (Ingestor/Emitter plugins)
- `event.proto` - Wire-format Event type (gRPC boundaries only)

---

## Naming

**Polku** (Finnish) = "path"

The path your messages take through the system.

---

## License

Apache 2.0
