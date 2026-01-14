# POLKU

**Programmatic Protocol Hub**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Event routing as code. No YAML. No config files. Just Rust.

---

## What is POLKU?

POLKU is an **event gateway library**. It receives events from multiple sources, transforms them, and routes them to multiple destinations.

```
                            ┌─────────────────────────────────────────────────────────────┐
                            │                         POLKU                                │
                            │                                                              │
   ┌─────────┐              │   ┌───────────┐    ┌────────────┐    ┌───────────┐          │              ┌─────────┐
   │ Agent 1 │──────────────┼──►│           │    │            │    │           │──────────┼─────────────►│  AHTI   │
   └─────────┘   gRPC       │   │           │    │            │    │           │          │   gRPC       └─────────┘
                            │   │ Ingestors │───►│ Middleware │───►│  Emitters │          │
   ┌─────────┐              │   │           │    │            │    │           │          │              ┌─────────┐
   │ Agent 2 │──────────────┼──►│           │    │            │    │           │──────────┼─────────────►│  Kafka  │
   └─────────┘   gRPC       │   └───────────┘    └────────────┘    └───────────┘          │              └─────────┘
                            │        │                │                  │                 │
   ┌─────────┐              │        │           ┌────▼────┐             │                 │              ┌─────────┐
   │ Plugin  │◄─────────────┼────────┘           │ Buffer  │             └─────────────────┼─────────────►│   S3    │
   │ (Go/Py) │  gRPC        │                    │ (Ring)  │                               │              └─────────┘
   └─────────┘              │                    └─────────┘                               │
                            │                                                              │
                            └─────────────────────────────────────────────────────────────┘
```

**Key features:**
- **Programmable** - Routing logic is Rust code, not config files
- **Pluggable** - Write plugins in any language (Go, Python, Rust) via gRPC
- **Lightweight** - 10-20MB footprint, no external dependencies
- **Fire-and-forget** - Buffered delivery, not request/response

---

## Quick Start

### Installation

```bash
git clone https://github.com/yairfalse/polku
cd polku
cargo build --release
```

### Run the Gateway

```bash
# With environment config
POLKU_GRPC_ADDR=0.0.0.0:50051 \
POLKU_BUFFER_CAPACITY=100000 \
POLKU_LOG_LEVEL=info \
./target/release/polku-gateway
```

### Send Events

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "source": "my-agent",
  "cluster": "prod",
  "events": {
    "events": [{
      "id": "evt-001",
      "source": "my-agent",
      "event_type": "user.login"
    }]
  }
}' localhost:50051 polku.v1.Gateway/SendEvent
```

---

## How It Works

### The Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              EVENT FLOW                                       │
│                                                                              │
│  1. INGEST          2. TRANSFORM         3. BUFFER         4. EMIT          │
│  ┌─────────┐        ┌─────────────┐      ┌─────────┐      ┌─────────┐       │
│  │ Raw     │        │ Filter      │      │ Ring    │      │ Fan-out │       │
│  │ Bytes   │───────►│ Transform   │─────►│ Buffer  │─────►│ to all  │       │
│  │ → Event │        │ Enrich      │      │ (async) │      │ emitters│       │
│  └─────────┘        └─────────────┘      └─────────┘      └─────────┘       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

1. **Ingestors** parse raw bytes into typed `Event` structs
2. **Middleware** transforms, filters, and routes events
3. **Buffer** decouples ingestion from emission (backpressure handling)
4. **Emitters** send events to destinations in parallel

### The Event

```rust
pub struct Event {
    pub id: String,                        // Unique ID (ULID)
    pub timestamp_unix_ns: i64,            // When it happened
    pub source: String,                    // Origin ("tapio", "my-agent")
    pub event_type: String,                // Type ("network.connection", "k8s.pod.created")
    pub metadata: HashMap<String, String>, // Key-value context
    pub severity: Severity,                // Debug, Info, Warning, Error, Critical
    pub outcome: Outcome,                  // Success, Failure, Timeout

    // Typed data (preferred over raw payload)
    pub data: Option<EventData>,           // Network, Kernel, Container, K8s, Process, Resource
}
```

---

## Writing Plugins

POLKU supports **external plugins** written in any language with gRPC support. Plugins communicate via the `IngestorPlugin` service.

### Plugin Protocol

```protobuf
// proto/polku/v1/plugin.proto

service IngestorPlugin {
    rpc Info(Empty) returns (PluginInfo);           // Plugin metadata
    rpc Health(Empty) returns (PluginHealthResponse); // Health check
    rpc Ingest(IngestRequest) returns (IngestResponse); // Transform bytes → Events
}

message IngestRequest {
    string source = 1;   // Source identifier
    string cluster = 2;  // Cluster/environment
    string format = 3;   // Format hint ("json", "protobuf", "msgpack")
    bytes data = 4;      // Raw bytes to transform
}

message IngestResponse {
    repeated Event events = 1;  // Transformed events
    repeated IngestError errors = 2;  // Any errors
}
```

### Python Plugin Example

```python
# my_plugin.py
import grpc
from concurrent import futures
from polku.v1 import plugin_pb2, plugin_pb2_grpc
from polku.event.v1 import event_pb2

class MyPlugin(plugin_pb2_grpc.IngestorPluginServicer):

    def Info(self, request, context):
        return plugin_pb2.PluginInfo(
            name="my-python-plugin",
            version="1.0.0",
            type=plugin_pb2.PLUGIN_TYPE_INGESTOR,
            sources=["my-custom-source"],  # POLKU routes this source here
        )

    def Health(self, request, context):
        return plugin_pb2.PluginHealthResponse(healthy=True)

    def Ingest(self, request, context):
        # request.data contains raw bytes
        # Parse your custom format and return Events
        events = []
        for record in parse_my_format(request.data):
            events.append(event_pb2.Event(
                id=generate_id(),
                source=request.source,
                event_type="my.event.type",
                payload=record.to_bytes(),
            ))
        return plugin_pb2.IngestResponse(events=events)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    plugin_pb2_grpc.add_IngestorPluginServicer_to_server(MyPlugin(), server)
    server.add_insecure_port('[::]:9001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
```

### Go Plugin Example

```go
// main.go
package main

import (
    "context"
    "net"

    "google.golang.org/grpc"
    pb "github.com/yairfalse/proto/gen/go/polku/v1"
)

type myPlugin struct {
    pb.UnimplementedIngestorPluginServer
}

func (p *myPlugin) Info(ctx context.Context, _ *emptypb.Empty) (*pb.PluginInfo, error) {
    return &pb.PluginInfo{
        Name:    "my-go-plugin",
        Version: "1.0.0",
        Type:    pb.PluginType_PLUGIN_TYPE_INGESTOR,
        Sources: []string{"my-custom-source"},
    }, nil
}

func (p *myPlugin) Health(ctx context.Context, _ *emptypb.Empty) (*pb.PluginHealthResponse, error) {
    return &pb.PluginHealthResponse{Healthy: true}, nil
}

func (p *myPlugin) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
    // Parse req.Data and return events
    events := parseMyFormat(req.Data)
    return &pb.IngestResponse{Events: events}, nil
}

func main() {
    lis, _ := net.Listen("tcp", ":9001")
    s := grpc.NewServer()
    pb.RegisterIngestorPluginServer(s, &myPlugin{})
    s.Serve(lis)
}
```

### Register Plugin in POLKU

```rust
use polku_gateway::{Hub, ExternalIngestor, StdoutEmitter};

fn main() {
    let (raw_sender, msg_sender, runner) = Hub::new()
        // Register external plugin
        .ingestor(ExternalIngestor::new("my-custom-source", "http://localhost:9001"))
        // Built-in emitter
        .emitter(StdoutEmitter::new())
        .build();

    // Start the hub
    tokio::runtime::Runtime::new().unwrap().block_on(runner.run());
}
```

Now when POLKU receives events with `source: "my-custom-source"`, it routes them to your plugin for parsing.

---

## Built-in Components

### Ingestors

| Ingestor | Description |
|----------|-------------|
| `PassthroughIngestor` | Events already in protobuf `Event` format |
| `JsonIngestor` | JSON matching the Event schema |
| `ExternalIngestor` | Delegates to external gRPC plugin |

### Middleware

| Middleware | Description |
|------------|-------------|
| `Filter` | Drop events based on predicate |
| `Transform` | Modify events (add metadata, change fields) |
| `Router` | Route to specific emitters based on event type |
| `RateLimiter` | Limit events per second |
| `Sampler` | Sample a percentage of events |
| `Deduplicator` | Drop duplicate event IDs |
| `Aggregator` | Batch events before emitting |

### Emitters

| Emitter | Description |
|---------|-------------|
| `StdoutEmitter` | Print events to stdout (debugging) |
| `GrpcEmitter` | Send to any gRPC endpoint |
| `WebhookEmitter` | POST events to HTTP endpoint |
| `AhtiEmitter` | Send to AHTI knowledge graph (feature-gated) |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLKU_GRPC_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `POLKU_BUFFER_CAPACITY` | `10000` | Ring buffer size |
| `POLKU_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `POLKU_EMIT_AHTI_ENDPOINT` | - | AHTI emitter endpoint (if enabled) |

### Programmatic Configuration

```rust
use polku_gateway::{Hub, BufferStrategy, Filter, Transform, StdoutEmitter};

let hub = Hub::new()
    // Buffer strategy
    .buffer_strategy(BufferStrategy::tiered(10_000, 5_000))

    // Middleware chain
    .middleware(Filter::new(|msg| msg.event_type.starts_with("user.")))
    .middleware(Transform::new(|mut msg| {
        msg.metadata.insert("processed".into(), "true".into());
        msg
    }))

    // Emitters (fan-out to all)
    .emitter(StdoutEmitter::new())
    .emitter(GrpcEmitter::new("http://downstream:50051"))

    .build();
```

---

## Architecture

```
polku/
├── proto/                    # Protocol definitions
│   └── polku/v1/
│       ├── gateway.proto     # Gateway service (client → POLKU)
│       ├── plugin.proto      # Plugin service (POLKU → plugins)
│       └── event.proto       # Event envelope + typed data
│
├── core/                     # polku-core crate
│   └── src/
│       ├── emit.rs           # Emitter trait
│       ├── error.rs          # PluginError
│       └── proto/            # Generated Event types
│
└── gateway/                  # polku-gateway crate
    └── src/
        ├── main.rs           # Entry point
        ├── hub/              # Hub builder + runner
        ├── ingest/           # Ingestors (Passthrough, Json, External)
        ├── middleware/       # Filter, Transform, Router, etc.
        ├── emit/             # Emitters (Stdout, gRPC, Webhook, AHTI)
        └── buffer.rs         # Ring buffer
```

### Crate Dependencies

```
polku-core ◄── polku-gateway
    ▲
    └────────── external plugins (ahti-emitter, etc.)
```

`polku-core` contains shared types (`Emitter`, `Event`, `PluginError`) to avoid dependency cycles.

---

## Deployment Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Library** | Import as Rust crate | Embedded in your service |
| **Standalone** | Central gateway | Cluster-wide event routing |
| **Sidecar** | Per-service proxy | Legacy protocol translation |

---

## Performance

| Metric | Value |
|--------|-------|
| Memory footprint | 10-20MB |
| Throughput | 100k+ events/sec |
| Plugin latency | ~100-500μs per call |
| Buffer overflow | Configurable (drop or compress) |

For high-volume sources (>10k events/sec), implement a native Rust ingestor instead of using external plugins.

---

## Naming

**Polku** (Finnish) = "path"

The path events take through your system.

---

## License

Apache 2.0
