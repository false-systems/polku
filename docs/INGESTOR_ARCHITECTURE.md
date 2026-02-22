# Polku Ingestor Architecture

**Pluggable Format Transformation for Any Data Source**

---

## Overview

Polku's ingestor system transforms raw bytes from any source into `Message` objects. This is the core feature that makes Polku a **smart gateway** rather than a dumb pipe.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Raw Protobuf   │     │  Raw JSON       │     │  Custom Format  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Built-in        │     │  JsonIngestor   │     │  Your Ingestor  │
│ Ingestor (Rust) │     │     (Rust)      │     │  (Any Language) │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │       Message           │
                    │  ├─ id (MessageId)      │
                    │  ├─ source (InternedStr)│
                    │  ├─ message_type        │
                    │  ├─ metadata            │
                    │  └─ payload (Bytes)     │
                    └────────────┬────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │  Middleware → Emitters  │
                    └─────────────────────────┘
```

## Why Ingestors Matter

### The Problem

Data sources speak different languages:
- eBPF programs emit binary events with kernel-specific structures
- Kubernetes API returns JSON with deeply nested objects
- Cloud providers have their own API response formats
- Legacy systems have proprietary protocols

Forcing all sources to conform to one format shifts complexity to the collectors.

### The Solution

Polku handles transformation centrally:

1. **Collectors stay simple** - just ship raw bytes
2. **Polku transforms** - ingestors convert to Messages
3. **Schema lives in one place** - change once, applies everywhere
4. **Opaque payload** - payload bytes flow through zero-copy, decoded at the edges

## Ingestor Types

### Built-in Ingestors (Rust)

High-performance, zero-copy transformations compiled into Polku:

| Ingestor | Source | Input Format | Output |
|----------|--------|--------------|--------|
| `PassthroughIngestor` | any | `polku.Event` protobuf | Direct passthrough |
| `JsonIngestor` | any | JSON matching Event schema | Parsed Message |

### Plugin Ingestors (gRPC)

For custom formats, write an ingestor in any language that speaks gRPC:

```
┌─────────────────────────────────────────────────────────────┐
│                         POLKU                                │
│                                                              │
│  Raw bytes ──► ExternalIngestor ──► gRPC ──┐                │
│                                             │                │
└─────────────────────────────────────────────┼────────────────┘
                                              │
                                              ▼
                              ┌───────────────────────────────┐
                              │     Your Plugin (Go/Python)   │
                              │                               │
                              │  IngestorPlugin.Ingest(req)   │
                              │    → parse bytes              │
                              │    → return []Event           │
                              │                               │
                              └───────────────────────────────┘
```

## Configuration

### Hub Builder API

```rust
use polku_gateway::{Hub, ingest::*};

let (sender, runner) = Hub::new()
    // Built-in ingestors (fast path)
    .ingestor(JsonIngestor::new())

    // External plugin ingestor (flexible path)
    .ingestor_plugin("legacy-system", "localhost:9001")

    // Fallback for unknown sources
    .default_ingestor(PassthroughIngestor::new())

    // Rest of pipeline
    .middleware(RateLimiter::new(10_000))
    .emitter(GrpcEmitter::new("downstream:50051"))
    .build();
```

### How Routing Works

When Polku receives a message:

1. Extract `source` field from the request (e.g., `"my-agent"`)
2. Look up registered ingestor for that source
3. Call `ingestor.ingest(context, raw_bytes)`
4. Receive `Vec<Message>`
5. Pass messages through middleware -> emitters

```
IngestBatch {
    source: "my-agent",     <- Used to find ingestor
    cluster: "prod-east",
    payload: RawPayload {
        data: <bytes>,      <- Passed to ingestor
        format: "protobuf"  <- Hint for parsing
    }
}
```

## Writing a Built-in Ingestor

### The Trait

```rust
/// Ingestor transforms raw bytes into Messages
pub trait Ingestor: Send + Sync {
    /// Unique name for this ingestor
    fn name(&self) -> &'static str;

    /// Which source identifiers this ingestor handles
    fn sources(&self) -> &'static [&'static str];

    /// Transform raw bytes into Messages
    fn ingest(
        &self,
        ctx: &IngestContext,
        data: &[u8]
    ) -> Result<Vec<Message>, PluginError>;
}
```

### Example: Custom Protobuf Ingestor

```rust
use prost::Message as ProstMessage;
use polku_gateway::{Ingestor, IngestContext, Message, PluginError};
use bytes::Bytes;

pub struct MyProtobufIngestor;

impl Ingestor for MyProtobufIngestor {
    fn name(&self) -> &'static str {
        "my-proto"
    }

    fn sources(&self) -> &'static [&'static str] {
        &["my-service"]
    }

    fn ingest(
        &self,
        ctx: &IngestContext,
        data: &[u8],
    ) -> Result<Vec<Message>, PluginError> {
        // Decode your protobuf format
        let batch = MyEventBatch::decode(data)
            .map_err(|e| PluginError::Decode(e.to_string()))?;

        // Transform each raw event to a Message
        let messages: Vec<Message> = batch.events
            .into_iter()
            .map(|raw| {
                let payload = serde_json::to_vec(&raw.data).unwrap_or_default();
                let mut msg = Message::new(
                    ctx.source,
                    &format!("my-service.{}", raw.event_type),
                    Bytes::from(payload),
                );
                msg.metadata_mut().insert("cluster".into(), ctx.cluster.to_string());
                msg
            })
            .collect();

        Ok(messages)
    }
}
```

## Writing a Plugin Ingestor

For custom formats, implement the `IngestorPlugin` gRPC service:

### Proto Definition

```protobuf
service IngestorPlugin {
    rpc Info(google.protobuf.Empty) returns (PluginInfo);
    rpc Health(google.protobuf.Empty) returns (PluginHealthResponse);
    rpc Ingest(IngestRequest) returns (IngestResponse);
}

message IngestRequest {
    string source = 1;   // Source identifier
    string cluster = 2;  // Cluster context
    string format = 3;   // Format hint
    bytes data = 4;      // Raw bytes to transform
}

message IngestResponse {
    repeated Event events = 1;  // Transformed events
    repeated IngestError errors = 2;
}
```

### Go Example

```go
package main

import (
    "context"
    "encoding/json"

    pb "github.com/false-systems/proto/gen/go/polku/v1"
    "google.golang.org/grpc"
)

type LegacyIngestor struct {
    pb.UnimplementedIngestorPluginServer
}

func (i *LegacyIngestor) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
    var items []LegacyEvent
    if err := json.Unmarshal(req.Data, &items); err != nil {
        return nil, err
    }

    events := make([]*pb.Event, 0, len(items))
    for _, item := range items {
        event := &pb.Event{
            Id:              item.ID,
            TimestampUnixNs: item.Timestamp.UnixNano(),
            Source:          req.Source,
            EventType:       "legacy." + item.Type,
            Metadata: map[string]string{
                "region": item.Region,
            },
            Payload: item.RawData,
        }
        events = append(events, event)
    }

    return &pb.IngestResponse{Events: events}, nil
}
```

## Performance Considerations

### Built-in vs Plugin Performance

| Aspect | Built-in (Rust) | Plugin (gRPC) |
|--------|-----------------|---------------|
| Latency | ~1-10us | ~100-500us |
| Throughput | 1M+ events/sec | 100K events/sec |
| Memory | Zero-copy possible | Serialization overhead |
| Best for | High-volume sources | Custom/rare formats |

### Recommendations

1. **Use built-in ingestors** for known, high-volume sources
2. **Use plugin ingestors** for custom formats or when you can't write Rust
3. **Batch events** in plugin ingestors to amortize gRPC overhead
4. **Keep plugins close** - run on same node to minimize network latency

## Architecture Diagram

```
                              POLKU GATEWAY
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  RECEIVERS                                                           │
│  ┌─────────────┐  ┌─────────────┐                                   │
│  │    gRPC     │  │  PorttiSvc  │                                   │
│  │  Receiver   │  │  (K8s evts) │                                   │
│  └──────┬──────┘  └──────┬──────┘                                   │
│         │                │                                           │
│         └────────────────┘                                           │
│                  ▼                                                    │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    INGESTOR REGISTRY                          │  │
│  │                                                               │  │
│  │   source="json"    → JsonIngestor                            │  │
│  │   source="legacy"  → ExternalIngestor("localhost:9001")      │  │
│  │   default          → PassthroughIngestor                     │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                          │                                           │
│                          ▼                                           │
│                      Message                                         │
│                          │                                           │
│                          ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    MIDDLEWARE CHAIN                           │  │
│  │   RateLimiter → Filter → Router → Enricher → ...             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                          │                                           │
│                          ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                       EMITTERS                                │  │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │  │
│  │   │  gRPC   │  │ Webhook │  │ Stdout  │  │ Custom  │        │  │
│  │   │         │  │ (HTTP)  │  │         │  │ Plugin  │        │  │
│  │   └─────────┘  └─────────┘  └─────────┘  └─────────┘        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Summary

| Feature | Description |
|---------|-------------|
| **Pluggable** | Add custom ingestors without forking |
| **Fast** | Built-in Rust ingestors for high-volume |
| **Flexible** | gRPC plugins in any language |
| **Centralized** | Transform logic lives in gateway, not collectors |
| **Zero-copy** | `Bytes` payload flows through without allocation |
