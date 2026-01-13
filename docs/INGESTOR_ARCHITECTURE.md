# Polku Ingestor Architecture

**Pluggable Format Transformation for Any Data Source**

---

## Overview

Polku's ingestor system transforms raw bytes from any source into typed `polku.Event` messages. This is the core feature that makes Polku a **smart gateway** rather than a dumb pipe.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Raw eBPF bytes │     │  Raw K8s JSON   │     │  Custom Format  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  TapioIngestor  │     │ PorttiIngestor  │     │  Your Ingestor  │
│     (Rust)      │     │     (Rust)      │     │  (Any Language) │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │  Typed polku.Event      │
                    │  ├─ NetworkEventData    │
                    │  ├─ KernelEventData     │
                    │  ├─ ContainerEventData  │
                    │  ├─ K8sEventData        │
                    │  └─ ...                 │
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

Forcing all sources to conform to one format shifts complexity to the collectors. This means:
- Every collector needs transformation logic
- Changes to the event schema require updating all collectors
- No centralized control over data normalization

### The Solution

Polku handles transformation centrally:

1. **Collectors stay simple** - just ship raw bytes
2. **Polku transforms** - ingestors convert to typed events
3. **Schema lives in one place** - change once, applies everywhere
4. **Type safety** - events have structured data, not opaque blobs

## Ingestor Types

### Built-in Ingestors (Rust)

High-performance, zero-copy transformations compiled into Polku:

| Ingestor | Source | Input Format | Output |
|----------|--------|--------------|--------|
| `PassthroughIngestor` | any | `polku.Event` protobuf | Direct passthrough |
| `JsonIngestor` | any | JSON matching Event schema | Parsed Event |
| `TapioIngestor` | `tapio` | `tapio.v1.RawEbpfEvent` | Network/Kernel/Container events |
| `PorttiIngestor` | `portti` | `portti.v1.RawK8sEvent` | K8s events with entity extraction |
| `ElavaIngestor` | `elava` | `elava.v1.RawCloudEvent` | Cloud resource events |

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
    .ingestor(TapioIngestor::new())
    .ingestor(PorttiIngestor::new())
    .ingestor(ElavaIngestor::new())
    .ingestor(JsonIngestor::new())

    // External plugin ingestor (flexible path)
    .ingestor_plugin("legacy-system", "localhost:9001")

    // Fallback for unknown sources
    .default_ingestor(PassthroughIngestor::new())

    // Rest of pipeline
    .middleware(RateLimiter::new(10_000))
    .emitter(GrpcEmitter::new("ahti:50051"))
    .build();
```

### How Routing Works

When Polku receives a message:

1. Extract `source` field from the request (e.g., `"tapio"`)
2. Look up registered ingestor for that source
3. Call `ingestor.ingest(context, raw_bytes)`
4. Receive typed `Vec<Event>`
5. Pass events through middleware → emitters

```
IngestBatch {
    source: "tapio",        ← Used to find ingestor
    cluster: "prod-east",
    payload: RawPayload {
        data: <bytes>,      ← Passed to ingestor
        format: "protobuf"  ← Hint for parsing
    }
}
```

## Writing a Built-in Ingestor

### The Trait

```rust
/// Ingestor transforms raw bytes into typed Events
pub trait Ingestor: Send + Sync {
    /// Unique name for this ingestor
    fn name(&self) -> &'static str;

    /// Which source identifiers this ingestor handles
    /// e.g., ["tapio"] or ["json", "json-lines"]
    fn sources(&self) -> &[&'static str];

    /// Transform raw bytes into Events
    fn ingest(
        &self,
        ctx: &IngestContext,
        data: &[u8]
    ) -> Result<Vec<Event>, PluginError>;
}
```

### Example: TapioIngestor

```rust
use prost::Message;
use tapio_proto::RawEbpfEvent;

pub struct TapioIngestor;

impl Ingestor for TapioIngestor {
    fn name(&self) -> &'static str {
        "tapio"
    }

    fn sources(&self) -> &[&'static str] {
        &["tapio"]
    }

    fn ingest(
        &self,
        ctx: &IngestContext,
        data: &[u8],
    ) -> Result<Vec<Event>, PluginError> {
        // Decode protobuf
        let batch = tapio_proto::EventBatch::decode(data)
            .map_err(|e| PluginError::Decode(e.to_string()))?;

        // Transform each raw event to polku.Event
        let events: Vec<Event> = batch.events
            .into_iter()
            .map(|raw| self.transform_event(raw, ctx))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(events)
    }
}

impl TapioIngestor {
    fn transform_event(
        &self,
        raw: RawEbpfEvent,
        ctx: &IngestContext,
    ) -> Result<Event, PluginError> {
        let mut event = Event {
            id: raw.id,
            timestamp_unix_ns: raw.timestamp.unwrap().seconds * 1_000_000_000,
            source: "tapio".into(),
            event_type: format!("ebpf.{}", raw.subtype),
            metadata: HashMap::new(),
            ..Default::default()
        };

        // Add cluster context
        event.metadata.insert("cluster".into(), ctx.cluster.into());
        event.metadata.insert("node".into(), raw.node_name);
        event.metadata.insert("namespace".into(), raw.namespace);
        event.metadata.insert("pod".into(), raw.pod_name);

        // Set typed data based on event type
        match raw.data {
            Some(Data::Network(net)) => {
                event.data = Some(EventData::Network(NetworkEventData {
                    protocol: net.protocol,
                    src_ip: net.src_ip,
                    dst_ip: net.dst_ip,
                    src_port: net.src_port,
                    dst_port: net.dst_port,
                    bytes_sent: net.bytes_sent,
                    bytes_received: net.bytes_recv,
                    latency_ms: (net.rtt_us as f64) / 1000.0,
                    ..Default::default()
                }));
            }
            Some(Data::Container(ctr)) => {
                event.data = Some(EventData::Container(ContainerEventData {
                    container_id: ctr.container_id,
                    container_name: ctr.container_name,
                    image: ctr.image,
                    state: ctr.state,
                    exit_code: ctr.exit_code,
                    ..Default::default()
                }));
            }
            // ... handle other types
            _ => {}
        }

        Ok(event)
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

    pb "github.com/yairfalse/proto/gen/go/polku/v1"
    "google.golang.org/grpc"
)

type LegacyCRMIngestor struct {
    pb.UnimplementedIngestorPluginServer
}

func (i *LegacyCRMIngestor) Info(ctx context.Context, _ *emptypb.Empty) (*pb.PluginInfo, error) {
    return &pb.PluginInfo{
        Name:    "legacy-crm-ingestor",
        Version: "1.0.0",
        Type:    pb.PluginType_PLUGIN_TYPE_INGESTOR,
        Sources: []string{"legacy-crm"},
    }, nil
}

func (i *LegacyCRMIngestor) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
    // Parse your legacy format
    var legacyEvents []LegacyCRMEvent
    if err := json.Unmarshal(req.Data, &legacyEvents); err != nil {
        return nil, err
    }

    // Transform to polku.Event
    events := make([]*pb.Event, 0, len(legacyEvents))
    for _, le := range legacyEvents {
        event := &pb.Event{
            Id:              le.ID,
            TimestampUnixNs: le.Timestamp.UnixNano(),
            Source:          "legacy-crm",
            EventType:       "crm." + le.Type,
            Metadata: map[string]string{
                "customer_id": le.CustomerID,
                "region":      le.Region,
            },
            Payload: le.RawData,
        }
        events = append(events, event)
    }

    return &pb.IngestResponse{Events: events}, nil
}

func main() {
    lis, _ := net.Listen("tcp", ":9001")
    server := grpc.NewServer()
    pb.RegisterIngestorPluginServer(server, &LegacyCRMIngestor{})
    server.Serve(lis)
}
```

### Python Example

```python
from concurrent import futures
import grpc
import json

from polku.v1 import plugin_pb2, plugin_pb2_grpc, event_pb2

class LegacyCRMIngestor(plugin_pb2_grpc.IngestorPluginServicer):

    def Info(self, request, context):
        return plugin_pb2.PluginInfo(
            name="legacy-crm-ingestor",
            version="1.0.0",
            type=plugin_pb2.PLUGIN_TYPE_INGESTOR,
            sources=["legacy-crm"],
        )

    def Ingest(self, request, context):
        # Parse your legacy format
        legacy_events = json.loads(request.data)

        # Transform to polku.Event
        events = []
        for le in legacy_events:
            event = event_pb2.Event(
                id=le["id"],
                timestamp_unix_ns=le["timestamp_ns"],
                source="legacy-crm",
                event_type=f"crm.{le['type']}",
                metadata={
                    "customer_id": le["customer_id"],
                    "region": le["region"],
                },
                payload=le.get("raw_data", b""),
            )
            events.append(event)

        return plugin_pb2.IngestResponse(events=events)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    plugin_pb2_grpc.add_IngestorPluginServicer_to_server(
        LegacyCRMIngestor(), server
    )
    server.add_insecure_port("[::]:9001")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
```

## Type-Safe Event Data

The power of Polku's ingestors is producing **typed event data**, not opaque JSON blobs.

### Available Event Data Types

```protobuf
message Event {
    // ... common fields ...

    // Typed data - only one populated per event
    oneof data {
        NetworkEventData network = 20;     // TCP, UDP, DNS, HTTP
        KernelEventData kernel = 21;       // OOM, signals, syscalls
        ContainerEventData container = 22; // Lifecycle, resources
        K8sEventData k8s = 23;             // Deployments, pods, services
        ProcessEventData process = 24;     // Process lifecycle
        ResourceEventData resource = 25;   // Node resources
    }
}
```

### Why Typed Data Matters

**Without typed data (opaque payload):**
```json
{
  "id": "evt-123",
  "source": "tapio",
  "event_type": "network.connection",
  "payload": "{\"src_ip\":\"10.0.0.1\",\"dst_ip\":\"10.0.0.2\",\"bytes\":1234}"
}
```
- Downstream must parse JSON again
- No schema validation
- Field names can drift between sources
- Hard to query/filter efficiently

**With typed data:**
```protobuf
Event {
    id: "evt-123"
    source: "tapio"
    event_type: "network.connection"
    network: NetworkEventData {
        src_ip: "10.0.0.1"
        dst_ip: "10.0.0.2"
        bytes_sent: 1234
    }
}
```
- Parsed once at ingestion
- Schema enforced
- Consistent field names
- Binary protobuf - fast serialization
- Easy to query: `event.network.src_ip`

## Performance Considerations

### Built-in vs Plugin Performance

| Aspect | Built-in (Rust) | Plugin (gRPC) |
|--------|-----------------|---------------|
| Latency | ~1-10μs | ~100-500μs |
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
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │    gRPC     │  │    HTTP     │  │    Kafka    │                  │
│  │  Receiver   │  │  Receiver   │  │  Receiver   │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         │                │                │                          │
│         └────────────────┼────────────────┘                          │
│                          ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    INGESTOR REGISTRY                          │  │
│  │                                                               │  │
│  │   source="tapio"   → TapioIngestor                           │  │
│  │   source="portti"  → PorttiIngestor                          │  │
│  │   source="elava"   → ElavaIngestor                           │  │
│  │   source="json"    → JsonIngestor                            │  │
│  │   source="legacy"  → ExternalIngestor("localhost:9001")      │  │
│  │   default          → PassthroughIngestor                     │  │
│  │                                                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                          │                                           │
│                          ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                     polku.Event                               │  │
│  │   ┌─────────────────────────────────────────────────────┐    │  │
│  │   │ id, timestamp, source, event_type, metadata         │    │  │
│  │   │ + typed data (network/kernel/container/k8s/...)     │    │  │
│  │   └─────────────────────────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────────┘  │
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
│  │   │  Ahti   │  │ Webhook │  │  Kafka  │  │ Custom  │        │  │
│  │   │ (gRPC)  │  │ (HTTP)  │  │         │  │ Plugin  │        │  │
│  │   └─────────┘  └─────────┘  └─────────┘  └─────────┘        │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Summary

| Feature | Description |
|---------|-------------|
| **Pluggable** | Add custom ingestors without forking |
| **Type-safe** | Events have structured data, not blobs |
| **Fast** | Built-in Rust ingestors for high-volume |
| **Flexible** | gRPC plugins in any language |
| **Centralized** | Transform logic lives in gateway, not collectors |
| **Extensible** | Add new event data types as needed |
