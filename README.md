# POLKU

**Programmatic Protocol Hub**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Logic IS code. Internal event routing as a library.

---

## What is POLKU?

POLKU is an **infrastructure library** for internal event routing. Services and agents send events to the hub; the hub transforms, filters, and fans out to destinations.

- **Event-driven**: Fire-and-forget with buffering, not request/response
- **Programmable**: Routing logic is Rust code, not config files
- **Embeddable**: Import as a crate or run standalone
- **Lightweight**: 10-20MB footprint, self-contained runtime (no external infrastructure services required)

```
┌─────────────────────────────────────────────────────────────┐
│                       POLKU HUB                              │
│                                                              │
│  Ingestors           Middleware            Emitters          │
│  ┌──────────┐       ┌──────────┐        ┌──────────┐        │
│  │ gRPC     │──────►│ Transform│───────►│ AHTI     │        │
│  │ REST     │       │ Filter   │        │ Kafka    │        │
│  │ Webhook  │       │ Route    │        │ S3       │        │
│  └──────────┘       └──────────┘        └──────────┘        │
│                          │                                   │
│                     ┌────▼────┐                              │
│                     │ Buffer  │                              │
│                     │ (Ring)  │                              │
│                     └─────────┘                              │
└─────────────────────────────────────────────────────────────┘
```

**You own `main.rs`.** POLKU provides traits + engine, you wire up your Ingestors/Emitters.

---

## Crate Architecture

```
polku-core ◄── polku-gateway
    ▲
    └────────── external emitter plugins
```

- **polku-core**: Shared types (`Emitter` trait, `Event`, `PluginError`) that break dependency cycles
- **polku-gateway**: The hub implementation with ingestors, middleware, buffer, and built-in emitters

---

## Quick Start

```bash
cargo build --release
./target/release/polku-gateway
```

```bash
POLKU_GRPC_ADDR=0.0.0.0:50051
POLKU_BUFFER_CAPACITY=100000
POLKU_LOG_LEVEL=info
```

---

## Triadic Architecture

### Ingestor → Hub → Emitter

```rust
// Ingestor: decode protocol → Message
pub trait Ingestor: Send + Sync {
    fn name(&self) -> &'static str;
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>>;
}

// Emitter: Message → destination
#[async_trait]
pub trait Emitter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn emit(&self, events: &[Event]) -> Result<()>;
    async fn health(&self) -> bool;
}

// Middleware: transform, filter, route
#[async_trait]
pub trait Middleware: Send + Sync {
    fn name(&self) -> &'static str;
    async fn process(&self, msg: Message) -> Option<Message>;
}
```

### The Event Envelope

```rust
pub struct Event {
    pub id: String,                        // ULID
    pub timestamp_unix_ns: i64,            // Unix nanos
    pub source: String,                    // Origin (e.g., "tapio", "portti")
    pub event_type: String,                // Hierarchical type (e.g., "network.connection")
    pub metadata: HashMap<String, String>, // Headers (cluster, namespace, trace_id)
    pub payload: Vec<u8>,                  // Legacy: opaque bytes
    pub route_to: Vec<String>,             // Routing hints
    pub severity: Severity,                // Debug, Info, Warning, Error, Critical
    pub outcome: Outcome,                  // Success, Failure, Timeout, Unknown
    pub data: Option<EventData>,           // Typed event data (preferred)
}
```

### Typed Event Data

Events can carry strongly-typed data instead of opaque payload bytes:

```rust
pub enum EventData {
    Network(NetworkEventData),   // TCP, UDP, DNS, HTTP connections
    Kernel(KernelEventData),     // eBPF events: OOM, signals, syscalls
    Container(ContainerEventData), // Container lifecycle and resources
    K8s(K8sEventData),           // Kubernetes API events
    Process(ProcessEventData),   // Process lifecycle
    Resource(ResourceEventData), // Node/resource utilization
}
```

When typed data is present, emitters (like AHTI) can map fields directly without parsing.

### Type-Safe Event Flow

Events flow through the entire pipeline with compile-time type safety:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Collector  │────►│   POLKU     │────►│    AHTI     │────►│  Knowledge  │
│   (tapio)   │     │   Gateway   │     │   Emitter   │     │    Graph    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │                   │
  NetworkEventData    Event.data         AhtiEvent.data      Entity/Relationship
  (proto fields)     (oneof variant)    (direct mapping)     (graph nodes)
```

**No JSON parsing. No field guessing. No runtime type errors.**

The proto schema (`polku/v1/event.proto`) defines typed data once, and all components share the same generated types. When Tapio observes a TCP connection, it populates `NetworkEventData` fields. The AHTI emitter maps these directly to `ahti.v1.NetworkEventData` without parsing or field name translation.

---

## Hub Builder

```rust
use polku_gateway::{Hub, Transform, Filter, StdoutEmitter};

Hub::new()
    .middleware(Filter::new(|msg| msg.message_type.starts_with("user.")))
    .middleware(Transform::new(|mut msg| {
        msg.metadata.insert("processed".into(), "true".into());
        msg
    }))
    .emitter(StdoutEmitter::new())
    .build()
```

---

## Buffer Strategies

POLKU supports pluggable buffer strategies for different traffic patterns:

```rust
use polku_gateway::{Hub, BufferStrategy};

// Standard: Fast lock-free buffer, drops on overflow (default)
Hub::new()
    .buffer_strategy(BufferStrategy::standard(10_000))
    .build();

// Tiered: Primary + compressed overflow for graceful degradation
Hub::new()
    .buffer_strategy(BufferStrategy::tiered(10_000, 5_000))
    .build();
```

| Strategy | Best For | Overflow Behavior |
|----------|----------|-------------------|
| **Standard** | Steady traffic, low latency | Drops new messages |
| **Tiered** | Traffic spikes, reliability | Compresses to secondary buffer |

---

## Reliable Delivery

Enable checkpoint-based acknowledgment for at-least-once delivery:

```rust
use polku_gateway::{Hub, MemoryCheckpointStore};
use std::sync::Arc;

let checkpoints = Arc::new(MemoryCheckpointStore::new());

Hub::new()
    .checkpoint_store(checkpoints.clone())
    .emitter(MyEmitter::new())
    .build();

// Query safe retention point
let min_seq = checkpoints.min_checkpoint();
```

---

## AHTI Emitter

The gateway includes a feature-gated AHTI emitter for sending events to AHTI (knowledge graph):

```bash
# Build with AHTI support
cargo build --release --features emit-ahti

# Configure via environment
export POLKU_EMIT_AHTI_ENDPOINT="http://[::1]:50052"
./target/release/polku-gateway
```

The AHTI emitter:
- Converts `polku_core::Event` to `ahti.v1.AhtiEvent` via gRPC streaming
- Supports multiple endpoints with latency-based routing
- Includes retry/backoff via `ResilientEmitter` wrapper
- Maps typed event data directly to AHTI entities and relationships

---

## Deployment Modes

| Mode | Description |
|------|-------------|
| **Library** | Import as crate, zero network hop |
| **Standalone** | Central gateway for the cluster |
| **Sidecar** | Per-service proxy (legacy translation) |

---

## Project Structure

```
polku/
├── core/                 # polku-core crate
│   └── src/
│       ├── lib.rs        # Re-exports Emitter, Event, PluginError
│       ├── emit.rs       # Emitter trait definition
│       ├── error.rs      # PluginError enum
│       └── proto/        # Generated proto types (Event, typed data)
├── gateway/              # polku-gateway crate
│   └── src/
│       ├── main.rs       # Entry point (you own this)
│       ├── hub.rs        # Hub builder
│       ├── buffer.rs     # Ring buffer
│       ├── ingest/       # Ingestor trait
│       ├── emit/         # Built-in emitters
│       │   ├── stdout/   # StdoutEmitter
│       │   └── ahti/     # AhtiEmitter (feature-gated)
│       └── middleware/   # Filter, Transform, etc.
├── proto/                # Central proto definitions
│   └── polku/v1/
│       └── event.proto   # Event envelope + typed data
├── sykli.rs              # CI pipeline
└── CLAUDE.md             # Dev guide
```

---

## Naming

**Polku** (Finnish) = "path"

The path messages take through your system.

---

## License

Apache 2.0
