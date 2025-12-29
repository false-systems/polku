# POLKU

**Programmatic Protocol Hub**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Logic IS code. For when Envoy is overkill.

---

## What is POLKU?

POLKU is an **infrastructure library** for internal service communication. You write Rust code to define how messages flow - no YAML, no config files.

```
┌─────────────────────────────────────────────────────────────┐
│                       POLKU HUB                              │
│                                                              │
│  Ingestors           Middleware            Emitters          │
│  ┌──────────┐       ┌──────────┐        ┌──────────┐        │
│  │ gRPC     │──────►│ Transform│───────►│ gRPC     │        │
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

## Why POLKU?

| | Envoy/Istio | Kafka | POLKU |
|---|---|---|---|
| Config | 1000s lines YAML | Properties | **Rust code** |
| Logic | C++ filters | Consumer groups | **Full language** |
| Ops | Complex | Zookeeper | **Zero** |
| Memory | 50-100MB+ | JVM GB | **10-20MB** |

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

### The Message Envelope

```rust
pub struct Message {
    pub id: String,                        // ULID
    pub timestamp: i64,                    // Unix nanos
    pub source: String,                    // Origin
    pub message_type: String,              // User-defined
    pub metadata: HashMap<String, String>, // Headers
    pub payload: Bytes,                    // Zero-copy
    pub route_to: Vec<String>,             // Routing hints
}
```

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
├── gateway/src/
│   ├── main.rs          # Entry point (you own this)
│   ├── message.rs       # Message envelope
│   ├── hub.rs           # Hub builder
│   ├── buffer.rs        # Ring buffer
│   ├── ingest/          # Ingestor trait
│   ├── emit/            # Emitter trait + StdoutEmitter
│   └── middleware/      # Filter, Transform, etc.
├── sykli.rs             # CI pipeline
└── CLAUDE.md            # Dev guide
```

---

## Naming

**Polku** (Finnish) = "path"

The path messages take through your system.

---

## License

Apache 2.0
