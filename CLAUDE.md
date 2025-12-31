# POLKU: Programmatic Protocol Hub

**Infrastructure Library for internal event routing**

---

## PROJECT NATURE

**POLKU IS AN INFRASTRUCTURE LIBRARY, NOT A FRAMEWORK**

- **Philosophy**: Logic IS code. You have full Rust power to decide how events flow.
- **User owns `main.rs`**: POLKU provides traits + engine, you wire up your Ingestors/Emitters.
- **Language**: 100% Rust
- **Use case**: Internal event routing between services/agents. Fan-in from many sources, transform, fan-out to many destinations.

---

## WHAT POLKU DOES

```
┌─────────────────────────────────────────────────────────────┐
│                                                              │
│  POLKU:                                                      │
│  • Internal event routing (service ↔ service, agent → hub)  │
│  • Logic IS code (if-statements, loops, custom math)        │
│  • 10-20MB RAM footprint                                     │
│  • Zero ops (no cluster, no config files)                   │
│  • Type-safe plugin architecture                             │
│  • Fire-and-forget with buffering                            │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## TRIADIC PLUGIN ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────┐
│                         POLKU HUB                            │
│                                                              │
│  Ingestors            Middleware            Emitters         │
│  ┌──────────┐        ┌──────────┐        ┌──────────┐       │
│  │ gRPC     │───────►│ Transform│───────►│ gRPC     │       │
│  │ REST     │        │ Filter   │        │ Kafka    │       │
│  │ Webhook  │        │ Route    │        │ S3       │       │
│  └──────────┘        └──────────┘        └──────────┘       │
│       │                   │                   │              │
│       │              ┌────▼────┐              │              │
│       └─────────────►│ Buffer  │◄─────────────┘              │
│                      │ (Ring)  │                             │
│                      └─────────┘                             │
└─────────────────────────────────────────────────────────────┘
```

| Component | Role | Implementation |
|-----------|------|----------------|
| **Ingestors** | Decode raw bytes into Message | `trait Ingestor` - Axum/Tonic listeners |
| **Hub** | Orchestrate flow, fan-in/fan-out | Tokio + MPSC channels |
| **Emitters** | Re-encode Message for target | `trait Emitter` - Protocol adapters |

---

## DEPLOYMENT MODES

| Mode | Description | Use Case |
|------|-------------|----------|
| **Library** | Import as Rust crate, zero network hop | Embedded in your service |
| **Standalone Hub** | Single central gateway ("Entrance" pattern) | Cluster-wide routing |
| **Sidecar** | Per-service proxy | Legacy protocol translation |

---

## CORE TYPES

```rust
// Universal Message - protocol agnostic, zero-copy
pub struct Message {
    pub id: String,                        // ULID
    pub timestamp: i64,                    // Unix nanos
    pub source: String,                    // Origin
    pub message_type: String,              // User-defined
    pub metadata: HashMap<String, String>, // Headers
    pub payload: Bytes,                    // Zero-copy (Arc-based)
    pub route_to: Vec<String>,             // Routing hints
}

// Ingestor: decode protocol → Message
#[async_trait]
pub trait Ingestor: Send + Sync {
    fn name(&self) -> &'static str;
    fn transform(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>>;
}

// Emitter: Message → target protocol
#[async_trait]
pub trait Emitter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn emit(&self, messages: &[Message]) -> Result<()>;
    async fn health(&self) -> bool;
}

// Middleware: transform, filter, route
#[async_trait]
pub trait Middleware: Send + Sync {
    fn name(&self) -> &'static str;
    async fn process(&self, msg: Message) -> Option<Message>;
}
```

---

## UNIVERSAL HUB PATTERNS

| Pattern | Implementation | Benefit |
|---------|----------------|---------|
| **Internal Message** | `Bytes` + `HashMap<String, String>` | Zero-copy: data not cloned between plugins |
| **Dynamic Dispatch** | `Box<dyn Ingestor>`, `Box<dyn Emitter>` | Open-ended: add protocols without modifying core |
| **Fan-In/Fan-Out** | `futures::join_all` | One input can trigger multiple outputs in parallel |

---

## RUST REQUIREMENTS

### Absolute Rules

1. **No `.unwrap()` in production** - Use `?` or proper error handling
2. **No `println!`** - Use `tracing::{info, warn, error, debug}`
3. **No TODOs or stubs** - Complete implementations only
4. **Use `Bytes` for payloads** - Zero-copy, reference counted

### Zero-Copy with Bytes

```rust
// BAD: cloning data
let copy = data.clone();  // Expensive for large payloads!

// GOOD: zero-copy
let reference = data.clone();  // Just increments Arc refcount
```

---

## TDD WORKFLOW

**RED → GREEN → REFACTOR**

```rust
#[tokio::test]
async fn test_middleware_transforms_message() {
    let mw = Transform::new(|mut msg| {
        msg.metadata.insert("processed".into(), "true".into());
        msg
    });

    let msg = Message::new("test", "evt", Bytes::from("payload"));
    let result = mw.process(msg).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap().metadata.get("processed"), Some(&"true".into()));
}
```

---

## FILE LOCATIONS

| What | Where |
|------|-------|
| Entry point | `gateway/src/main.rs` |
| Message type | `gateway/src/message.rs` |
| Hub builder | `gateway/src/hub.rs` |
| Ring buffer | `gateway/src/buffer.rs` |
| Ingestor trait | `gateway/src/ingest/mod.rs` |
| Emitter trait | `gateway/src/emit/mod.rs` |
| Middleware trait | `gateway/src/middleware/mod.rs` |
| Stdout emitter | `gateway/src/emit/stdout.rs` |

---

## DEPENDENCIES

| Crate | Purpose |
|-------|---------|
| `tonic` | gRPC server/client |
| `prost` | Protobuf serialization |
| `bytes` | Zero-copy buffers |
| `tokio` | Async runtime |
| `tracing` | Structured logging |
| `parking_lot` | Fast mutex |
| `thiserror` | Error types |
| `async-trait` | Async trait support |

---

## VERIFICATION

Before every commit:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

---

## AGENT INSTRUCTIONS

1. **Read first** - Understand existing patterns
2. **TDD always** - Write failing test, implement, refactor
3. **Use Bytes** - Zero-copy for all payloads
4. **No YAML** - Programmatic configuration via Builder
5. **Run checks** - `cargo fmt && cargo clippy && cargo test`

---

## POLKU STRENGTHS

| Aspect | How |
|--------|-----|
| **Programmable** | Routing logic is Rust code, not config |
| **Full language** | if/else, loops, custom transforms |
| **Zero ops** | No clusters, no external services |
| **Lightweight** | 10-20MB footprint |
| **Type-safe** | Compiler catches plugin errors |
