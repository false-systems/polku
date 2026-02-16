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

## MESSAGE-FIRST ARCHITECTURE

`Message` is the **sole pipeline type**. No conversions in the hot path.

```
bytes ──► Ingestor ──► Vec<Message> ──► Middleware ──► Buffer ──► Emitter ──► bytes
           (decode)     (zero-copy)     (transform)   (ring)     (encode)
```

`Event` (protobuf) is a **wire format only**, used at gRPC boundaries. It is NOT in the pipeline.

### Triadic Plugin Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         POLKU HUB                            │
│                                                              │
│  Ingestors            Middleware            Emitters         │
│  ┌──────────┐        ┌──────────┐        ┌──────────┐       │
│  │ gRPC     │───────►│ Transform│───────►│ gRPC     │       │
│  │ JSON     │  Msg   │ Filter   │  Msg   │ Webhook  │       │
│  │ External │───────►│ Route    │───────►│ Stdout   │       │
│  └──────────┘        │ Enrich   │        │ External │       │
│                      │ Validate │        └──────────┘       │
│                      └──────────┘                            │
│                           │                                  │
│                      ┌────▼────┐                             │
│                      │ Buffer  │                             │
│                      │ (Ring)  │                             │
│                      └─────────┘                             │
└─────────────────────────────────────────────────────────────┘
```

| Component | Role | Implementation |
|-----------|------|----------------|
| **Ingestors** | Decode raw bytes into `Vec<Message>` | `trait Ingestor` - protocol decoders |
| **Middleware** | Transform, filter, route messages | `trait Middleware` - composable chain |
| **Hub** | Orchestrate flow, fan-in/fan-out | Tokio + MPSC channels |
| **Emitters** | Re-encode `&[Message]` for target | `trait Emitter` - protocol adapters |

---

## CORE TYPES

```rust
// Universal Message - protocol agnostic, zero-copy
pub struct Message {
    pub id: MessageId,                            // Binary ULID (16 bytes, Copy)
    pub timestamp: i64,                           // Unix nanos
    pub source: InternedStr,                      // O(1) clone via interning
    pub message_type: InternedStr,                // O(1) clone via interning
    pub metadata: Option<Box<HashMap<String, String>>>,  // Lazy-allocated headers
    pub payload: Bytes,                           // Zero-copy (Arc-based)
    pub route_to: SmallVec<[String; 2]>,          // Inline for ≤2 routes
}

// Ingestor: decode protocol → Message
pub trait Ingestor: Send + Sync {
    fn name(&self) -> &'static str;
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError>;
}

// Emitter: Message → target protocol
#[async_trait]
pub trait Emitter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError>;
    async fn health(&self) -> bool;
}

// Middleware: transform, filter, route
#[async_trait]
pub trait Middleware: Send + Sync {
    fn name(&self) -> &'static str;
    async fn process(&self, msg: Message) -> Option<Message>;
    fn flush(&self) -> Option<Message> { None }  // For stateful middleware
}
```

---

## METADATA CONVENTIONS

Protocol-specific data is carried through the generic pipeline via reserved metadata keys:

| Key | Purpose | Example |
|-----|---------|---------|
| `polku.severity` | FALSE Protocol severity level | `"2"` (Warning) |
| `polku.outcome` | FALSE Protocol outcome | `"1"` (Success) |
| `polku.data_type` | Typed data discriminator | `"network"`, `"kernel"` |
| `polku.content_type` | Payload encoding | `"application/protobuf"` |
| `polku.trace` | Enable processing trace | `"true"` |
| `_polku.trace` | Processing trace output (JSON) | `[{"mw":"filter","action":"passed","us":42}]` |
| `polku.trace_id` | Distributed trace ID | OpenTelemetry trace ID |
| `polku.span_id` | Distributed span ID | OpenTelemetry span ID |

---

## AI-NATIVE CAPABILITIES

### Processing Trace

Opt-in per message via `metadata["polku.trace"] = "true"`. Each middleware appends to `metadata["_polku.trace"]`:

```json
[{"mw":"filter","action":"passed","us":42},{"mw":"router","action":"passed","us":3}]
```

### Structured Error Context

```rust
let err = PluginError::Send("timeout".into())
    .with_context(ErrorContext {
        component: "grpc-emitter".to_string(),
        stage: PipelineStage::Emit,
        message_id: Some("01HQXYZ".to_string()),
    });
```

### Pipeline Manifest

Self-describing topology generated at `Hub::build()` time. Available via `HubRunner::manifest()`.

---

## DEPLOYMENT MODES

| Mode | Description | Use Case |
|------|-------------|----------|
| **Library** | Import as Rust crate, zero network hop | Embedded in your service |
| **Standalone Hub** | Single central gateway ("Entrance" pattern) | Cluster-wide routing |
| **Sidecar** | Per-service proxy | Legacy protocol translation |

---

## ZERO-COPY DESIGN

| Type | Optimization | Cost of Clone |
|------|-------------|---------------|
| `Bytes` payload | Arc-based reference counting | Arc increment (no data copy) |
| `InternedStr` | Global string interner (lasso) | Copy a u32 key |
| `MessageId` | 16-byte ULID, implements `Copy` | Memcpy 16 bytes |
| `Metadata` | `Option<Box<HashMap>>` | None = zero cost; Some = deep clone |
| `Routes` | `SmallVec<[String; 2]>` | Inline for ≤2 routes, no heap |

---

## RUST REQUIREMENTS

### Absolute Rules

1. **No `.unwrap()` in production** - Use `?` or proper error handling
2. **No `println!`** - Use `tracing::{info, warn, error, debug}`
3. **No TODOs or stubs** - Complete implementations only
4. **Use `Bytes` for payloads** - Zero-copy, reference counted

---

## TDD WORKFLOW

**RED → GREEN → REFACTOR**

```rust
#[tokio::test]
async fn test_middleware_transforms_message() {
    let mw = Transform::new(|mut msg: Message| {
        msg.metadata_mut().insert("processed".into(), "true".into());
        msg
    });

    let msg = Message::new("test", "evt", Bytes::from("payload"));
    let result = mw.process(msg).await;

    assert!(result.is_some());
    assert_eq!(
        result.unwrap().metadata().get("processed"),
        Some(&"true".to_string()),
    );
}
```

---

## FILE LOCATIONS

| What | Where |
|------|-------|
| **Core types** | |
| Message, MessageId | `core/src/message.rs` |
| Emitter trait | `core/src/emit.rs` |
| PluginError, ErrorContext | `core/src/error.rs` |
| InternedStr | `core/src/intern.rs` |
| Metadata key constants | `core/src/metadata_keys.rs` |
| Event (wire format) | `core/src/proto/polku.event.v1.rs` |
| **Gateway** | |
| Entry point | `gateway/src/main.rs` |
| Hub builder + runner | `gateway/src/hub/mod.rs`, `gateway/src/hub/runner.rs` |
| Ring buffer | `gateway/src/buffer.rs` |
| Tiered buffer | `gateway/src/buffer_tiered.rs` |
| Ingestor trait | `gateway/src/ingest/mod.rs` |
| Middleware trait + chain | `gateway/src/middleware/mod.rs` |
| Pipeline manifest | `gateway/src/manifest.rs` |
| Emitters | `gateway/src/emit/` |

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
| `ulid` | Binary ULID message IDs |
| `lasso` | String interning (InternedStr) |
| `smallvec` | Inline small collections (Routes) |

---

## VERIFICATION

Before every commit:

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

---

## AGENT INSTRUCTIONS

1. **Read first** - Understand existing patterns
2. **TDD always** - Write failing test, implement, refactor
3. **Use Bytes** - Zero-copy for all payloads
4. **No YAML** - Programmatic configuration via Builder
5. **Run checks** - `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test`
6. **Message is the pipeline type** - Event is only for gRPC wire format

---

## POLKU STRENGTHS

| Aspect | How |
|--------|-----|
| **Programmable** | Routing logic is Rust code, not config |
| **Full language** | if/else, loops, custom transforms |
| **Zero ops** | No clusters, no external services |
| **Lightweight** | 10-20MB footprint |
| **Type-safe** | Compiler catches plugin errors |
| **Zero-copy** | Bytes payload flows end-to-end without allocation |
| **AI-native** | Processing traces, structured errors, pipeline manifest |
