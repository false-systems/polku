# POLKU: Programmable Protocol Hub

Infrastructure library for internal event routing. 100% Rust.

## Philosophy

- **Logic IS code** — routing, transforms, filters are Rust, not config files
- **User owns `main.rs`** — POLKU provides traits + engine, you wire everything up
- **Zero ops** — no clusters, no external services, 10-20MB RAM

## Pipeline

```
bytes ──► Ingestor ──► Vec<Message> ──► Middleware ──► Buffer ──► Emitter ──► bytes
           (decode)     (zero-copy)     (transform)   (ring)     (encode)
```

`Message` is the **sole pipeline type**. `Event` (protobuf) is wire format only at gRPC boundaries.

## Core Types

```rust
pub struct Message {
    pub id: MessageId,                                   // Binary ULID (16 bytes, Copy)
    pub timestamp: i64,                                  // Unix nanos
    pub source: InternedStr,                             // O(1) clone via interning
    pub message_type: InternedStr,                       // O(1) clone via interning
    pub metadata: Option<Box<HashMap<String, String>>>,  // Lazy-allocated
    pub payload: Bytes,                                  // Zero-copy (Arc-based)
    pub route_to: SmallVec<[String; 2]>,                 // Inline for ≤2 routes
}
```

### Traits

```rust
// Ingestor: decode protocol → Message
pub trait Ingestor: Send + Sync {
    fn name(&self) -> &'static str;
    fn sources(&self) -> &'static [&'static str];
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError>;
}

// Emitter: Message → target protocol
#[async_trait]
pub trait Emitter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError>;
    async fn health(&self) -> bool;
    async fn shutdown(&self) -> Result<(), PluginError> { Ok(()) }
}

// Middleware: transform, filter, route
#[async_trait]
pub trait Middleware: Send + Sync {
    fn name(&self) -> &'static str;
    async fn process(&self, msg: Message) -> Option<Message>;
    fn flush(&self) -> Option<Message> { None }
}
```

## Zero-Copy Design

| Type | Optimization | Cost of Clone |
|------|-------------|---------------|
| `Bytes` payload | Arc-based ref counting | Arc increment (no copy) |
| `InternedStr` | Global interner (lasso) | Copy a u32 key |
| `MessageId` | 16-byte ULID, `Copy` | Memcpy 16 bytes |
| `Metadata` | `Option<Box<HashMap>>` | None = zero; Some = deep clone |
| `Routes` | `SmallVec<[String; 2]>` | Inline ≤2 routes |

## Metadata Conventions

Reserved keys in `metadata`:

| Key | Purpose |
|-----|---------|
| `polku.severity` | Severity level (string of i32) |
| `polku.outcome` | Outcome (string of i32) |
| `polku.content_type` | Payload encoding (e.g., `"application/protobuf"`) |
| `polku.trace` | Enable processing trace (`"true"`) |
| `_polku.trace` | Processing trace output (JSON array) |
| `polku.trace_id` | Distributed trace ID |
| `polku.span_id` | Distributed span ID |

## File Locations

| What | Where |
|------|-------|
| Message, MessageId | `core/src/message.rs` |
| Emitter trait | `core/src/emit.rs` |
| PluginError, ErrorContext | `core/src/error.rs` |
| InternedStr | `core/src/intern.rs` |
| Metadata key constants | `core/src/metadata_keys.rs` |
| Event (wire format) | `core/src/proto/polku.event.v1.rs` |
| Hub builder + runner | `gateway/src/hub/mod.rs`, `gateway/src/hub/runner.rs` |
| Ring buffer | `gateway/src/buffer.rs` |
| Tiered buffer | `gateway/src/buffer_tiered.rs` |
| Ingestor trait | `gateway/src/ingest/mod.rs` |
| Middleware trait + chain | `gateway/src/middleware/mod.rs` |
| Pipeline manifest | `gateway/src/manifest.rs` |
| Emitters (gRPC, stdout, webhook) | `gateway/src/emit/` |
| Entry point | `gateway/src/main.rs` |

## Rules

1. **No `.unwrap()` in production** — use `?` or proper error handling
2. **No `println!`** — use `tracing::{info, warn, error, debug}`
3. **No TODOs or stubs** — complete implementations only
4. **Use `Bytes` for payloads** — zero-copy, reference counted
5. **Message is the pipeline type** — Event is only for gRPC wire format
6. **TDD** — write failing test, implement, refactor

## Verification

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `tonic` | gRPC |
| `prost` | Protobuf |
| `bytes` | Zero-copy buffers |
| `tokio` | Async runtime |
| `tracing` | Structured logging |
| `parking_lot` | Fast mutex |
| `thiserror` | Error types |
| `async-trait` | Async trait support |
| `ulid` | Binary ULID message IDs |
| `lasso` | String interning |
| `smallvec` | Inline small collections |
