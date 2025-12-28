# POLKU

**Pluggable gRPC Event Gateway**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.83%2B-orange.svg)](https://www.rust-lang.org)

A high-performance gRPC gateway for transforming and routing events from edge agents to central intelligence. Plugin in, plugin out.

---

## What is POLKU?

POLKU is the **path events take from edge to brain**.

```
┌─────────────────────────────────────────────────────────────┐
│                         POLKU                                │
│                                                              │
│  Input Plugins        Core              Output Plugins       │
│  ┌──────────┐    ┌───────────┐        ┌──────────┐         │
│  │  TAPIO   │───►│ Transform │───────►│   AHTI   │         │
│  │  PORTTI  │    │  Buffer   │        │   OTEL   │         │
│  │  ELAVA   │    │  Route    │        │   File   │         │
│  │  ...     │    └───────────┘        │   ...    │         │
│  └──────────┘                         └──────────┘         │
└─────────────────────────────────────────────────────────────┘
```

**Why a gateway?**
- **Decouple sources from destinations** - Add new agents without changing AHTI
- **Transform formats** - Each agent has its own schema, AHTI wants unified events
- **Handle backpressure** - Buffer during downstream slowdowns
- **Pluggable** - Input and output are traits, easy to extend

---

## Status

| Component | Status | Notes |
|-----------|--------|-------|
| Proto definitions | ✅ | AhtiEvent, Gateway service |
| Ring buffer | ✅ | FIFO eviction, backpressure signaling |
| Config | ✅ | Env var configuration |
| Error types | ✅ | thiserror-based |
| Prometheus metrics | ✅ | events_received, buffer_size, etc. |
| InputPlugin trait | ✅ | Transform bytes → Events |
| OutputPlugin trait | ✅ | Send events to destinations |
| gRPC server | ✅ | StreamEvents, SendEvent, Health |
| Main entry point | ✅ | Graceful shutdown |
| Plugin registry | 🚧 | In progress |
| Example plugins | 🚧 | TapioInput, StdoutOutput, AhtiOutput |

---

## Quick Start

```bash
# Build
cargo build --release

# Run tests
cargo test

# Run gateway
./target/release/polku-gateway
```

**Requirements:**
- Rust 1.83+
- protoc (protobuf compiler)

**Environment Variables:**
```bash
POLKU_GRPC_ADDR=0.0.0.0:50051      # gRPC server address
POLKU_METRICS_ADDR=0.0.0.0:9090   # Prometheus metrics
POLKU_BUFFER_CAPACITY=100000      # Event buffer size
POLKU_LOG_LEVEL=info              # Logging level
```

---

## Architecture

### Event Flow

```
1. Agent (TAPIO) streams RawEbpfEvents via gRPC
   └── polku.v1.Gateway.StreamEvents

2. Input plugin transforms to unified format
   └── RawEbpfEvent → AhtiEvent

3. Events buffered (ring buffer)
   └── FIFO eviction on overflow

4. Output plugin forwards to destination
   └── AhtiEvent → AHTI gRPC

5. Ack returned with backpressure signal
   └── buffer_size in response
```

### Plugin Traits

```rust
// Input: Transform raw bytes → Events
#[async_trait]
pub trait InputPlugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn transform(&self, source: &str, data: &[u8]) -> Result<Vec<Event>, PluginError>;
}

// Output: Send events to destination
#[async_trait]
pub trait OutputPlugin: Send + Sync {
    fn name(&self) -> &'static str;
    async fn send(&self, events: &[Event]) -> Result<(), PluginError>;
    async fn health(&self) -> bool;
}
```

### Proto Structure

Imports from the central [falsesystems/proto](https://github.com/falsesystems/proto) repo:

```
proto/
├── ahti/v1/events.proto      # AhtiEvent (unified format)
├── polku/v1/gateway.proto    # Gateway service
└── tapio/v1/raw.proto        # RawEbpfEvent (TAPIO format)
```

---

## Project Structure

```
polku/
├── Cargo.toml                # Workspace
├── CLAUDE.md                 # Agent instructions
├── gateway/
│   ├── Cargo.toml
│   ├── build.rs              # Proto compilation
│   └── src/
│       ├── main.rs           # Entry point
│       ├── lib.rs            # Library exports
│       ├── config.rs         # Configuration
│       ├── error.rs          # Error types
│       ├── buffer.rs         # Ring buffer
│       ├── metrics.rs        # Prometheus
│       ├── server.rs         # gRPC service
│       ├── input/mod.rs      # InputPlugin trait
│       └── output/mod.rs     # OutputPlugin trait
└── ../proto/                 # Central proto repo (sibling)
```

---

## Development

```bash
# Build
cargo build

# Test
cargo test

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt
```

---

## Tech Stack

| Crate | Purpose |
|-------|---------|
| `tonic` | gRPC server/client |
| `prost` | Protocol buffer serialization |
| `tokio` | Async runtime |
| `tracing` | Structured logging |
| `prometheus` | Metrics |
| `parking_lot` | Fast mutex |
| `thiserror` | Error types |

---

## The Ecosystem

| Tool | Purpose | Language |
|------|---------|----------|
| **TAPIO** | eBPF agent (kernel events) | Rust |
| **PORTTI** | K8s API watcher | Go |
| **ELAVA** | OTEL collector adapter | Go |
| **POLKU** | Event gateway | Rust |
| **AHTI** | Central intelligence | Elixir |

---

## Naming

**Polku** (Finnish: "path") - The path events take from edge agents to the central brain.

---

## License

Apache 2.0

---

**False Systems** 🇫🇮
