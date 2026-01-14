# ExternalIngestor Architecture Design

## Overview

ExternalIngestor enables language-agnostic plugins via gRPC. Plugin authors can implement
ingestors in any language with gRPC support (Go, Python, Java, etc.).

## Protocol

Uses the `IngestorPlugin` service defined in `proto/polku/v1/plugin.proto`:

```protobuf
service IngestorPlugin {
    rpc Info(Empty) returns (PluginInfo);      // Plugin metadata
    rpc Health(Empty) returns (PluginHealthResponse);
    rpc Ingest(IngestRequest) returns (IngestResponse);
}

message IngestRequest {
    string source = 1;   // Source identifier
    string cluster = 2;  // Cluster/environment
    string format = 3;   // Format hint
    bytes data = 4;      // Raw bytes to transform
}

message IngestResponse {
    repeated Event events = 1;
    repeated IngestError errors = 2;
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      POLKU GATEWAY                           │
│                                                              │
│  ┌──────────────────┐     ┌──────────────────────────────┐  │
│  │ ExternalIngestor │────▶│ IngestorPluginClient (tonic) │  │
│  │                  │     │                              │  │
│  │ • source: String │     │ • Lazy connection            │  │
│  │ • address: String│     │ • Auto-reconnect             │  │
│  │ • client: Lazy   │     │ • Timeout handling           │  │
│  └──────────────────┘     └──────────────────────────────┘  │
│            │                           │                     │
└────────────│───────────────────────────│─────────────────────┘
             │                           │
             │         gRPC/HTTP2        │
             │                           ▼
┌────────────│───────────────────────────────────────────────┐
│            │          EXTERNAL PLUGIN (any language)       │
│            ▼                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ IngestorPlugin Server                                │  │
│  │                                                      │  │
│  │ • Info() → PluginInfo { name, sources, ... }         │  │
│  │ • Health() → PluginHealthResponse                    │  │
│  │ • Ingest(data) → Vec<Event>                          │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                            │
│  Example implementations:                                  │
│  • Go: github.com/yairfalse/polku-plugin-go               │
│  • Python: pip install polku-plugin                       │
│  • Rust: polku_plugin crate                               │
└────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Sync-to-Async Bridge

**Problem**: `Ingestor::ingest()` is synchronous, but gRPC calls are async.

**Solution**: Use `tokio::task::block_in_place()` to run async code from sync context.

```rust
fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(self.ingest_async(ctx, data))
    })
}
```

**Why this approach**:
- `block_in_place` moves the current worker to blocking mode without spawning a new thread
- Works well for occasional blocking calls (plugins are typically high-latency anyway)
- Keeps the Ingestor trait simple and testable
- Alternative: Make Ingestor trait async, but that complicates the entire pipeline

### 2. Connection Management

**Strategy**: Lazy connection with reconnection on failure.

```rust
struct ExternalIngestor {
    source: String,
    address: String,
    client: OnceCell<IngestorPluginClient<Channel>>,
}
```

**Connection behavior**:
- Connect on first `ingest()` call, not at construction
- Cache the client for subsequent calls
- On connection error: clear cache, retry on next call
- Timeout: 5 seconds for initial connection, 30 seconds for ingest calls

**Why lazy connection**:
- Plugin may not be running when POLKU starts
- Allows graceful degradation if plugin is temporarily unavailable
- Construction is infallible (important for Hub::build())

### 3. Error Handling

| Error Type | Handling |
|------------|----------|
| Connection failed | Return `PluginError::Connection`, clear client cache |
| Plugin returned errors | Return `PluginError::Transform` with details |
| Timeout | Return `PluginError::Connection` with timeout message |
| Partial success | Return events + log warnings for individual errors |

### 4. Plugin Contract

Plugins MUST implement:
1. `Info()` - Return metadata including which sources this plugin handles
2. `Health()` - Return health status (used by POLKU health checks)
3. `Ingest()` - Transform raw bytes into Events

Plugins SHOULD:
- Return meaningful error details in IngestError
- Handle partial failures (return events that succeed, errors for failures)
- Respond to Health() even under load

## Configuration

```rust
let hub = Hub::new()
    .ingestor(ExternalIngestor::new("datadog", "localhost:9001"))
    .ingestor(ExternalIngestor::new("custom-format", "plugins.internal:9002"))
    .build();
```

## Test Strategy

### Unit Tests (mock gRPC)
- Test connection failure handling
- Test timeout behavior
- Test partial error handling (some events succeed, some fail)
- Test reconnection after failure

### Integration Tests (real plugin)
- Start a test plugin server (in Rust using tonic)
- Test full roundtrip: raw bytes → plugin → Events
- Test plugin crash recovery

## Performance Considerations

| Aspect | Impact | Mitigation |
|--------|--------|------------|
| gRPC latency | 100-500μs per call | Batch events, use built-in ingestors for high-volume |
| Connection overhead | ~1ms initial | Lazy connect, connection reuse |
| Serialization | ~10μs for typical payload | Protobuf is efficient |

**Recommendation**: For sources exceeding 10k events/second, implement a native Rust ingestor.

## Future Extensions

1. **Connection pooling** - Multiple connections per plugin for parallelism
2. **Circuit breaker** - Stop calling failing plugins temporarily
3. **Plugin discovery** - Plugins register via PluginRegistry service
4. **Streaming** - Bidirectional streaming for lower latency
