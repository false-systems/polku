# External Emitter Design

## Overview

ExternalEmitter is a gRPC client that delegates event emission to external plugins. This enables writing emitter plugins in any language with gRPC support (Python, Go, Java, etc.).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         POLKU Gateway                                │
│                                                                      │
│  ┌─────────────┐    ┌──────────────────┐    ┌───────────────────┐  │
│  │  Middleware │───►│  ExternalEmitter │───►│ gRPC Client       │  │
│  │  Pipeline   │    │  (impl Emitter)  │    │ EmitterPluginClient│  │
│  └─────────────┘    └──────────────────┘    └─────────┬─────────┘  │
│                                                        │            │
└────────────────────────────────────────────────────────┼────────────┘
                                                         │
                                                         │ gRPC
                                                         ▼
                                              ┌────────────────────┐
                                              │  External Plugin   │
                                              │  (Go/Python/etc)   │
                                              │                    │
                                              │  EmitterPlugin     │
                                              │  service impl      │
                                              └────────────────────┘
```

## Key Differences from ExternalIngestor

| Aspect | ExternalIngestor | ExternalEmitter |
|--------|------------------|-----------------|
| Trait | `Ingestor` (sync) | `Emitter` (async) |
| Bridge | `block_in_place` needed | Native async, no bridge |
| Direction | Plugin → Events | Events → Destination |
| Error handling | Transform errors | Send errors with retry info |

## Implementation Design

### Connection Management
- Lazy connection on first `emit()` call
- `Arc<Mutex<Option<Client>>>` for shared, lazy-initialized client
- Auto-reconnect on connection failure (clear cache on error)

### Error Handling
```rust
// EmitResponse provides granular error information
pub struct EmitResponse {
    success_count: i64,
    failed_event_ids: Vec<String>,
    errors: Vec<EmitError>,
}

pub struct EmitError {
    event_id: String,
    message: String,
    retryable: bool,
}
```

Strategy:
- If all events succeed → return `Ok(())`
- If any events fail → return `Err(PluginError::Send(...))` with failed event IDs
- Let resilience layer (RetryEmitter, CircuitBreaker) handle retries

### Health Checks
- Call plugin's `Health()` RPC
- Return `true` if healthy, `false` if unhealthy or connection fails

### Shutdown
- Call plugin's `Shutdown()` RPC
- Allow plugin to flush buffers and close connections
- Return result from plugin

## API

```rust
use polku_gateway::emit::ExternalEmitter;

// Create emitter pointing to external plugin
let emitter = ExternalEmitter::new("splunk", "http://localhost:9001");

// Or with custom configuration
let emitter = ExternalEmitter::builder("splunk", "http://localhost:9001")
    .timeout(Duration::from_secs(30))
    .build();

// Register with hub
let hub = Hub::new()
    .emitter(emitter)
    .build();
```

## Protocol

Uses the `EmitterPlugin` service from `plugin.proto`:

```protobuf
service EmitterPlugin {
    rpc Info(google.protobuf.Empty) returns (PluginInfo);
    rpc Health(google.protobuf.Empty) returns (PluginHealthResponse);
    rpc Emit(EmitRequest) returns (EmitResponse);
    rpc Shutdown(google.protobuf.Empty) returns (ShutdownResponse);
}

message EmitRequest {
    repeated polku.event.v1.Event events = 1;
}

message EmitResponse {
    int64 success_count = 1;
    repeated string failed_event_ids = 2;
    repeated EmitError errors = 3;
}
```

## Test Plan

1. **Unit Tests** (no network)
   - `name()` returns "external:{emitter_name}"
   - Address is stored correctly

2. **Integration Tests** (mock gRPC server)
   - Successful emit returns Ok
   - Empty batch succeeds
   - Connection failure returns PluginError::Connection
   - Plugin error (failed events) returns PluginError::Send
   - Health check returns true/false
   - Shutdown calls plugin's Shutdown RPC
   - Multiple calls reuse connection
   - Connection is retried after failure

## File Structure

```
gateway/src/emit/
├── mod.rs           # Add ExternalEmitter export
└── external.rs      # NEW: ExternalEmitter implementation
```
