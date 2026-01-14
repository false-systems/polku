# Plugin Discovery Design

## Overview

Plugin Discovery allows external plugins to **self-register** with POLKU at runtime instead of being statically configured. POLKU exposes a `PluginRegistry` gRPC service that plugins call on startup.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              POLKU Gateway                                       │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                      PluginRegistryServer                                  │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐  │ │
│  │  │  gRPC Service: polku.v1.PluginRegistry                              │  │ │
│  │  │                                                                      │  │ │
│  │  │    Register()   ─────►  Create ExternalIngestor/Emitter             │  │ │
│  │  │    Heartbeat()  ─────►  Update last-seen timestamp                  │  │ │
│  │  │    Unregister() ─────►  Remove from registry                        │  │ │
│  │  └─────────────────────────────────────────────────────────────────────┘  │ │
│  │                              │                                             │ │
│  │                              ▼                                             │ │
│  │                   ┌──────────────────────┐                                │ │
│  │                   │   DynamicRegistry    │                                │ │
│  │                   │                      │                                │ │
│  │                   │  plugins: HashMap    │                                │ │
│  │                   │  plugin_id → info    │                                │ │
│  │                   └──────────────────────┘                                │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │ Register()         │ Register()         │ Register()
         │                    │                    │
    ┌────┴─────┐        ┌────┴─────┐        ┌────┴─────┐
    │ Python   │        │ Go       │        │ Rust     │
    │ Plugin   │        │ Plugin   │        │ Plugin   │
    │          │        │          │        │          │
    │ Ingestor │        │ Emitter  │        │ Emitter  │
    └──────────┘        └──────────┘        └──────────┘
```

## Flow

### 1. Plugin Registration

```
Plugin                          POLKU
  │                               │
  │─────── Register() ───────────►│
  │        {info, address}        │
  │                               │ ─► Validate plugin info
  │                               │ ─► Create ExternalIngestor/Emitter
  │                               │ ─► Add to registry
  │◄───── RegisterResponse ───────│
  │       {accepted, plugin_id}   │
```

### 2. Heartbeat (keepalive)

```
Plugin                          POLKU
  │                               │
  │─────── Heartbeat() ──────────►│
  │        {plugin_id}            │
  │                               │ ─► Update last_seen timestamp
  │◄───── HeartbeatResponse ──────│
  │       {acknowledged}          │
```

### 3. Graceful Shutdown

```
Plugin                          POLKU
  │                               │
  │─────── Unregister() ─────────►│
  │        {plugin_id}            │
  │                               │ ─► Remove from registry
  │                               │ ─► Shutdown emitter if needed
  │◄───── Empty ──────────────────│
```

## Protocol (from plugin.proto)

```protobuf
service PluginRegistry {
    rpc Register(RegisterRequest) returns (RegisterResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Unregister(UnregisterRequest) returns (google.protobuf.Empty);
}

message RegisterRequest {
    PluginInfo info = 1;
    string address = 2;  // e.g., "localhost:9001"
}

message RegisterResponse {
    bool accepted = 1;
    string message = 2;
    string plugin_id = 3;  // Used for heartbeat/unregister
}
```

## Implementation Design

### DynamicRegistry

Thread-safe container for dynamically registered plugins:

```rust
pub struct DynamicRegistry {
    /// Registered plugins
    plugins: RwLock<HashMap<String, RegisteredPlugin>>,
    /// Plugin registry to add ingestors/emitters to
    registry: Arc<PluginRegistry>,
}

struct RegisteredPlugin {
    id: String,
    info: PluginInfo,
    address: String,
    registered_at: Instant,
    last_heartbeat: Instant,
}
```

### PluginRegistryServer

gRPC service implementation:

```rust
#[tonic::async_trait]
impl PluginRegistryService for PluginRegistryServer {
    async fn register(&self, request: Request<RegisterRequest>)
        -> Result<Response<RegisterResponse>, Status>;

    async fn heartbeat(&self, request: Request<HeartbeatRequest>)
        -> Result<Response<HeartbeatResponse>, Status>;

    async fn unregister(&self, request: Request<UnregisterRequest>)
        -> Result<Response<()>, Status>;
}
```

### Plugin ID Generation

Use ULID for unique, sortable plugin IDs:

```rust
fn generate_plugin_id() -> String {
    ulid::Ulid::new().to_string()
}
```

## Error Handling

| Error | Behavior |
|-------|----------|
| Duplicate registration | Replace existing (same source/emitter_name) |
| Invalid plugin info | Return `accepted: false` with message |
| Unknown plugin_id in heartbeat | Return `acknowledged: false` |
| Connection failure to plugin | Mark as unhealthy, don't remove |

## Health Monitoring

Optional: Background task to check plugin health:

```rust
async fn health_monitor(registry: Arc<DynamicRegistry>) {
    loop {
        for plugin in registry.all_plugins() {
            // Check if heartbeat is stale (> 30s)
            if plugin.last_heartbeat.elapsed() > Duration::from_secs(30) {
                registry.mark_unhealthy(&plugin.id);
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
```

## Test Plan

1. **Unit Tests**
   - Plugin ID generation is unique
   - Register creates correct ingestor/emitter type
   - Duplicate registration replaces existing
   - Unregister removes plugin

2. **Integration Tests** (mock plugin)
   - Full registration flow
   - Heartbeat updates timestamp
   - Unregister removes from registry
   - Invalid registration returns error

## File Structure

```
gateway/src/
├── registry.rs              # Existing static PluginRegistry
├── discovery/
│   ├── mod.rs               # Module exports
│   ├── dynamic_registry.rs  # DynamicRegistry implementation
│   └── server.rs            # PluginRegistryServer gRPC impl
```
