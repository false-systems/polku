# POLKU Plugin Development Guide

Build custom Ingestors and Emitters to connect POLKU to any protocol.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              POLKU HUB                                  │
│                                                                         │
│   YOUR SOURCES              POLKU CORE              YOUR DESTINATIONS   │
│  ┌───────────┐           ┌────────────┐           ┌───────────────┐    │
│  │ Tapio     │──►Adapter │            │           │ AhtiEmitter   │    │
│  │ Portti    │──►Adapter │ Middleware │──►Buffer──│ KafkaEmitter  │    │
│  │ CustomSrc │──►Adapter │   Chain    │           │ S3Emitter     │    │
│  └───────────┘           └────────────┘           └───────────────┘    │
│       │                        │                         │              │
│       │                        │                         │              │
│   trait Ingestor          trait Middleware          trait Emitter       │
│   (decode bytes)          (transform/filter)        (send events)       │
└─────────────────────────────────────────────────────────────────────────┘
```

POLKU uses a **Triadic Plugin Architecture**:

| Component | Purpose | You Implement |
|-----------|---------|---------------|
| **Ingestor** | Decode raw bytes from your protocol | `trait Ingestor` |
| **Middleware** | Transform, filter, route messages | `trait Middleware` |
| **Emitter** | Send events to destination | `trait Emitter` |

---

## Creating an Emitter

Emitters send events to destinations. This is the most common plugin type.

### The Emitter Trait

```rust
use polku_gateway::{Emitter, Event, PluginError};
use async_trait::async_trait;

#[async_trait]
pub trait Emitter: Send + Sync {
    /// Unique name for logging and routing
    fn name(&self) -> &'static str;

    /// Send a batch of events to the destination
    async fn emit(&self, events: &[Event]) -> Result<(), PluginError>;

    /// Health check - is the destination reachable?
    async fn health(&self) -> bool;

    /// Graceful shutdown (optional - default is no-op)
    async fn shutdown(&self) -> Result<(), PluginError> {
        Ok(())
    }
}
```

### Example: Ahti Emitter (gRPC Client)

```rust
use polku_gateway::{Emitter, Event, PluginError};
use async_trait::async_trait;
use tonic::transport::Channel;

pub struct AhtiEmitter {
    client: AhtiClient<Channel>,
    endpoint: String,
}

impl AhtiEmitter {
    pub async fn connect(endpoint: &str) -> Result<Self, PluginError> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| PluginError::Config(e.to_string()))?
            .connect()
            .await
            .map_err(|e| PluginError::Connection(e.to_string()))?;

        Ok(Self {
            client: AhtiClient::new(channel),
            endpoint: endpoint.to_string(),
        })
    }
}

#[async_trait]
impl Emitter for AhtiEmitter {
    fn name(&self) -> &'static str {
        "ahti"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        // Convert POLKU Events to Ahti's format
        let ahti_events: Vec<AhtiEvent> = events
            .iter()
            .map(|e| AhtiEvent {
                id: e.id.clone(),
                payload: e.payload.clone(),
                // ... map other fields
            })
            .collect();

        // Send to Ahti
        self.client
            .clone()
            .send_events(ahti_events)
            .await
            .map_err(|e| PluginError::Send(e.to_string()))?;

        Ok(())
    }

    async fn health(&self) -> bool {
        self.client.clone().health_check().await.is_ok()
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        tracing::info!(endpoint = %self.endpoint, "AhtiEmitter shutting down");
        Ok(())
    }
}
```

### Example: Simple HTTP Webhook Emitter

```rust
use polku_gateway::{Emitter, Event, PluginError};
use async_trait::async_trait;

pub struct WebhookEmitter {
    client: reqwest::Client,
    url: String,
}

impl WebhookEmitter {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
        }
    }
}

#[async_trait]
impl Emitter for WebhookEmitter {
    fn name(&self) -> &'static str {
        "webhook"
    }

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        for event in events {
            self.client
                .post(&self.url)
                .header("X-Event-ID", &event.id)
                .header("X-Event-Type", &event.event_type)
                .body(event.payload.clone())
                .send()
                .await
                .map_err(|e| PluginError::Send(e.to_string()))?;
        }
        Ok(())
    }

    async fn health(&self) -> bool {
        self.client.get(&self.url).send().await.is_ok()
    }
}
```

---

## Creating an Ingestor

Ingestors decode raw bytes from source protocols into POLKU Events.

### The Ingestor Trait

```rust
use polku_gateway::{Ingestor, IngestContext, Event, PluginError};

pub trait Ingestor: Send + Sync {
    /// Unique name for identification
    fn name(&self) -> &'static str;

    /// Decode raw bytes into Events
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError>;
}
```

### The IngestContext

```rust
pub struct IngestContext<'a> {
    /// Source identifier (e.g., "my-agent", "otel-collector")
    pub source: &'a str,
    /// Cluster/environment identifier
    pub cluster: &'a str,
    /// Format hint (e.g., "protobuf", "json", "msgpack")
    pub format: &'a str,
}
```

### Example: Tapio Adapter (JSON to Event)

```rust
use polku_gateway::{Ingestor, IngestContext, Event, PluginError};
use serde::Deserialize;

#[derive(Deserialize)]
struct TapioMessage {
    id: String,
    message_type: String,
    data: serde_json::Value,
    timestamp_ms: i64,
}

pub struct TapioAdapter;

impl Ingestor for TapioAdapter {
    fn name(&self) -> &'static str {
        "tapio"
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
        // Parse Tapio's JSON format
        let messages: Vec<TapioMessage> = serde_json::from_slice(data)
            .map_err(|e| PluginError::Decode(e.to_string()))?;

        // Convert to POLKU Events
        let events = messages
            .into_iter()
            .map(|msg| Event {
                id: msg.id,
                timestamp_unix_ns: msg.timestamp_ms * 1_000_000, // ms → ns
                source: ctx.source.to_string(),
                event_type: msg.message_type,
                metadata: std::collections::HashMap::new(),
                payload: serde_json::to_vec(&msg.data).unwrap_or_default(),
                route_to: vec![],
            })
            .collect();

        Ok(events)
    }
}
```

### Example: Protobuf Ingestor

```rust
use polku_gateway::{Ingestor, IngestContext, Event, PluginError};
use prost::Message;

// Your proto-generated type
// message PorttiEvent { string id = 1; bytes payload = 2; ... }

pub struct PorttiAdapter;

impl Ingestor for PorttiAdapter {
    fn name(&self) -> &'static str {
        "portti"
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Event>, PluginError> {
        // Decode protobuf
        let portti_event = PorttiEvent::decode(data)
            .map_err(|e| PluginError::Decode(e.to_string()))?;

        // Convert to POLKU Event
        let event = Event {
            id: portti_event.id,
            timestamp_unix_ns: portti_event.timestamp_ns,
            source: ctx.source.to_string(),
            event_type: portti_event.event_type,
            metadata: portti_event.headers,
            payload: portti_event.payload,
            route_to: vec![],
        };

        Ok(vec![event])
    }
}
```

---

## Creating Middleware

Middleware transforms, filters, or routes messages as they flow through the pipeline.

### The Middleware Trait

```rust
use polku_gateway::{Middleware, Message};
use async_trait::async_trait;

#[async_trait]
pub trait Middleware: Send + Sync {
    /// Unique name for logging
    fn name(&self) -> &'static str;

    /// Process a message
    /// Return Some(msg) to pass through, None to drop
    async fn process(&self, msg: Message) -> Option<Message>;
}
```

### Built-in Middleware

POLKU provides several middleware out of the box:

```rust
use polku_gateway::{Filter, Transform, Router, RateLimiter, Deduplicator, Sampler};

// Filter: drop messages that don't match
let filter = Filter::new(|msg| msg.message_type.starts_with("important."));

// Transform: modify messages
let transform = Transform::new(|mut msg| {
    msg.metadata.insert("processed".into(), "true".into());
    msg
});

// Router: set route_to based on content
let router = Router::new(|msg| {
    if msg.message_type.contains("critical") {
        vec!["pagerduty".into(), "slack".into()]
    } else {
        vec!["logs".into()]
    }
});
```

### Example: Custom Enrichment Middleware

```rust
use polku_gateway::{Middleware, Message};
use async_trait::async_trait;

pub struct EnrichWithHostname {
    hostname: String,
}

impl EnrichWithHostname {
    pub fn new() -> Self {
        Self {
            hostname: hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "unknown".into()),
        }
    }
}

#[async_trait]
impl Middleware for EnrichWithHostname {
    fn name(&self) -> &'static str {
        "enrich_hostname"
    }

    async fn process(&self, mut msg: Message) -> Option<Message> {
        msg.metadata.insert("host".into(), self.hostname.clone());
        Some(msg)
    }
}
```

---

## Registering Plugins with the Hub

```rust
use polku_gateway::{Hub, Filter, Transform};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create your plugins
    let ahti = AhtiEmitter::connect("http://ahti:50051").await?;
    let webhook = WebhookEmitter::new("https://api.example.com/events");

    // Build the hub
    let (sender, runner) = Hub::new()
        .buffer_capacity(10_000)
        // Middleware chain (order matters!)
        .middleware(Filter::new(|msg| !msg.message_type.starts_with("debug.")))
        .middleware(EnrichWithHostname::new())
        .middleware(Transform::new(|mut msg| {
            msg.metadata.insert("version".into(), "1.0".into());
            msg
        }))
        // Emitters (fan-out to all)
        .emitter(ahti)
        .emitter(webhook)
        .build();

    // Run the hub
    runner.run().await?;

    Ok(())
}
```

---

## Adding Resilience to Emitters

POLKU provides composable resilience wrappers. Wrap any emitter with retry, circuit breaker, and failure capture:

```rust
use polku_gateway::{
    ResilientEmitter, BackoffConfig, CircuitBreakerConfig, FailureBuffer, FailureCaptureConfig,
};
use std::sync::Arc;
use std::time::Duration;

// Your base emitter
let ahti = Arc::new(AhtiEmitter::connect("http://ahti:50051").await?);

// Failure buffer for debugging (in-memory, NOT persistent)
let failure_buffer = Arc::new(FailureBuffer::new(1000));

// Wrap with resilience (innermost → outermost)
let resilient = ResilientEmitter::wrap_arc(ahti)
    .with_retry(BackoffConfig {
        max_attempts: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
        multiplier: 2.0,
        jitter_factor: 0.25,
    })
    .with_circuit_breaker(CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        reset_timeout: Duration::from_secs(30),
        half_open_max_requests: 3,
    })
    .with_failure_capture(failure_buffer.clone(), FailureCaptureConfig::default())
    .build();

// Use in Hub
let hub = Hub::new()
    .emitter_arc(resilient)
    .build();
```

### Wrapper Order Matters

```
emit() call
    │
    ▼
┌─────────────────────┐
│ FailureCaptureEmitter│ ← Captures failures for debugging
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ CircuitBreakerEmitter│ ← Fail-fast when unhealthy
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│    RetryEmitter     │ ← Retry with exponential backoff
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Your Emitter      │ ← Actual destination
└─────────────────────┘
```

---

## The Event Type

This is what flows through the pipeline:

```rust
pub struct Event {
    /// Unique identifier (ULID recommended)
    pub id: String,

    /// Unix timestamp in nanoseconds
    pub timestamp_unix_ns: i64,

    /// Origin identifier
    pub source: String,

    /// User-defined event type (e.g., "user.created")
    pub event_type: String,

    /// Headers and context
    pub metadata: HashMap<String, String>,

    /// Opaque payload bytes
    pub payload: Vec<u8>,

    /// Routing hints (empty = broadcast to all emitters)
    pub route_to: Vec<String>,
}
```

---

## The Message Type (Internal)

For middleware processing, POLKU uses a zero-copy `Message` type internally:

```rust
pub struct Message {
    pub id: String,
    pub timestamp: i64,
    pub source: String,
    pub message_type: String,
    pub metadata: HashMap<String, String>,
    pub payload: Bytes,        // Zero-copy! Arc-based reference counting
    pub route_to: Vec<String>,
}
```

The `Bytes` type means cloning a `Message` doesn't copy the payload - it just increments a reference count. This is critical for high-throughput scenarios.

---

## Error Handling

Use `PluginError` for all plugin errors:

```rust
use polku_gateway::PluginError;

// Available error variants:
PluginError::Config(String)      // Configuration error
PluginError::Connection(String)  // Connection failed
PluginError::Send(String)        // Failed to send
PluginError::Decode(String)      // Failed to decode input
PluginError::Encode(String)      // Failed to encode output
PluginError::NotReady(String)    // Plugin not ready (circuit breaker)
PluginError::Timeout(String)     // Operation timed out
PluginError::Other(String)       // Generic error
```

---

## Testing Your Plugins

### Unit Testing an Emitter

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event(id: &str) -> Event {
        Event {
            id: id.to_string(),
            timestamp_unix_ns: 0,
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            metadata: HashMap::new(),
            payload: vec![1, 2, 3],
            route_to: vec![],
        }
    }

    #[tokio::test]
    async fn test_emitter_health() {
        let emitter = MyEmitter::new("http://localhost:8080");
        // Health check should work even if destination is down
        let _ = emitter.health().await;
    }

    #[tokio::test]
    async fn test_emitter_emit() {
        let emitter = MyEmitter::new("http://localhost:8080");
        let events = vec![make_test_event("e1"), make_test_event("e2")];

        let result = emitter.emit(&events).await;
        // Assert based on your emitter's behavior
    }
}
```

### Integration Testing with Hub

```rust
#[tokio::test]
async fn test_full_pipeline() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Counting emitter for testing
    struct CountingEmitter(AtomicUsize);

    #[async_trait]
    impl Emitter for CountingEmitter {
        fn name(&self) -> &'static str { "counter" }
        async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
            self.0.fetch_add(events.len(), Ordering::Relaxed);
            Ok(())
        }
        async fn health(&self) -> bool { true }
    }

    let counter = Arc::new(CountingEmitter(AtomicUsize::new(0)));

    let (sender, runner) = Hub::new()
        .emitter_arc(counter.clone())
        .build();

    // Send messages
    tokio::spawn(async move {
        for i in 0..10 {
            sender.send(Message::new("test", format!("evt-{i}"), Bytes::new())).await.ok();
        }
    });

    // Run briefly
    tokio::time::timeout(Duration::from_millis(100), runner.run()).await.ok();

    assert_eq!(counter.0.load(Ordering::Relaxed), 10);
}
```

---

## Best Practices

1. **Use `&'static str` for names** - Avoids allocations on every log
2. **Batch operations** - `emit()` receives a slice, send in batches when possible
3. **Health checks should be fast** - Don't block on slow operations
4. **Handle shutdown gracefully** - Flush buffers, close connections
5. **Use structured logging** - `tracing::{info, warn, error}` with fields
6. **Return proper errors** - Use specific `PluginError` variants
7. **Don't panic** - Return `Err` instead, let the Hub handle failures

---

## File Structure

When adding plugins to POLKU:

```
gateway/src/
├── emit/
│   ├── mod.rs           # Add: pub mod my_emitter;
│   ├── my_emitter.rs    # Your emitter implementation
│   └── ...
├── ingest/
│   ├── mod.rs           # Add: pub mod my_adapter;
│   ├── my_adapter.rs    # Your ingestor implementation
│   └── ...
└── lib.rs               # Re-export: pub use emit::MyEmitter;
```

---

## Summary

| I want to... | Implement | Key method |
|--------------|-----------|------------|
| Send events to a new destination | `trait Emitter` | `emit(&[Event])` |
| Decode a new source format | `trait Ingestor` | `ingest(&[u8]) → Vec<Event>` |
| Transform/filter messages | `trait Middleware` | `process(Message) → Option<Message>` |
| Add retry/circuit breaker | `ResilientEmitter` builder | `.with_retry()`, `.with_circuit_breaker()` |
