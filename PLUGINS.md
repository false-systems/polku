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
│  │ Agent A   │──►Adapter │            │           │ GrpcEmitter   │    │
│  │ Agent B   │──►Adapter │ Middleware │──►Buffer──│ WebhookEmitter│    │
│  │ CustomSrc │──►Adapter │   Chain    │           │ StdoutEmitter │    │
│  └───────────┘           └────────────┘           └───────────────┘    │
│       │                        │                         │              │
│       │                        │                         │              │
│   trait Ingestor          trait Middleware          trait Emitter       │
│   (decode bytes)          (transform/filter)        (send messages)    │
└─────────────────────────────────────────────────────────────────────────┘
```

POLKU uses a **Triadic Plugin Architecture**:

| Component | Purpose | You Implement |
|-----------|---------|---------------|
| **Ingestor** | Decode raw bytes from your protocol | `trait Ingestor` |
| **Middleware** | Transform, filter, route messages | `trait Middleware` |
| **Emitter** | Send messages to destination | `trait Emitter` |

---

## Creating an Emitter

Emitters send messages to destinations. This is the most common plugin type.

### The Emitter Trait

```rust
use polku_core::{Emitter, Message, PluginError};
use async_trait::async_trait;

#[async_trait]
pub trait Emitter: Send + Sync {
    /// Unique name for logging and routing
    fn name(&self) -> &'static str;

    /// Send a batch of messages to the destination
    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError>;

    /// Health check - is the destination reachable?
    async fn health(&self) -> bool;

    /// Graceful shutdown (optional - default is no-op)
    async fn shutdown(&self) -> Result<(), PluginError> {
        Ok(())
    }
}
```

### Example: gRPC Emitter

```rust
use polku_core::{Emitter, Message, PluginError};
use async_trait::async_trait;
use tonic::transport::Channel;

pub struct MyGrpcEmitter {
    client: MyServiceClient<Channel>,
    endpoint: String,
}

impl MyGrpcEmitter {
    pub async fn connect(endpoint: &str) -> Result<Self, PluginError> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| PluginError::Config(e.to_string()))?
            .connect()
            .await
            .map_err(|e| PluginError::Connection(e.to_string()))?;

        Ok(Self {
            client: MyServiceClient::new(channel),
            endpoint: endpoint.to_string(),
        })
    }
}

#[async_trait]
impl Emitter for MyGrpcEmitter {
    fn name(&self) -> &'static str {
        "my-grpc"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        // Convert POLKU Messages to your service's format
        let requests: Vec<MyRequest> = messages
            .iter()
            .map(|m| MyRequest {
                id: m.id.to_string(),
                payload: m.payload.to_vec(),
                // ... map other fields
            })
            .collect();

        self.client
            .clone()
            .send_batch(requests)
            .await
            .map_err(|e| PluginError::Send(e.to_string()))?;

        Ok(())
    }

    async fn health(&self) -> bool {
        self.client.clone().health_check().await.is_ok()
    }

    async fn shutdown(&self) -> Result<(), PluginError> {
        tracing::info!(endpoint = %self.endpoint, "MyGrpcEmitter shutting down");
        Ok(())
    }
}
```

### Example: Simple HTTP Webhook Emitter

```rust
use polku_core::{Emitter, Message, PluginError};
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

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        for msg in messages {
            self.client
                .post(&self.url)
                .header("X-Event-ID", msg.id.to_string())
                .header("X-Event-Type", msg.message_type.as_str())
                .body(msg.payload.clone())
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

Ingestors decode raw bytes from source protocols into POLKU Messages.

### The Ingestor Trait

```rust
use polku_gateway::{Ingestor, IngestContext, Message, PluginError};

pub trait Ingestor: Send + Sync {
    /// Unique name for identification
    fn name(&self) -> &'static str;

    /// Which source identifiers this ingestor handles
    fn sources(&self) -> &'static [&'static str];

    /// Decode raw bytes into Messages
    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError>;
}
```

### Example: JSON Adapter

```rust
use polku_gateway::{Ingestor, IngestContext, Message, PluginError};
use serde::Deserialize;

#[derive(Deserialize)]
struct SourceMessage {
    id: String,
    message_type: String,
    data: serde_json::Value,
    timestamp_ms: i64,
}

pub struct JsonAdapter;

impl Ingestor for JsonAdapter {
    fn name(&self) -> &'static str {
        "json-adapter"
    }

    fn sources(&self) -> &'static [&'static str] {
        &["my-source"]
    }

    fn ingest(&self, ctx: &IngestContext, data: &[u8]) -> Result<Vec<Message>, PluginError> {
        let items: Vec<SourceMessage> = serde_json::from_slice(data)
            .map_err(|e| PluginError::Decode(e.to_string()))?;

        let messages = items
            .into_iter()
            .map(|item| {
                let payload = serde_json::to_vec(&item.data).unwrap_or_default();
                Message::new(ctx.source, &item.message_type, payload.into())
            })
            .collect();

        Ok(messages)
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

POLKU provides 10 middleware out of the box:

| Middleware | Purpose |
|------------|---------|
| `Filter` | Drop messages that don't match a predicate |
| `Transform` | Modify messages (payload, metadata) |
| `Router` | Set `route_to` based on content |
| `RateLimiter` | Global rate limiting (token bucket) |
| `Throttle` | Per-source rate limiting |
| `Deduplicator` | Drop duplicate messages within time window |
| `Sampler` | Pass X% of messages (probabilistic) |
| `Enricher` | Add metadata via async function |
| `Validator` | Schema validation with drop/tag modes |
| `Aggregator` | Batch N messages into 1 combined message |

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
        msg.metadata_mut().insert("host".into(), self.hostname.clone());
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
    let grpc = MyGrpcEmitter::connect("http://my-service:50051").await?;
    let webhook = WebhookEmitter::new("https://api.example.com/events");

    // Build the hub
    let (sender, runner) = Hub::new()
        .buffer_capacity(10_000)
        // Middleware chain (order matters!)
        .middleware(Filter::new(|msg| !msg.message_type.starts_with("debug.")))
        .middleware(EnrichWithHostname::new())
        .middleware(Transform::new(|mut msg| {
            msg.metadata_mut().insert("version".into(), "1.0".into());
            msg
        }))
        // Emitters (fan-out to all)
        .emitter(grpc)
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
let grpc = Arc::new(MyGrpcEmitter::connect("http://my-service:50051").await?);

// Failure buffer for debugging (in-memory, NOT persistent)
let failure_buffer = Arc::new(FailureBuffer::new(1000));

// Wrap with resilience (innermost -> outermost)
let resilient = ResilientEmitter::wrap_arc(grpc)
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
    |
    v
+---------------------+
| FailureCaptureEmitter| <- Captures failures for debugging
+---------+-----------+
          |
          v
+---------------------+
| CircuitBreakerEmitter| <- Fail-fast when unhealthy
+---------+-----------+
          |
          v
+---------------------+
|    RetryEmitter     | <- Retry with exponential backoff
+---------+-----------+
          |
          v
+---------------------+
|   Your Emitter      | <- Actual destination
+---------------------+
```

---

## Error Handling

Use `PluginError` for all plugin errors:

```rust
use polku_core::PluginError;

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

    fn make_test_message(id: &str) -> Message {
        Message::new("test", "test.event", Bytes::from(vec![1, 2, 3]))
    }

    #[tokio::test]
    async fn test_emitter_health() {
        let emitter = MyEmitter::new("http://localhost:8080");
        let _ = emitter.health().await;
    }

    #[tokio::test]
    async fn test_emitter_emit() {
        let emitter = MyEmitter::new("http://localhost:8080");
        let messages = vec![make_test_message("e1"), make_test_message("e2")];

        let result = emitter.emit(&messages).await;
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
        async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
            self.0.fetch_add(messages.len(), Ordering::Relaxed);
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

## Summary

| I want to... | Implement | Key method |
|--------------|-----------|------------|
| Send messages to a new destination | `trait Emitter` | `emit(&[Message])` |
| Decode a new source format | `trait Ingestor` | `ingest(&[u8]) -> Vec<Message>` |
| Transform/filter messages | `trait Middleware` | `process(Message) -> Option<Message>` |
| Add retry/circuit breaker | `ResilientEmitter` builder | `.with_retry()`, `.with_circuit_breaker()` |
