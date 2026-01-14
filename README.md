# POLKU

**The path your events take.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

```
Events in  ───►  POLKU  ───►  Events out
              (transform)
              (filter)
              (route)
```

---

## What is POLKU?

POLKU is an **event gateway**. Events come in, get transformed, and go out to multiple destinations.

```
                     ┌────────────────────────────────────────────────────┐
                     │                      POLKU                          │
                     │                                                     │
   Your App ────────►│   Ingestors ──► Middleware ──► Buffer ──► Emitters │────────► Slack
                     │       │              │                        │     │
   Agents ──────────►│       │         [transform]                   │     │────────► S3
                     │       │         [filter]                      │     │
   Webhooks ────────►│       │         [route]                       │     │────────► Kafka
                     │       │                                       │     │
                     │       ▼                                       ▼     │
                     │   ┌────────┐                            ┌────────┐  │
                     │   │ Plugin │  ◄── gRPC ──►              │ Plugin │  │
                     │   │ (any   │                            │ (any   │  │
                     │   │  lang) │                            │  lang) │  │
                     │   └────────┘                            └────────┘  │
                     └────────────────────────────────────────────────────┘
```

**Why POLKU?**

- **No YAML** - Routing logic is code, not config files
- **Any language** - Write plugins in Python, Go, Rust, whatever
- **Tiny** - 10-20MB memory, no dependencies
- **Fast** - 100k+ events/sec

---

## Quick Start

```bash
# Clone and build
git clone https://github.com/yairfalse/polku
cd polku && cargo build --release

# Run the gateway
./target/release/polku-gateway

# Send an event
grpcurl -plaintext -d '{
  "source": "my-app",
  "events": {"events": [{"id": "1", "event_type": "user.signup"}]}
}' localhost:50051 polku.v1.Gateway/SendEvent
```

That's it. Events flow through POLKU.

---

## Write a Plugin in 5 Minutes

Want to send events to Slack? Parse a custom format? Write a plugin.

### Python: Slack Emitter

```python
# slack_emitter.py
# Sends events to Slack as pretty messages

class SlackEmitter:

    def Info(self, request, context):
        return PluginInfo(
            name="slack-emitter",
            type=EMITTER,
            emitter_name="slack",  # ← POLKU knows us as "slack"
        )

    def Emit(self, request, context):
        for event in request.events:
            # Format the event nicely
            message = f"*{event.event_type}* from `{event.source}`"

            # Send to Slack
            requests.post(SLACK_WEBHOOK, json={"text": message})

        return EmitResponse(success_count=len(request.events))

# Start the plugin
server = grpc.server(ThreadPoolExecutor())
server.add_insecure_port('[::]:9001')
server.start()
```

```bash
# Run it
python slack_emitter.py

# Tell POLKU about it (or use auto-discovery)
# → Events now flow to Slack!
```

### Go: CSV Ingestor

```go
// csv_ingestor.go
// Turns CSV data into events

func (p *CSVIngestor) Info(ctx context.Context, _ *empty.Empty) (*PluginInfo, error) {
    return &PluginInfo{
        Name:    "csv-ingestor",
        Type:    INGESTOR,
        Sources: []string{"csv"},  // ← Handle source: "csv"
    }, nil
}

func (p *CSVIngestor) Ingest(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
    reader := csv.NewReader(bytes.NewReader(req.Data))
    headers, _ := reader.Read()

    var events []*Event
    for {
        row, err := reader.Read()
        if err != nil { break }

        // Each CSV row becomes an event
        metadata := make(map[string]string)
        for i, h := range headers {
            metadata[h] = row[i]
        }

        events = append(events, &Event{
            Id:        uuid.New().String(),
            EventType: "csv.row",
            Metadata:  metadata,
        })
    }

    return &IngestResponse{Events: events}, nil
}
```

```bash
# Run it
go run csv_ingestor.go

# Send CSV data to POLKU with source: "csv"
# → Each row becomes an event!
```

---

## Plugin Discovery (Auto-Registration)

Plugins can register themselves. No config needed.

```python
# In your plugin, call POLKU's registry:
stub = PluginRegistryStub(channel)
stub.Register(RegisterRequest(
    info=PluginInfo(name="my-plugin", type=EMITTER, emitter_name="slack"),
    address="localhost:9001",
))

# POLKU now routes events to your plugin
# Send heartbeats to stay registered
stub.Heartbeat(HeartbeatRequest(plugin_id=response.plugin_id))
```

Or configure statically in Rust:

```rust
Hub::new()
    .ingestor(ExternalIngestor::new("csv", "http://localhost:9002"))
    .emitter(ExternalEmitter::new("slack", "http://localhost:9001"))
    .build();
```

---

## The Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                           EVENT FLOW                                 │
│                                                                     │
│   INGEST           TRANSFORM            BUFFER           EMIT       │
│  ┌───────┐        ┌───────────┐        ┌───────┐       ┌───────┐   │
│  │ bytes │───────►│  Filter   │───────►│ Ring  │──────►│ Fan   │   │
│  │   ↓   │        │  Enrich   │        │Buffer │       │ Out   │   │
│  │ Event │        │  Route    │        │(async)│       │       │   │
│  └───────┘        └───────────┘        └───────┘       └───────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

1. **Ingestors** - Parse bytes into Events (JSON, protobuf, CSV, custom)
2. **Middleware** - Transform, filter, enrich, route
3. **Buffer** - Decouple ingestion from emission (backpressure)
4. **Emitters** - Send to destinations (Slack, S3, Kafka, custom)

---

## Built-in Components

### Ingestors
| Name | Description |
|------|-------------|
| `PassthroughIngestor` | Events already in protobuf format |
| `JsonIngestor` | JSON → Event |
| `ExternalIngestor` | Delegate to any gRPC plugin |

### Middleware
| Name | What it does |
|------|--------------|
| `Filter` | Drop events that don't match |
| `Transform` | Modify events |
| `Router` | Route to specific emitters |
| `RateLimiter` | Limit events/sec |
| `Sampler` | Sample N% of events |
| `Deduplicator` | Drop duplicates |

### Emitters
| Name | Description |
|------|-------------|
| `StdoutEmitter` | Print to console (debugging) |
| `GrpcEmitter` | Send via gRPC |
| `WebhookEmitter` | HTTP POST |
| `ExternalEmitter` | Delegate to any gRPC plugin |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLKU_GRPC_ADDR` | `0.0.0.0:50051` | Listen address |
| `POLKU_BUFFER_CAPACITY` | `10000` | Buffer size |
| `POLKU_LOG_LEVEL` | `info` | Log level |

### Programmatic (Rust)

```rust
use polku_gateway::*;

let hub = Hub::new()
    // Parse JSON events
    .ingestor(JsonIngestor::new())

    // External plugin for CSV
    .ingestor(ExternalIngestor::new("csv", "http://localhost:9002"))

    // Filter to only user events
    .middleware(Filter::new(|e| e.event_type.starts_with("user.")))

    // Add metadata
    .middleware(Transform::new(|mut e| {
        e.metadata.insert("env".into(), "prod".into());
        e
    }))

    // Send to stdout and Slack
    .emitter(StdoutEmitter::new())
    .emitter(ExternalEmitter::new("slack", "http://localhost:9001"))

    .build();
```

---

## Performance

| Metric | Value |
|--------|-------|
| Memory | 10-20 MB |
| Throughput | 100k+ events/sec |
| Plugin latency | ~100-500μs |

For high-volume sources (>10k/sec), write a native Rust ingestor instead of using external plugins.

---

## Project Structure

```
polku/
├── proto/polku/v1/
│   ├── gateway.proto      # Client → POLKU
│   ├── plugin.proto       # POLKU ↔ Plugins
│   └── event.proto        # Event schema
│
├── core/                  # Shared types (Emitter, Event, Error)
│
├── gateway/               # The gateway itself
│   ├── ingest/            # Ingestors
│   ├── middleware/        # Middleware
│   ├── emit/              # Emitters
│   └── discovery/         # Plugin auto-registration
│
└── examples/plugins/      # Example plugins (Python, Go)
```

---

## Naming

**Polku** (Finnish) = "path"

The path your events take through the system.

---

## License

Apache 2.0
