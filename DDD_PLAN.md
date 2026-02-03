# POLKU DDD Improvement Plan

## Overview

This document outlines the issues and implementation plan to address DDD violations identified in the codebase. Issues are ordered by priority and dependency.

---

## Issues

### CRITICAL

#### Issue #1: Split PluginRegistry into Separate Registries
**Priority:** P0
**Effort:** Medium
**Context Boundary Violation**

**Problem:**
```rust
pub struct PluginRegistry {
    ingestors: HashMap<String, Arc<dyn Ingestor>>,  // Ingest context
    emitters: Vec<Arc<dyn Emitter>>,                // Emit context
    default_ingestor: Option<Arc<dyn Ingestor>>,
}
```

Single registry manages two separate bounded contexts. Can't use Ingest without knowing about Emit.

**Solution:**
- Create `IngestorRegistry` in `src/ingest/registry.rs`
- Create `EmitterRegistry` in `src/emit/registry.rs`
- Update Hub builder to accept both registries
- Remove `PluginRegistry`

**Files to change:**
- `src/registry.rs` → split into two
- `src/hub.rs` → update builder
- `src/runner.rs` → update dependencies
- Tests

**Acceptance Criteria:**
- [ ] `IngestorRegistry` manages only ingestors
- [ ] `EmitterRegistry` manages only emitters
- [ ] Hub builder accepts both separately
- [ ] No compile errors
- [ ] All tests pass

---

#### Issue #2: Encapsulate Message.route_to
**Priority:** P0
**Effort:** Small
**Aggregate Boundary Violation**

**Problem:**
```rust
pub struct Message {
    pub route_to: Routes,  // Anyone can mutate
}
```

External code can modify routing, violating invariant "routes must be valid emitter names."

**Solution:**
```rust
pub struct Message {
    route_to: Routes,  // Private
}

impl Message {
    pub fn routes(&self) -> &[String] { ... }
    pub fn add_route(&mut self, route: impl Into<String>) { ... }
    pub fn with_routes(self, routes: impl IntoIterator<Item = String>) -> Self { ... }
    pub fn clear_routes(&mut self) { ... }
}
```

**Files to change:**
- `src/message.rs`
- `src/middleware/router.rs`
- Any code accessing `msg.route_to` directly

**Acceptance Criteria:**
- [ ] `route_to` field is private
- [ ] Public methods for route manipulation
- [ ] Router middleware uses methods, not direct access
- [ ] All tests pass

---

#### Issue #3: Unify Message/Event Model
**Priority:** P0
**Effort:** Large
**Model Duality**

**Problem:**
Two parallel representations of the same domain concept:
- `Event` (proto-generated, external interface)
- `Message` (Rust struct, internal)

Tests convert back and forth, suggesting unclear boundaries.

**Solution:**
- `Message` is THE internal domain model
- `Event` is ONLY for gRPC transport
- Convert at system boundaries only:
  - gRPC server receives `Event` → converts to `Message`
  - gRPC emitter converts `Message` → `Event` for sending
- Remove `From<Event> for Message` and `From<Message> for Event` from public API
- Make conversions explicit in boundary code only

**Files to change:**
- `src/message.rs` → remove public From impls
- `src/grpc/server.rs` → add explicit conversion
- `src/emitters/grpc.rs` → add explicit conversion
- `polku-core/src/event.rs` → audit usage
- Tests → update to not rely on implicit conversion

**Acceptance Criteria:**
- [ ] No implicit Event↔Message conversion in domain code
- [ ] Conversions only at gRPC boundary
- [ ] Clear documentation of when to use which
- [ ] All tests pass

---

#### Issue #4: Add Domain Events
**Priority:** P1
**Effort:** Large
**Missing Pattern**

**Problem:**
Side effects are invisible. No way to:
- Hook into checkpoint updates for observability
- React to delivery failures at domain level
- Build audit logs of routing decisions

**Solution:**
Define domain events:
```rust
pub enum DomainEvent {
    MessageIngested { id: MessageId, source: String, timestamp: i64 },
    MessageRouted { id: MessageId, routes: Vec<String> },
    DeliverySucceeded { id: MessageId, emitter: String },
    DeliveryFailed { id: MessageId, emitter: String, error: String },
    CheckpointUpdated { emitter: String, sequence: u64 },
    BufferOverflow { dropped_count: usize },
}

pub trait DomainEventHandler: Send + Sync {
    fn handle(&self, event: DomainEvent);
}
```

Add event bus to Hub:
```rust
impl HubBuilder {
    pub fn on_event(self, handler: impl DomainEventHandler) -> Self { ... }
}
```

**Files to change:**
- New: `src/events.rs`
- `src/hub.rs` → add event handler registration
- `src/runner.rs` → emit events at key points
- `src/checkpoint.rs` → emit CheckpointUpdated

**Acceptance Criteria:**
- [ ] DomainEvent enum defined
- [ ] Event handler trait defined
- [ ] Hub builder accepts event handlers
- [ ] Runner emits events at: ingest, route, deliver, checkpoint
- [ ] Example handler for logging/metrics
- [ ] All tests pass

---

### HIGH PRIORITY

#### Issue #5: Persist Deadletters
**Priority:** P1
**Effort:** Medium
**Missing Resilience**

**Problem:**
`FailureBuffer` is in-memory only. Process crash loses all failed events.

**Solution:**
```rust
pub trait DeadletterStore: Send + Sync {
    fn store(&self, msg: &Message, error: &str) -> Result<()>;
    fn replay(&self, handler: impl FnMut(Message)) -> Result<usize>;
    fn count(&self) -> usize;
    fn clear(&self) -> Result<()>;
}
```

Implementations:
- `MemoryDeadletterStore` (current behavior)
- `FileDeadletterStore` (persist to disk)

**Files to change:**
- New: `src/deadletter.rs`
- `src/resilience/failure_buffer.rs` → use trait
- `src/hub.rs` → add deadletter store config

**Acceptance Criteria:**
- [ ] DeadletterStore trait defined
- [ ] File-based implementation
- [ ] Replay endpoint/method
- [ ] Integration with FailureBuffer
- [ ] Tests for crash recovery

---

#### Issue #6: Separate Stateful/Stateless Middleware
**Priority:** P2
**Effort:** Small
**Type Safety**

**Problem:**
```rust
pub trait Middleware {
    fn flush(&self) -> Option<Message> { None }  // Optional, no enforcement
}
```

Aggregator overrides `flush()`, but no type system enforcement. HubRunner calls flush on all middleware unnecessarily.

**Solution:**
```rust
pub trait Middleware: Send + Sync {
    async fn process(&self, msg: Message) -> Option<Message>;
}

pub trait StatefulMiddleware: Middleware {
    fn flush(&self) -> Option<Message>;
}
```

Or use marker:
```rust
pub trait Middleware: Send + Sync {
    async fn process(&self, msg: Message) -> Option<Message>;
    fn is_stateful(&self) -> bool { false }
    fn flush(&self) -> Option<Message> { None }
}
```

**Files to change:**
- `src/middleware/mod.rs` → update trait
- `src/middleware/aggregator.rs` → implement StatefulMiddleware
- `src/runner.rs` → only flush stateful middleware

**Acceptance Criteria:**
- [ ] Clear distinction between stateful/stateless
- [ ] Runner only flushes when needed
- [ ] No behavior change for existing middleware
- [ ] All tests pass

---

#### Issue #7: Typed Event Data Validation
**Priority:** P2
**Effort:** Medium
**Underutilized Feature**

**Problem:**
Proto defines typed event data:
```protobuf
oneof data {
    NetworkEventData network = 20;
    ContainerEventData container = 21;
    K8sEventData k8s = 22;
}
```

But code ignores it. No type-safe access, no validation.

**Solution:**
Add typed accessors to Message:
```rust
impl Message {
    pub fn as_network_event(&self) -> Option<&NetworkEventData> { ... }
    pub fn as_container_event(&self) -> Option<&ContainerEventData> { ... }
    pub fn as_k8s_event(&self) -> Option<&K8sEventData> { ... }
}
```

Add validation middleware:
```rust
pub struct TypedEventValidator {
    required_fields: HashSet<String>,
}
```

**Files to change:**
- `src/message.rs` → add typed accessors
- `src/middleware/validator.rs` → add schema validation
- Tests for typed events

**Acceptance Criteria:**
- [ ] Type-safe getters for event data
- [ ] Validator can enforce required fields per event type
- [ ] Documentation on how to use typed events
- [ ] Tests

---

### MEDIUM PRIORITY

#### Issue #8: Decouple Checkpoint from Emitter Names
**Priority:** P2
**Effort:** Small
**Brittle Coupling**

**Problem:**
```rust
checkpoint_store.set(emitter.name(), seq);  // Keyed by emitter name
```

If two emitters have same name, checkpoint overwrites. Checkpoint is about delivery position, not emitter identity.

**Solution:**
Key by destination identifier, not plugin name:
```rust
pub trait Emitter {
    fn name(&self) -> &str;
    fn destination_id(&self) -> &str;  // New: unique delivery target
}
```

Or use composite key:
```rust
checkpoint_store.set(&format!("{}:{}", emitter.name(), emitter.destination()), seq);
```

**Files to change:**
- `polku-core/src/emitter.rs` → add destination_id
- `src/runner.rs` → use destination_id for checkpoint
- Existing emitters → implement destination_id

**Acceptance Criteria:**
- [ ] Checkpoint key is unique per delivery target
- [ ] Two emitters with same name but different destinations work
- [ ] Migration story for existing checkpoints
- [ ] Tests

---

#### Issue #9: Add Schema Versioning
**Priority:** P3
**Effort:** Medium
**Missing Feature**

**Problem:**
No version field in Message/Event. Schema evolution is implicit. No migration path when event format changes.

**Solution:**
Add version to Message:
```rust
pub struct Message {
    pub schema_version: u32,  // Default: 1
    // ...
}
```

Add version negotiation:
```rust
pub trait Ingestor {
    fn supported_versions(&self) -> &[u32] { &[1] }
    fn ingest(&self, ctx: IngestContext, data: &[u8], version: u32) -> Result<Vec<Event>>;
}
```

**Files to change:**
- `src/message.rs` → add version field
- `polku-core/src/ingestor.rs` → add version support
- Proto → add version field
- Documentation on versioning strategy

**Acceptance Criteria:**
- [ ] Version field in Message
- [ ] Ingestors can declare supported versions
- [ ] Documentation on schema evolution
- [ ] Tests for version mismatch handling

---

#### Issue #10: Priority-Based Load Shedding
**Priority:** P3
**Effort:** Medium
**Missing Feature**

**Problem:**
Current backpressure is binary: buffer full → drop all new messages. No priority awareness.

**Solution:**
Add priority to Message:
```rust
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

pub struct Message {
    pub priority: Priority,
    // ...
}
```

Update buffer to shed by priority:
```rust
impl TieredBuffer {
    fn push(&self, msg: Message) -> Result<()> {
        if self.is_full() {
            self.drop_lowest_priority();
        }
        // ...
    }
}
```

**Files to change:**
- `src/message.rs` → add priority
- `src/buffer/tiered.rs` → priority-aware shedding
- SDKs → add priority to Event proto

**Acceptance Criteria:**
- [ ] Priority enum defined
- [ ] Messages have default priority (Normal)
- [ ] Buffer drops low priority first when full
- [ ] Metrics on dropped messages by priority
- [ ] Tests

---

### LOW PRIORITY

#### Issue #11: OpenTelemetry Integration
**Priority:** P4
**Effort:** Large

Add automatic trace propagation via metadata and emit spans for middleware stages.

---

#### Issue #12: Time-Based Aggregation Windows
**Priority:** P4
**Effort:** Medium

Extend Aggregator middleware to support time windows, not just count-based.

---

## Implementation Plan

### Phase 1: Foundation (Week 1-2)
Focus: Fix aggregate boundaries and model clarity

| Issue | Task | Est |
|-------|------|-----|
| #1 | Split PluginRegistry | 3d |
| #2 | Encapsulate route_to | 1d |
| #3 | Unify Message/Event | 4d |

**Milestone:** Clean bounded contexts, single source of truth for domain model

### Phase 2: Observability (Week 3-4)
Focus: Add visibility into system behavior

| Issue | Task | Est |
|-------|------|-----|
| #4 | Add Domain Events | 4d |
| #5 | Persist Deadletters | 3d |

**Milestone:** Can audit what happened, recover from failures

### Phase 3: Type Safety (Week 5)
Focus: Leverage type system for correctness

| Issue | Task | Est |
|-------|------|-----|
| #6 | Stateful/Stateless Middleware | 2d |
| #7 | Typed Event Validation | 3d |

**Milestone:** Compile-time guarantees, validated events

### Phase 4: Resilience (Week 6)
Focus: Production hardening

| Issue | Task | Est |
|-------|------|-----|
| #8 | Decouple Checkpoint Keys | 2d |
| #9 | Schema Versioning | 3d |
| #10 | Priority Load Shedding | 3d |

**Milestone:** Safe schema evolution, graceful degradation

### Phase 5: Nice-to-Have (Future)
| Issue | Task |
|-------|------|
| #11 | OpenTelemetry |
| #12 | Time Windows |

---

## Summary

| Priority | Count | Effort |
|----------|-------|--------|
| P0 (Critical) | 3 | ~8 days |
| P1 (High) | 2 | ~7 days |
| P2 (Medium) | 3 | ~7 days |
| P3 (Low) | 2 | ~6 days |
| P4 (Future) | 2 | TBD |

**Total estimated effort:** ~4-6 weeks for P0-P3

---

## Notes

- Each issue should become a GitHub issue
- Issues can be worked in parallel where no dependencies exist
- #1, #2, #3 should be done first (foundation)
- #4 enables better debugging of all other changes
- Run full test suite after each issue completion
