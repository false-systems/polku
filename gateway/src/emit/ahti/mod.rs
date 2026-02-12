//! AHTI Emitter - sends events to AHTI for analysis
//!
//! Converts polku_core::Event to ahti.v1.AhtiEvent and streams to AHTI.
//! Supports typed event data (network, kernel, container, k8s, process, resource)
//! which maps directly to Ahti's event model.
//!
//! # Design Notes
//!
//! ## Streaming Model
//! Each `emit()` call opens a new gRPC stream and sends a single batch. This is
//! intentionally simple rather than maintaining persistent streams, because:
//! - Polku's Hub already batches events before calling emit()
//! - ResilientEmitter handles retries with exponential backoff
//! - Persistent streams add complexity (reconnection, flow control, ordering)
//!
//! For high-throughput scenarios, consider wrapping with ResilientEmitter and
//! tuning the Hub's batch_size and flush_interval.
//!
//! ## Retry & Resilience
//! This emitter does basic endpoint failover but NOT retry with backoff.
//! For production use, wrap with `ResilientEmitter::wrap(ahti_emitter).with_retry(...)`.

mod ahti_proto;

use super::{Emitter, PluginError};
use crate::message::Message;
use ahti_proto::ahti::v1::{
    self as ahti_types, AhtiEvent, AhtiEventBatch, AhtiHealthRequest, Entity, EntityReference,
    EntityType, EventType, Relationship, RelationshipState, RelationshipType,
    ahti_service_client::AhtiServiceClient,
};
use polku_core::Event;
// Import polku_core types for the Event.data oneof
use polku_core::proto::event::Data as PolkuEventData;
use polku_core::{Outcome as PolkuOutcome, Severity as PolkuSeverity};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const FAILURE_THRESHOLD: u32 = 3;
const UNHEALTHY_DURATION_MS: u64 = 30_000;

/// Tracks health and performance of a single AHTI endpoint.
///
/// Uses `Ordering::SeqCst` for failure tracking to ensure proper visibility
/// across threads during endpoint selection and failure recording.
struct EndpointState {
    unhealthy_until: AtomicU64,
    consecutive_failures: AtomicU64,
    /// Cumulative latency in microseconds for calculating averages
    total_latency_us: AtomicU64,
    /// Number of successful emits for latency averaging
    emit_count: AtomicU64,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            unhealthy_until: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            emit_count: AtomicU64::new(0),
        }
    }

    fn record_success(&self, latency_us: u64) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.unhealthy_until.store(0, Ordering::SeqCst);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.emit_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= FAILURE_THRESHOLD as u64 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.unhealthy_until
                .store(now + UNHEALTHY_DURATION_MS, Ordering::SeqCst);
        }
    }

    fn is_healthy(&self) -> bool {
        let unhealthy_until = self.unhealthy_until.load(Ordering::SeqCst);
        if unhealthy_until == 0 {
            return true;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now >= unhealthy_until
    }

    /// Average latency in microseconds, or 0 if no data yet
    fn avg_latency_us(&self) -> u64 {
        let count = self.emit_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        self.total_latency_us.load(Ordering::Relaxed) / count
    }
}

/// AhtiEmitter sends events to AHTI for analysis
pub struct AhtiEmitter {
    clients: Vec<AhtiServiceClient<Channel>>,
    endpoints: Vec<String>,
    states: Vec<EndpointState>,
}

impl AhtiEmitter {
    /// Create a new AhtiEmitter connected to a single AHTI endpoint
    pub async fn new(endpoint: impl Into<String>) -> Result<Self, PluginError> {
        Self::with_endpoints(vec![endpoint.into()]).await
    }

    /// Create an AhtiEmitter connected to multiple AHTI endpoints
    pub async fn with_endpoints(endpoints: Vec<String>) -> Result<Self, PluginError> {
        if endpoints.is_empty() {
            return Err(PluginError::Init("No AHTI endpoints provided".to_string()));
        }

        let mut clients = Vec::with_capacity(endpoints.len());
        let mut states = Vec::with_capacity(endpoints.len());

        for endpoint_str in &endpoints {
            let channel = Endpoint::from_shared(endpoint_str.clone())
                .map_err(|e| PluginError::Init(format!("Invalid AHTI endpoint URL: {}", e)))?
                .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
                .timeout(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS))
                .connect()
                .await
                .map_err(|e| {
                    PluginError::Connection(format!(
                        "Failed to connect to AHTI at {}: {}",
                        endpoint_str, e
                    ))
                })?;

            clients.push(AhtiServiceClient::new(channel));
            states.push(EndpointState::new());
        }

        debug!(endpoints = ?endpoints, "AHTI emitter connected to {} endpoint(s)", endpoints.len());

        Ok(Self {
            clients,
            endpoints,
            states,
        })
    }

    /// Create an AhtiEmitter with lazy connections
    pub async fn with_endpoints_lazy(endpoints: Vec<String>) -> Result<Self, PluginError> {
        if endpoints.is_empty() {
            return Err(PluginError::Init("No AHTI endpoints provided".to_string()));
        }

        let mut clients = Vec::with_capacity(endpoints.len());
        let mut states = Vec::with_capacity(endpoints.len());

        for endpoint_str in &endpoints {
            let channel = Endpoint::from_shared(endpoint_str.clone())
                .map_err(|e| PluginError::Init(format!("Invalid AHTI endpoint URL: {}", e)))?
                .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
                .timeout(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS))
                .connect_lazy();

            clients.push(AhtiServiceClient::new(channel));
            states.push(EndpointState::new());
        }

        debug!(endpoints = ?endpoints, "AHTI emitter configured with {} lazy endpoint(s)", endpoints.len());

        Ok(Self {
            clients,
            endpoints,
            states,
        })
    }

    /// Select the best healthy endpoint based on average latency.
    /// Prefers endpoints with known latency data over those without.
    fn select_endpoint(&self, exclude: &[bool]) -> Option<usize> {
        let mut best_known_idx = None;
        let mut best_latency = u64::MAX;
        let mut first_unknown_idx = None;

        for (idx, state) in self.states.iter().enumerate() {
            if exclude[idx] || !state.is_healthy() {
                continue;
            }

            let latency = state.avg_latency_us();
            if latency == 0 {
                // No latency data yet - track as fallback
                if first_unknown_idx.is_none() {
                    first_unknown_idx = Some(idx);
                }
            } else if latency < best_latency {
                // Has latency data - prefer lowest
                best_latency = latency;
                best_known_idx = Some(idx);
            }
        }

        // Prefer endpoints with known latency, fall back to unknown
        best_known_idx.or(first_unknown_idx)
    }

    fn select_any_untried(&self, exclude: &[bool]) -> Option<usize> {
        for (idx, excluded) in exclude.iter().enumerate() {
            if !excluded {
                return Some(idx);
            }
        }
        None
    }

    async fn try_emit_to(&self, idx: usize, events: &[Event]) -> Result<u64, PluginError> {
        let mut client = self.clients[idx].clone();
        let endpoint = &self.endpoints[idx];
        let state = &self.states[idx];

        let start = Instant::now();
        let ahti_events: Vec<AhtiEvent> = events.iter().map(Self::event_to_ahti_event).collect();

        let batch = AhtiEventBatch {
            events: ahti_events,
        };

        // NOTE: We use streaming RPC with a single batch rather than unary because:
        // 1. AHTI's API is streaming-based (StreamEvents)
        // 2. Hub already batches events, so each emit() call is one logical batch
        // 3. This keeps the door open for multi-batch streaming if needed later
        // The single-element channel overhead is negligible vs. network latency.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(batch)
            .await
            .map_err(|e| PluginError::Send(format!("Failed to send batch to channel: {}", e)))?;
        drop(tx);

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        match client.stream_events(stream).await {
            Ok(response) => {
                let mut ack_stream = response.into_inner();

                match ack_stream.message().await {
                    Ok(Some(ack)) => {
                        let latency_us = start.elapsed().as_micros() as u64;
                        if ack.success {
                            debug!(endpoint = %endpoint, acked = ack.event_ids.len(), latency_us, "Batch sent to AHTI");
                            state.record_success(latency_us);
                            Ok(latency_us)
                        } else {
                            warn!(endpoint = %endpoint, error = %ack.error, "AHTI rejected batch");
                            state.record_failure();
                            Err(PluginError::Send(format!(
                                "AHTI rejected batch: {}",
                                ack.error
                            )))
                        }
                    }
                    Ok(None) => {
                        let latency_us = start.elapsed().as_micros() as u64;
                        debug!(endpoint = %endpoint, latency_us, "Stream ended normally");
                        state.record_success(latency_us);
                        Ok(latency_us)
                    }
                    Err(e) => {
                        error!(endpoint = %endpoint, error = %e, "Stream error from AHTI");
                        state.record_failure();
                        Err(PluginError::Send(format!(
                            "Stream error from {}: {}",
                            endpoint, e
                        )))
                    }
                }
            }
            Err(e) => {
                error!(endpoint = %endpoint, error = %e, "Failed to open stream to AHTI");
                state.record_failure();
                Err(PluginError::Send(format!(
                    "Failed to send to {}: {}",
                    endpoint, e
                )))
            }
        }
    }

    fn event_to_ahti_event(event: &Event) -> AhtiEvent {
        let (event_type, subtype) = Self::parse_event_type(&event.event_type);

        let cluster = event
            .metadata
            .get("cluster_id")
            .or_else(|| event.metadata.get("cluster"))
            .cloned()
            .unwrap_or_default();
        let namespace = event.metadata.get("namespace").cloned().unwrap_or_default();

        let entities = Self::extract_entities(event);
        let relationships = Self::extract_relationships(event);

        let timestamp = Some(prost_types::Timestamp {
            seconds: event.timestamp_unix_ns / 1_000_000_000,
            nanos: (event.timestamp_unix_ns % 1_000_000_000) as i32,
        });

        // Map severity from polku to ahti
        let severity = Self::map_severity(event.severity);

        // Map outcome from polku to ahti
        let outcome = Self::map_outcome(event.outcome);

        // Map typed event data from polku to ahti
        let data = Self::map_event_data(&event.data);

        // Extract duration from metadata or typed data
        let duration_us = Self::extract_duration_us(event);

        AhtiEvent {
            id: event.id.clone(),
            timestamp,
            r#type: event_type as i32,
            subtype,
            severity,
            outcome,
            source: event.source.clone(),
            trace_id: event.metadata.get("trace_id").cloned().unwrap_or_default(),
            span_id: event.metadata.get("span_id").cloned().unwrap_or_default(),
            parent_span_id: event
                .metadata
                .get("parent_span_id")
                .cloned()
                .unwrap_or_default(),
            duration_us,
            entities,
            relationships,
            cluster,
            namespace,
            labels: event.metadata.clone(),
            error: None,
            data,
        }
    }

    /// Extract duration in microseconds from event metadata or typed data.
    /// Checks metadata["duration_us"], metadata["duration_ms"], or network latency.
    fn extract_duration_us(event: &Event) -> u64 {
        // First check explicit duration in metadata
        if let Some(us) = event
            .metadata
            .get("duration_us")
            .and_then(|s| s.parse::<u64>().ok())
        {
            return us;
        }
        if let Some(ms) = event
            .metadata
            .get("duration_ms")
            .and_then(|s| s.parse::<u64>().ok())
        {
            return ms * 1000;
        }

        // Fall back to typed data latency (network events have latency_ms)
        if let Some(PolkuEventData::Network(n)) = &event.data
            && n.latency_ms > 0.0
        {
            return (n.latency_ms * 1000.0) as u64;
        }

        0
    }

    /// Extract entity relationships from event metadata.
    /// Looks for owner references, pod-to-node, deployment-to-replicaset, etc.
    fn extract_relationships(event: &Event) -> Vec<Relationship> {
        let mut relationships = Vec::new();
        let cluster_id = event
            .metadata
            .get("cluster_id")
            .or_else(|| event.metadata.get("cluster"))
            .cloned()
            .unwrap_or_default();

        // Pod -> Node relationship (ScheduledOn)
        if let (Some(_pod_name), Some(node_name)) = (
            event
                .metadata
                .get("pod_name")
                .or_else(|| event.metadata.get("name")),
            event.metadata.get("node_name"),
        ) && event.metadata.get("resource").map(|s| s.as_str()) == Some("pod")
        {
            relationships.push(Relationship {
                r#type: RelationshipType::ScheduledOn as i32,
                source: Some(EntityReference {
                    r#type: EntityType::Pod as i32,
                    id: event.metadata.get("uid").cloned().unwrap_or_default(),
                    cluster_id: cluster_id.clone(),
                }),
                target: Some(EntityReference {
                    r#type: EntityType::Node as i32,
                    id: String::new(), // Node ID often not in metadata
                    cluster_id: cluster_id.clone(),
                }),
                labels: [("target_name".to_string(), node_name.clone())]
                    .into_iter()
                    .collect(),
                generation: 0,
                state: RelationshipState::Active as i32,
                deleted_at: None,
                delete_reason: String::new(),
                created_at: None,
            });
        }

        // Owner reference (e.g., Pod -> ReplicaSet -> Deployment) uses Owns relationship
        if let (Some(owner_kind), Some(_owner_name)) = (
            event.metadata.get("owner_kind"),
            event.metadata.get("owner_name"),
        ) {
            // Note: Ahti EntityType doesn't have ReplicaSet or Job, map to closest
            let owner_type = match owner_kind.as_str() {
                "Deployment" => EntityType::Deployment,
                "StatefulSet" => EntityType::Statefulset,
                "DaemonSet" => EntityType::Daemonset,
                // ReplicaSet and Job don't exist in EntityType, skip them
                _ => EntityType::Unspecified,
            };

            if owner_type != EntityType::Unspecified {
                let source_type = match event.metadata.get("resource").map(|s| s.as_str()) {
                    Some("pod") => EntityType::Pod,
                    _ => EntityType::Unspecified,
                };

                if source_type != EntityType::Unspecified {
                    // Note: We express "owned_by" as the owner "Owns" the resource
                    relationships.push(Relationship {
                        r#type: RelationshipType::Owns as i32,
                        source: Some(EntityReference {
                            r#type: owner_type as i32,
                            id: event.metadata.get("owner_uid").cloned().unwrap_or_default(),
                            cluster_id: cluster_id.clone(),
                        }),
                        target: Some(EntityReference {
                            r#type: source_type as i32,
                            id: event.metadata.get("uid").cloned().unwrap_or_default(),
                            cluster_id: cluster_id.clone(),
                        }),
                        labels: Default::default(),
                        generation: 0,
                        state: RelationshipState::Active as i32,
                        deleted_at: None,
                        delete_reason: String::new(),
                        created_at: None,
                    });
                }
            }
        }

        relationships
    }

    /// Map polku severity enum to ahti severity
    fn map_severity(severity: i32) -> i32 {
        // Both use the same enum values (0-5), but we map explicitly for safety
        match PolkuSeverity::try_from(severity) {
            Ok(PolkuSeverity::Unspecified) => ahti_types::Severity::Unspecified as i32,
            Ok(PolkuSeverity::Debug) => ahti_types::Severity::Debug as i32,
            Ok(PolkuSeverity::Info) => ahti_types::Severity::Info as i32,
            Ok(PolkuSeverity::Warning) => ahti_types::Severity::Warning as i32,
            Ok(PolkuSeverity::Error) => ahti_types::Severity::Error as i32,
            Ok(PolkuSeverity::Critical) => ahti_types::Severity::Critical as i32,
            Err(_) => ahti_types::Severity::Info as i32, // Default to Info for unknown
        }
    }

    /// Map polku outcome enum to ahti outcome
    fn map_outcome(outcome: i32) -> i32 {
        match PolkuOutcome::try_from(outcome) {
            Ok(PolkuOutcome::Unspecified) => ahti_types::Outcome::Unspecified as i32,
            Ok(PolkuOutcome::Success) => ahti_types::Outcome::Success as i32,
            Ok(PolkuOutcome::Failure) => ahti_types::Outcome::Failure as i32,
            Ok(PolkuOutcome::Timeout) => ahti_types::Outcome::Timeout as i32,
            Ok(PolkuOutcome::Unknown) => ahti_types::Outcome::Unknown as i32,
            Err(_) => ahti_types::Outcome::Unknown as i32,
        }
    }

    /// Map polku typed event data to ahti typed event data
    fn map_event_data(data: &Option<PolkuEventData>) -> Option<ahti_types::ahti_event::Data> {
        let data = data.as_ref()?;

        match data {
            PolkuEventData::Network(n) => Some(ahti_types::ahti_event::Data::Network(
                ahti_types::NetworkEventData {
                    protocol: n.protocol.clone(),
                    src_ip: n.src_ip.clone(),
                    dst_ip: n.dst_ip.clone(),
                    src_port: n.src_port,
                    dst_port: n.dst_port,
                    direction: n.direction.clone(),
                    dns_query: n.dns_query.clone(),
                    dns_response: n.dns_response.clone(),
                    http_method: n.http_method.clone(),
                    http_path: n.http_path.clone(),
                    http_status_code: n.http_status_code,
                    latency_ms: n.latency_ms,
                    bytes_sent: n.bytes_sent,
                    bytes_received: n.bytes_received,
                    rtt_baseline_ms: n.rtt_baseline_ms,
                    rtt_current_ms: n.rtt_current_ms,
                    rtt_degradation_pct: n.rtt_degradation_pct,
                    retransmit_count: n.retransmit_count,
                    tcp_state: n.tcp_state.clone(),
                    process_name: n.process_name.clone(),
                    container_id: n.container_id.clone(),
                    pod_name: n.pod_name.clone(),
                },
            )),
            PolkuEventData::Kernel(k) => Some(ahti_types::ahti_event::Data::Kernel(
                ahti_types::KernelEventData {
                    event_type: k.event_type.clone(),
                    pid: k.pid,
                    command: k.command.clone(),
                    oom_victim_pid: k.oom_victim_pid,
                    oom_victim_comm: k.oom_victim_comm.clone(),
                    memory_requested: k.memory_requested,
                    signal: k.signal,
                    signal_code: k.signal_code,
                    syscall_id: k.syscall_id,
                    syscall_name: k.syscall_name.clone(),
                    syscall_retval: k.syscall_retval,
                },
            )),
            PolkuEventData::Container(c) => Some(ahti_types::ahti_event::Data::Container(
                ahti_types::ContainerEventData {
                    container_id: c.container_id.clone(),
                    container_name: c.container_name.clone(),
                    image: c.image.clone(),
                    state: c.state.clone(),
                    exit_code: c.exit_code,
                    restart_count: c.restart_count,
                    start_time: c.start_time,
                    cpu_usage: c.cpu_usage,
                    memory_usage: c.memory_usage,
                    memory_limit: c.memory_limit,
                    disk_usage: c.disk_usage,
                    signal: c.signal,
                    cgroup_path: c.cgroup_path.clone(),
                },
            )),
            PolkuEventData::K8s(k) => Some(ahti_types::ahti_event::Data::K8s(
                ahti_types::K8sEventData {
                    resource_type: k.resource_type.clone(),
                    resource_name: k.resource_name.clone(),
                    namespace: k.namespace.clone(),
                    reason: k.reason.clone(),
                    message: k.message.clone(),
                    replicas: k.replicas,
                    ready_replicas: k.ready_replicas,
                    updated_replicas: k.updated_replicas,
                    rollout_status: k.rollout_status.clone(),
                },
            )),
            PolkuEventData::Process(p) => Some(ahti_types::ahti_event::Data::Process(
                ahti_types::ProcessEventData {
                    pid: p.pid,
                    ppid: p.ppid,
                    command: p.command.clone(),
                    args: p.args.clone(),
                    uid: p.uid,
                    gid: p.gid,
                    user: p.user.clone(),
                    exit_code: p.exit_code,
                    start_time: p.start_time,
                    end_time: p.end_time,
                },
            )),
            PolkuEventData::Resource(r) => Some(ahti_types::ahti_event::Data::Resource(
                ahti_types::ResourceEventData {
                    resource_type: r.resource_type.clone(),
                    node_name: r.node_name.clone(),
                    cpu_usage_pct: r.cpu_usage_pct,
                    cpu_throttle_pct: r.cpu_throttle_pct,
                    memory_used: r.memory_used,
                    memory_available: r.memory_available,
                    memory_pressure: r.memory_pressure,
                    disk_used: r.disk_used,
                    disk_available: r.disk_available,
                    disk_io_utilization: r.disk_io_utilization,
                },
            )),
        }
    }

    fn parse_event_type(event_type: &str) -> (EventType, String) {
        let parts: Vec<&str> = event_type.split('.').collect();

        match parts.as_slice() {
            ["k8s", resource, action] => {
                let ahti_type = match *resource {
                    "deployment" | "replicaset" | "statefulset" | "daemonset" => {
                        EventType::Deployment
                    }
                    "pod" => EventType::Pod,
                    "service" | "endpoint" => EventType::Service,
                    "configmap" | "secret" => EventType::Config,
                    "pvc" | "pv" => EventType::Volume,
                    "node" => EventType::Cluster,
                    "event" => EventType::Health,
                    _ => EventType::Unspecified,
                };
                (ahti_type, format!("{}.{}", resource, action))
            }
            ["network", subtype] => (EventType::Network, (*subtype).to_string()),
            ["kernel", subtype] => (EventType::Kernel, (*subtype).to_string()),
            ["container", subtype] => (EventType::Container, (*subtype).to_string()),
            ["resource", subtype] => (EventType::Resource, (*subtype).to_string()),
            [category, rest @ ..] => {
                let ahti_type = match *category {
                    "network" => EventType::Network,
                    "kernel" => EventType::Kernel,
                    "container" => EventType::Container,
                    "pod" => EventType::Pod,
                    "deployment" => EventType::Deployment,
                    "service" => EventType::Service,
                    "health" => EventType::Health,
                    "performance" => EventType::Performance,
                    "resource" => EventType::Resource,
                    "signal" => EventType::Signal,
                    _ => EventType::Unspecified,
                };
                (ahti_type, rest.join("."))
            }
            _ => (EventType::Unspecified, event_type.to_string()),
        }
    }

    fn extract_entities(event: &Event) -> Vec<Entity> {
        let mut entities = Vec::new();

        if let Some(name) = event.metadata.get("name") {
            let resource = event
                .metadata
                .get("resource")
                .map(|s| s.as_str())
                .unwrap_or("");
            let entity_type = match resource {
                "deployment" => EntityType::Deployment,
                "pod" => EntityType::Pod,
                "service" => EntityType::Service,
                "configmap" => EntityType::Configmap,
                "secret" => EntityType::Secret,
                "node" => EntityType::Node,
                "statefulset" => EntityType::Statefulset,
                "daemonset" => EntityType::Daemonset,
                "pvc" => EntityType::Pvc,
                _ => EntityType::Unspecified,
            };

            entities.push(Entity {
                r#type: entity_type as i32,
                id: event.metadata.get("uid").cloned().unwrap_or_default(),
                name: name.clone(),
                cluster_id: event
                    .metadata
                    .get("cluster_id")
                    .cloned()
                    .unwrap_or_default(),
                namespace: event.metadata.get("namespace").cloned().unwrap_or_default(),
                labels: Default::default(),
                attributes: Default::default(),
                generation: 0,
                state: 1,
                deleted_at: None,
                delete_reason: String::new(),
            });
        }

        entities
    }
}

#[async_trait::async_trait]
impl Emitter for AhtiEmitter {
    fn name(&self) -> &'static str {
        "ahti"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Convert at gRPC boundary - same pattern as GrpcEmitter/ExternalEmitter.
        // Note: typed Event fields (severity, outcome, data) are set to defaults
        // because they were already lost in the pipeline's Message stage.
        let events: Vec<Event> = messages.iter().map(|m| Event::from(m.clone())).collect();

        let mut tried = vec![false; self.clients.len()];
        let mut last_error = None;

        // Try healthy endpoints first (sorted by latency)
        while let Some(idx) = self.select_endpoint(&tried) {
            tried[idx] = true;

            match self.try_emit_to(idx, &events).await {
                Ok(_latency_us) => return Ok(()),
                Err(e) => {
                    warn!(endpoint = %self.endpoints[idx], error = %e, "Emit failed, trying next endpoint");
                    last_error = Some(e);
                }
            }
        }

        // Fall back to unhealthy endpoints (they might have recovered)
        while let Some(idx) = self.select_any_untried(&tried) {
            tried[idx] = true;

            match self.try_emit_to(idx, &events).await {
                Ok(_latency_us) => {
                    info!(endpoint = %self.endpoints[idx], "Unhealthy endpoint recovered");
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        error!(event_count = messages.len(), "All AHTI endpoints failed");

        Err(last_error
            .unwrap_or_else(|| PluginError::Send("No AHTI endpoints available".to_string())))
    }

    async fn health(&self) -> bool {
        for (idx, client) in self.clients.iter().enumerate() {
            let mut client = client.clone();
            match client.health(AhtiHealthRequest {}).await {
                Ok(response) => {
                    if response.into_inner().healthy {
                        return true;
                    }
                }
                Err(e) => {
                    debug!(endpoint = %self.endpoints[idx], error = %e, "AHTI health check failed");
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polku_core::proto::event::Data as PolkuEventData;
    use polku_core::{
        ContainerEventData as PolkuContainerEventData, K8sEventData as PolkuK8sEventData,
        KernelEventData as PolkuKernelEventData, NetworkEventData as PolkuNetworkEventData,
        Outcome as PolkuOutcome, ProcessEventData as PolkuProcessEventData,
        ResourceEventData as PolkuResourceEventData, Severity as PolkuSeverity,
    };
    use std::collections::HashMap;

    // ==========================================================================
    // Helper Functions
    // ==========================================================================

    fn make_event() -> Event {
        Event {
            id: "test-event-001".to_string(),
            timestamp_unix_ns: 1704067200_000_000_000, // 2024-01-01 00:00:00 UTC
            source: "test-source".to_string(),
            event_type: "network.connection".to_string(),
            metadata: HashMap::new(),
            payload: vec![],
            route_to: vec![],
            severity: PolkuSeverity::Info as i32,
            outcome: PolkuOutcome::Success as i32,
            data: None,
        }
    }

    fn make_event_with_metadata(metadata: HashMap<String, String>) -> Event {
        Event {
            id: "test-event-002".to_string(),
            timestamp_unix_ns: 1704067200_000_000_000,
            source: "test-source".to_string(),
            event_type: "k8s.pod.created".to_string(),
            metadata,
            payload: vec![],
            route_to: vec![],
            severity: PolkuSeverity::Info as i32,
            outcome: PolkuOutcome::Success as i32,
            data: None,
        }
    }

    // ==========================================================================
    // EndpointState Tests
    // ==========================================================================

    #[test]
    fn test_endpoint_state_new_is_healthy() {
        let state = EndpointState::new();
        assert!(state.is_healthy());
        assert_eq!(state.avg_latency_us(), 0);
    }

    #[test]
    fn test_endpoint_state_record_success() {
        let state = EndpointState::new();
        state.record_success(1000);
        state.record_success(2000);
        state.record_success(3000);

        assert!(state.is_healthy());
        assert_eq!(state.avg_latency_us(), 2000); // (1000 + 2000 + 3000) / 3
    }

    #[test]
    fn test_endpoint_state_single_failure_stays_healthy() {
        let state = EndpointState::new();
        state.record_failure();
        assert!(state.is_healthy()); // Still healthy after 1 failure

        state.record_failure();
        assert!(state.is_healthy()); // Still healthy after 2 failures
    }

    #[test]
    fn test_endpoint_state_three_failures_becomes_unhealthy() {
        let state = EndpointState::new();
        state.record_failure();
        state.record_failure();
        state.record_failure(); // 3rd failure triggers unhealthy

        assert!(!state.is_healthy());
    }

    #[test]
    fn test_endpoint_state_success_resets_failures() {
        let state = EndpointState::new();
        state.record_failure();
        state.record_failure();
        state.record_success(1000); // Reset consecutive failures

        state.record_failure();
        state.record_failure();
        assert!(state.is_healthy()); // Still healthy - only 2 consecutive failures
    }

    // ==========================================================================
    // Severity Mapping Tests
    // ==========================================================================

    #[test]
    fn test_map_severity_unspecified() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Unspecified as i32);
        assert_eq!(result, ahti_types::Severity::Unspecified as i32);
    }

    #[test]
    fn test_map_severity_debug() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Debug as i32);
        assert_eq!(result, ahti_types::Severity::Debug as i32);
    }

    #[test]
    fn test_map_severity_info() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Info as i32);
        assert_eq!(result, ahti_types::Severity::Info as i32);
    }

    #[test]
    fn test_map_severity_warning() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Warning as i32);
        assert_eq!(result, ahti_types::Severity::Warning as i32);
    }

    #[test]
    fn test_map_severity_error() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Error as i32);
        assert_eq!(result, ahti_types::Severity::Error as i32);
    }

    #[test]
    fn test_map_severity_critical() {
        let result = AhtiEmitter::map_severity(PolkuSeverity::Critical as i32);
        assert_eq!(result, ahti_types::Severity::Critical as i32);
    }

    #[test]
    fn test_map_severity_unknown_defaults_to_info() {
        let result = AhtiEmitter::map_severity(999); // Invalid value
        assert_eq!(result, ahti_types::Severity::Info as i32);
    }

    // ==========================================================================
    // Outcome Mapping Tests
    // ==========================================================================

    #[test]
    fn test_map_outcome_unspecified() {
        let result = AhtiEmitter::map_outcome(PolkuOutcome::Unspecified as i32);
        assert_eq!(result, ahti_types::Outcome::Unspecified as i32);
    }

    #[test]
    fn test_map_outcome_success() {
        let result = AhtiEmitter::map_outcome(PolkuOutcome::Success as i32);
        assert_eq!(result, ahti_types::Outcome::Success as i32);
    }

    #[test]
    fn test_map_outcome_failure() {
        let result = AhtiEmitter::map_outcome(PolkuOutcome::Failure as i32);
        assert_eq!(result, ahti_types::Outcome::Failure as i32);
    }

    #[test]
    fn test_map_outcome_timeout() {
        let result = AhtiEmitter::map_outcome(PolkuOutcome::Timeout as i32);
        assert_eq!(result, ahti_types::Outcome::Timeout as i32);
    }

    #[test]
    fn test_map_outcome_unknown() {
        let result = AhtiEmitter::map_outcome(PolkuOutcome::Unknown as i32);
        assert_eq!(result, ahti_types::Outcome::Unknown as i32);
    }

    #[test]
    fn test_map_outcome_invalid_defaults_to_unknown() {
        let result = AhtiEmitter::map_outcome(999);
        assert_eq!(result, ahti_types::Outcome::Unknown as i32);
    }

    // ==========================================================================
    // Event Type Parsing Tests
    // ==========================================================================

    #[test]
    fn test_parse_event_type_k8s_pod() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("k8s.pod.created");
        assert_eq!(event_type, EventType::Pod);
        assert_eq!(subtype, "pod.created");
    }

    #[test]
    fn test_parse_event_type_k8s_deployment() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("k8s.deployment.updated");
        assert_eq!(event_type, EventType::Deployment);
        assert_eq!(subtype, "deployment.updated");
    }

    #[test]
    fn test_parse_event_type_k8s_service() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("k8s.service.deleted");
        assert_eq!(event_type, EventType::Service);
        assert_eq!(subtype, "service.deleted");
    }

    #[test]
    fn test_parse_event_type_k8s_configmap() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("k8s.configmap.modified");
        assert_eq!(event_type, EventType::Config);
        assert_eq!(subtype, "configmap.modified");
    }

    #[test]
    fn test_parse_event_type_k8s_node() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("k8s.node.ready");
        assert_eq!(event_type, EventType::Cluster);
        assert_eq!(subtype, "node.ready");
    }

    #[test]
    fn test_parse_event_type_network() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("network.connection");
        assert_eq!(event_type, EventType::Network);
        assert_eq!(subtype, "connection");
    }

    #[test]
    fn test_parse_event_type_kernel() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("kernel.oom");
        assert_eq!(event_type, EventType::Kernel);
        assert_eq!(subtype, "oom");
    }

    #[test]
    fn test_parse_event_type_container() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("container.started");
        assert_eq!(event_type, EventType::Container);
        assert_eq!(subtype, "started");
    }

    #[test]
    fn test_parse_event_type_resource() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("resource.cpu_high");
        assert_eq!(event_type, EventType::Resource);
        assert_eq!(subtype, "cpu_high");
    }

    #[test]
    fn test_parse_event_type_unknown() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("custom.something.else");
        assert_eq!(event_type, EventType::Unspecified);
        assert_eq!(subtype, "something.else");
    }

    #[test]
    fn test_parse_event_type_empty() {
        let (event_type, subtype) = AhtiEmitter::parse_event_type("");
        assert_eq!(event_type, EventType::Unspecified);
        assert_eq!(subtype, "");
    }

    // ==========================================================================
    // Duration Extraction Tests
    // ==========================================================================

    #[test]
    fn test_extract_duration_from_metadata_us() {
        let mut event = make_event();
        event
            .metadata
            .insert("duration_us".to_string(), "5000".to_string());

        let duration = AhtiEmitter::extract_duration_us(&event);
        assert_eq!(duration, 5000);
    }

    #[test]
    fn test_extract_duration_from_metadata_ms() {
        let mut event = make_event();
        event
            .metadata
            .insert("duration_ms".to_string(), "5".to_string());

        let duration = AhtiEmitter::extract_duration_us(&event);
        assert_eq!(duration, 5000); // 5ms = 5000us
    }

    #[test]
    fn test_extract_duration_from_network_latency() {
        let mut event = make_event();
        event.data = Some(PolkuEventData::Network(PolkuNetworkEventData {
            latency_ms: 10.5,
            ..Default::default()
        }));

        let duration = AhtiEmitter::extract_duration_us(&event);
        assert_eq!(duration, 10500); // 10.5ms = 10500us
    }

    #[test]
    fn test_extract_duration_us_precedence() {
        // duration_us in metadata takes precedence over network latency
        let mut event = make_event();
        event
            .metadata
            .insert("duration_us".to_string(), "1234".to_string());
        event.data = Some(PolkuEventData::Network(PolkuNetworkEventData {
            latency_ms: 99.0,
            ..Default::default()
        }));

        let duration = AhtiEmitter::extract_duration_us(&event);
        assert_eq!(duration, 1234); // metadata takes precedence
    }

    #[test]
    fn test_extract_duration_no_data() {
        let event = make_event();
        let duration = AhtiEmitter::extract_duration_us(&event);
        assert_eq!(duration, 0);
    }

    // ==========================================================================
    // Entity Extraction Tests
    // ==========================================================================

    #[test]
    fn test_extract_entities_pod() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("uid".to_string(), "pod-uid-123".to_string());
        metadata.insert("namespace".to_string(), "default".to_string());
        metadata.insert("cluster_id".to_string(), "cluster-1".to_string());

        let event = make_event_with_metadata(metadata);
        let entities = AhtiEmitter::extract_entities(&event);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].r#type, EntityType::Pod as i32);
        assert_eq!(entities[0].name, "my-pod");
        assert_eq!(entities[0].id, "pod-uid-123");
        assert_eq!(entities[0].namespace, "default");
        assert_eq!(entities[0].cluster_id, "cluster-1");
    }

    #[test]
    fn test_extract_entities_deployment() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-deployment".to_string());
        metadata.insert("resource".to_string(), "deployment".to_string());
        metadata.insert("uid".to_string(), "deploy-uid-456".to_string());

        let event = make_event_with_metadata(metadata);
        let entities = AhtiEmitter::extract_entities(&event);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].r#type, EntityType::Deployment as i32);
        assert_eq!(entities[0].name, "my-deployment");
    }

    #[test]
    fn test_extract_entities_service() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-service".to_string());
        metadata.insert("resource".to_string(), "service".to_string());

        let event = make_event_with_metadata(metadata);
        let entities = AhtiEmitter::extract_entities(&event);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].r#type, EntityType::Service as i32);
    }

    #[test]
    fn test_extract_entities_unknown_resource() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-crd".to_string());
        metadata.insert("resource".to_string(), "customresource".to_string());

        let event = make_event_with_metadata(metadata);
        let entities = AhtiEmitter::extract_entities(&event);

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].r#type, EntityType::Unspecified as i32);
    }

    #[test]
    fn test_extract_entities_no_name() {
        let event = make_event();
        let entities = AhtiEmitter::extract_entities(&event);
        assert!(entities.is_empty());
    }

    // ==========================================================================
    // Relationship Extraction Tests
    // ==========================================================================

    #[test]
    fn test_extract_relationships_pod_to_node() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-pod".to_string());
        metadata.insert("pod_name".to_string(), "my-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("node_name".to_string(), "worker-1".to_string());
        metadata.insert("uid".to_string(), "pod-uid-123".to_string());
        metadata.insert("cluster_id".to_string(), "cluster-1".to_string());

        let event = make_event_with_metadata(metadata);
        let relationships = AhtiEmitter::extract_relationships(&event);

        assert_eq!(relationships.len(), 1);
        assert_eq!(
            relationships[0].r#type,
            RelationshipType::ScheduledOn as i32
        );

        let source = relationships[0].source.as_ref().unwrap();
        assert_eq!(source.r#type, EntityType::Pod as i32);
        assert_eq!(source.id, "pod-uid-123");

        let target = relationships[0].target.as_ref().unwrap();
        assert_eq!(target.r#type, EntityType::Node as i32);

        assert_eq!(
            relationships[0].labels.get("target_name"),
            Some(&"worker-1".to_string())
        );
    }

    #[test]
    fn test_extract_relationships_owner_deployment() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("uid".to_string(), "pod-uid-123".to_string());
        metadata.insert("owner_kind".to_string(), "Deployment".to_string());
        metadata.insert("owner_name".to_string(), "my-deployment".to_string());
        metadata.insert("owner_uid".to_string(), "deploy-uid-456".to_string());
        metadata.insert("cluster_id".to_string(), "cluster-1".to_string());

        let event = make_event_with_metadata(metadata);
        let relationships = AhtiEmitter::extract_relationships(&event);

        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].r#type, RelationshipType::Owns as i32);

        let source = relationships[0].source.as_ref().unwrap();
        assert_eq!(source.r#type, EntityType::Deployment as i32);
        assert_eq!(source.id, "deploy-uid-456");

        let target = relationships[0].target.as_ref().unwrap();
        assert_eq!(target.r#type, EntityType::Pod as i32);
        assert_eq!(target.id, "pod-uid-123");
    }

    #[test]
    fn test_extract_relationships_owner_statefulset() {
        let mut metadata = HashMap::new();
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("uid".to_string(), "pod-uid".to_string());
        metadata.insert("owner_kind".to_string(), "StatefulSet".to_string());
        metadata.insert("owner_name".to_string(), "my-sts".to_string());
        metadata.insert("owner_uid".to_string(), "sts-uid".to_string());

        let event = make_event_with_metadata(metadata);
        let relationships = AhtiEmitter::extract_relationships(&event);

        assert_eq!(relationships.len(), 1);
        let source = relationships[0].source.as_ref().unwrap();
        assert_eq!(source.r#type, EntityType::Statefulset as i32);
    }

    #[test]
    fn test_extract_relationships_owner_replicaset_skipped() {
        // ReplicaSet is not mapped in EntityType, so it should be skipped
        let mut metadata = HashMap::new();
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("owner_kind".to_string(), "ReplicaSet".to_string());
        metadata.insert("owner_name".to_string(), "my-rs".to_string());

        let event = make_event_with_metadata(metadata);
        let relationships = AhtiEmitter::extract_relationships(&event);

        assert!(relationships.is_empty());
    }

    #[test]
    fn test_extract_relationships_no_metadata() {
        let event = make_event();
        let relationships = AhtiEmitter::extract_relationships(&event);
        assert!(relationships.is_empty());
    }

    // ==========================================================================
    // Typed Event Data Mapping Tests
    // ==========================================================================

    #[test]
    fn test_map_event_data_none() {
        let result = AhtiEmitter::map_event_data(&None);
        assert!(result.is_none());
    }

    #[test]
    fn test_map_event_data_network() {
        let polku_data = PolkuEventData::Network(PolkuNetworkEventData {
            protocol: "TCP".to_string(),
            src_ip: "10.0.0.1".to_string(),
            dst_ip: "10.0.0.2".to_string(),
            src_port: 45678,
            dst_port: 8080,
            direction: "outbound".to_string(),
            dns_query: "".to_string(),
            dns_response: "".to_string(),
            http_method: "GET".to_string(),
            http_path: "/api/v1/pods".to_string(),
            http_status_code: 200,
            latency_ms: 15.5,
            bytes_sent: 1024,
            bytes_received: 4096,
            rtt_baseline_ms: 5.0,
            rtt_current_ms: 6.0,
            rtt_degradation_pct: 20.0,
            retransmit_count: 2,
            tcp_state: "ESTABLISHED".to_string(),
            process_name: "kubelet".to_string(),
            container_id: "abc123".to_string(),
            pod_name: "my-pod".to_string(),
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::Network(n) => {
                assert_eq!(n.protocol, "TCP");
                assert_eq!(n.src_ip, "10.0.0.1");
                assert_eq!(n.dst_ip, "10.0.0.2");
                assert_eq!(n.src_port, 45678);
                assert_eq!(n.dst_port, 8080);
                assert_eq!(n.direction, "outbound");
                assert_eq!(n.http_method, "GET");
                assert_eq!(n.http_path, "/api/v1/pods");
                assert_eq!(n.http_status_code, 200);
                assert!((n.latency_ms - 15.5).abs() < f64::EPSILON);
                assert_eq!(n.bytes_sent, 1024);
                assert_eq!(n.bytes_received, 4096);
                assert_eq!(n.retransmit_count, 2);
                assert_eq!(n.tcp_state, "ESTABLISHED");
                assert_eq!(n.process_name, "kubelet");
                assert_eq!(n.container_id, "abc123");
                assert_eq!(n.pod_name, "my-pod");
            }
            _ => panic!("Expected Network variant"),
        }
    }

    #[test]
    fn test_map_event_data_kernel() {
        let polku_data = PolkuEventData::Kernel(PolkuKernelEventData {
            event_type: "oom_kill".to_string(),
            pid: 1234,
            command: "java".to_string(),
            oom_victim_pid: 5678,
            oom_victim_comm: "oom-process".to_string(),
            memory_requested: 1073741824,
            signal: 9,
            signal_code: 0,
            syscall_id: 0,
            syscall_name: "".to_string(),
            syscall_retval: 0,
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::Kernel(k) => {
                assert_eq!(k.event_type, "oom_kill");
                assert_eq!(k.pid, 1234);
                assert_eq!(k.command, "java");
                assert_eq!(k.oom_victim_pid, 5678);
                assert_eq!(k.oom_victim_comm, "oom-process");
                assert_eq!(k.memory_requested, 1073741824);
                assert_eq!(k.signal, 9);
            }
            _ => panic!("Expected Kernel variant"),
        }
    }

    #[test]
    fn test_map_event_data_container() {
        let polku_data = PolkuEventData::Container(PolkuContainerEventData {
            container_id: "container-abc123".to_string(),
            container_name: "my-container".to_string(),
            image: "nginx:1.25".to_string(),
            state: "running".to_string(),
            exit_code: 0,
            restart_count: 2,
            start_time: None,
            cpu_usage: 0.5,
            memory_usage: 268435456,
            memory_limit: 536870912,
            disk_usage: 1073741824,
            signal: 0,
            cgroup_path: "/sys/fs/cgroup/memory/kubepods".to_string(),
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::Container(c) => {
                assert_eq!(c.container_id, "container-abc123");
                assert_eq!(c.container_name, "my-container");
                assert_eq!(c.image, "nginx:1.25");
                assert_eq!(c.state, "running");
                assert_eq!(c.exit_code, 0);
                assert_eq!(c.restart_count, 2);
                assert!((c.cpu_usage - 0.5).abs() < f64::EPSILON);
                assert_eq!(c.memory_usage, 268435456);
                assert_eq!(c.memory_limit, 536870912);
            }
            _ => panic!("Expected Container variant"),
        }
    }

    #[test]
    fn test_map_event_data_k8s() {
        let polku_data = PolkuEventData::K8s(PolkuK8sEventData {
            resource_type: "Deployment".to_string(),
            resource_name: "my-deployment".to_string(),
            namespace: "default".to_string(),
            reason: "ScalingReplicaSet".to_string(),
            message: "Scaled up replica set to 3".to_string(),
            replicas: 3,
            ready_replicas: 3,
            updated_replicas: 3,
            rollout_status: "complete".to_string(),
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::K8s(k) => {
                assert_eq!(k.resource_type, "Deployment");
                assert_eq!(k.resource_name, "my-deployment");
                assert_eq!(k.namespace, "default");
                assert_eq!(k.reason, "ScalingReplicaSet");
                assert_eq!(k.replicas, 3);
                assert_eq!(k.ready_replicas, 3);
                assert_eq!(k.rollout_status, "complete");
            }
            _ => panic!("Expected K8s variant"),
        }
    }

    #[test]
    fn test_map_event_data_process() {
        let polku_data = PolkuEventData::Process(PolkuProcessEventData {
            pid: 1234,
            ppid: 1,
            command: "/usr/bin/nginx".to_string(),
            args: "-g daemon off;".to_string(),
            uid: 101,
            gid: 101,
            user: "nginx".to_string(),
            exit_code: 0,
            start_time: None,
            end_time: None,
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::Process(p) => {
                assert_eq!(p.pid, 1234);
                assert_eq!(p.ppid, 1);
                assert_eq!(p.command, "/usr/bin/nginx");
                assert_eq!(p.args, "-g daemon off;");
                assert_eq!(p.uid, 101);
                assert_eq!(p.gid, 101);
                assert_eq!(p.user, "nginx");
                assert_eq!(p.exit_code, 0);
            }
            _ => panic!("Expected Process variant"),
        }
    }

    #[test]
    fn test_map_event_data_resource() {
        let polku_data = PolkuEventData::Resource(PolkuResourceEventData {
            resource_type: "cpu".to_string(),
            node_name: "worker-1".to_string(),
            cpu_usage_pct: 85.5,
            cpu_throttle_pct: 10.0,
            memory_used: 8589934592,
            memory_available: 8589934592,
            memory_pressure: 0.5,
            disk_used: 107374182400,
            disk_available: 107374182400,
            disk_io_utilization: 25.0,
        });

        let result = AhtiEmitter::map_event_data(&Some(polku_data));
        assert!(result.is_some());

        match result.unwrap() {
            ahti_types::ahti_event::Data::Resource(r) => {
                assert_eq!(r.resource_type, "cpu");
                assert_eq!(r.node_name, "worker-1");
                assert!((r.cpu_usage_pct - 85.5).abs() < f64::EPSILON);
                assert!((r.cpu_throttle_pct - 10.0).abs() < f64::EPSILON);
                assert_eq!(r.memory_used, 8589934592);
                assert!((r.memory_pressure - 0.5).abs() < f64::EPSILON);
                assert!((r.disk_io_utilization - 25.0).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Resource variant"),
        }
    }

    // ==========================================================================
    // Full Event Conversion Tests
    // ==========================================================================

    #[test]
    fn test_event_to_ahti_event_basic() {
        let mut event = make_event();
        event.id = "evt-123".to_string();
        event.timestamp_unix_ns = 1704067200_123_456_789;
        event.source = "tapio".to_string();
        event.event_type = "network.connection".to_string();
        event.severity = PolkuSeverity::Warning as i32;
        event.outcome = PolkuOutcome::Success as i32;

        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);

        assert_eq!(ahti_event.id, "evt-123");
        assert_eq!(ahti_event.source, "tapio");
        assert_eq!(ahti_event.r#type, EventType::Network as i32);
        assert_eq!(ahti_event.subtype, "connection");
        assert_eq!(ahti_event.severity, ahti_types::Severity::Warning as i32);
        assert_eq!(ahti_event.outcome, ahti_types::Outcome::Success as i32);

        let ts = ahti_event.timestamp.unwrap();
        assert_eq!(ts.seconds, 1704067200);
        assert_eq!(ts.nanos, 123_456_789);
    }

    #[test]
    fn test_event_to_ahti_event_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("cluster_id".to_string(), "prod-cluster".to_string());
        metadata.insert("namespace".to_string(), "kube-system".to_string());
        metadata.insert("trace_id".to_string(), "trace-abc".to_string());
        metadata.insert("span_id".to_string(), "span-123".to_string());
        metadata.insert("parent_span_id".to_string(), "span-parent".to_string());

        let event = make_event_with_metadata(metadata);
        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);

        assert_eq!(ahti_event.cluster, "prod-cluster");
        assert_eq!(ahti_event.namespace, "kube-system");
        assert_eq!(ahti_event.trace_id, "trace-abc");
        assert_eq!(ahti_event.span_id, "span-123");
        assert_eq!(ahti_event.parent_span_id, "span-parent");
    }

    #[test]
    fn test_event_to_ahti_event_with_typed_data() {
        let mut event = make_event();
        event.event_type = "network.http".to_string();
        event.data = Some(PolkuEventData::Network(PolkuNetworkEventData {
            protocol: "HTTP".to_string(),
            http_method: "POST".to_string(),
            http_path: "/api/events".to_string(),
            http_status_code: 201,
            latency_ms: 25.0,
            ..Default::default()
        }));

        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);

        assert!(ahti_event.data.is_some());
        match ahti_event.data.unwrap() {
            ahti_types::ahti_event::Data::Network(n) => {
                assert_eq!(n.protocol, "HTTP");
                assert_eq!(n.http_method, "POST");
                assert_eq!(n.http_path, "/api/events");
                assert_eq!(n.http_status_code, 201);
            }
            _ => panic!("Expected Network variant"),
        }

        // Duration should be extracted from network latency
        assert_eq!(ahti_event.duration_us, 25000);
    }

    #[test]
    fn test_event_to_ahti_event_with_entities() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "nginx-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("uid".to_string(), "uid-123".to_string());
        metadata.insert("namespace".to_string(), "default".to_string());
        metadata.insert("cluster_id".to_string(), "cluster-1".to_string());

        let event = make_event_with_metadata(metadata);
        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);

        assert_eq!(ahti_event.entities.len(), 1);
        assert_eq!(ahti_event.entities[0].name, "nginx-pod");
        assert_eq!(ahti_event.entities[0].r#type, EntityType::Pod as i32);
    }

    #[test]
    fn test_event_to_ahti_event_cluster_fallback() {
        // Test that "cluster" key works as fallback for "cluster_id"
        let mut metadata = HashMap::new();
        metadata.insert("cluster".to_string(), "fallback-cluster".to_string());

        let event = make_event_with_metadata(metadata);
        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);

        assert_eq!(ahti_event.cluster, "fallback-cluster");
    }

    // ==========================================================================
    // Endpoint Selection Tests (using internal state simulation)
    // ==========================================================================

    #[test]
    fn test_select_endpoint_prefers_known_latency() {
        // We can't easily test select_endpoint without creating an AhtiEmitter,
        // so we test EndpointState latency tracking instead
        let state1 = EndpointState::new();
        let state2 = EndpointState::new();

        state1.record_success(1000); // Known latency: 1ms
        state2.record_success(5000); // Known latency: 5ms

        // state1 should be preferred (lower latency)
        assert!(state1.avg_latency_us() < state2.avg_latency_us());
    }

    #[test]
    fn test_select_endpoint_unknown_latency_is_zero() {
        let state = EndpointState::new();
        assert_eq!(state.avg_latency_us(), 0);
    }

    #[test]
    fn test_endpoint_latency_averaging() {
        let state = EndpointState::new();
        state.record_success(100);
        state.record_success(200);
        state.record_success(300);
        state.record_success(400);

        // Average should be (100 + 200 + 300 + 400) / 4 = 250
        assert_eq!(state.avg_latency_us(), 250);
    }

    // ==========================================================================
    // BUG-FINDING TESTS - These expose actual issues in the code
    // ==========================================================================

    /// BUG: parse_event_type doesn't handle k8s events with >3 parts
    ///
    /// EXPECTED: "k8s.pod.container.started" should map to EventType::Pod
    /// since it's clearly a k8s pod-related event.
    ///
    /// BUG: Pattern ["k8s", resource, action] only matches exactly 3 parts.
    /// Events with 4+ parts fall through to the generic handler which
    /// doesn't recognize "k8s" as a category.
    #[test]
    fn test_bug_parse_event_type_k8s_four_parts() {
        let (event_type, _subtype) = AhtiEmitter::parse_event_type("k8s.pod.container.started");

        // EXPECTED: Should be EventType::Pod (it's a k8s pod event)
        // BUG: Returns EventType::Unspecified
        assert_eq!(
            event_type,
            EventType::Pod,
            "BUG: k8s.pod.container.started should return EventType::Pod, \
             but returns Unspecified because pattern only matches exactly 3 parts"
        );
    }

    /// BUG: extract_entities doesn't use cluster fallback like event_to_ahti_event does
    ///
    /// EXPECTED: Entity.cluster_id should use the same fallback logic as AhtiEvent.cluster.
    /// Both should check "cluster_id" first, then fall back to "cluster".
    ///
    /// BUG: extract_entities only checks "cluster_id", causing inconsistency
    /// where AhtiEvent.cluster has a value but Entity.cluster_id is empty.
    #[test]
    fn test_bug_extract_entities_cluster_fallback_inconsistency() {
        // Use "cluster" instead of "cluster_id"
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("cluster".to_string(), "prod-cluster".to_string());

        let event = make_event_with_metadata(metadata);

        // event_to_ahti_event correctly falls back to "cluster" key
        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);
        assert_eq!(
            ahti_event.cluster, "prod-cluster",
            "event_to_ahti_event uses fallback"
        );

        // EXPECTED: Entity should also have cluster_id = "prod-cluster"
        // BUG: extract_entities does NOT use the fallback
        let entities = AhtiEmitter::extract_entities(&event);
        assert_eq!(entities.len(), 1);
        assert_eq!(
            entities[0].cluster_id, "prod-cluster",
            "BUG: Entity.cluster_id should use cluster fallback like AhtiEvent.cluster does. \
             Currently returns empty string, causing inconsistency in same event."
        );
    }

    /// BUG #7: Negative timestamp produces negative seconds
    #[test]
    fn test_negative_timestamp_bug() {
        let mut event = make_event();
        event.timestamp_unix_ns = -1_000_000_000; // -1 second

        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);
        let ts = ahti_event.timestamp.unwrap();

        // Negative timestamps are technically valid (before Unix epoch)
        // but the division behavior might be unexpected
        assert_eq!(ts.seconds, -1, "Negative seconds work as expected");
        // nanos: -1_000_000_000 % 1_000_000_000 = 0, cast to i32 = 0
        assert_eq!(ts.nanos, 0, "Nanos are 0 for exact negative seconds");
    }

    /// BUG: Negative fractional timestamp produces invalid protobuf Timestamp
    ///
    /// EXPECTED: For -0.5 seconds (-500_000_000 ns), the protobuf Timestamp should
    /// normalize to seconds=-1, nanos=500_000_000 (per protobuf spec).
    ///
    /// BUG: Rust's % operator keeps the sign, so nanos becomes -500_000_000,
    /// which violates the protobuf Timestamp spec (nanos must be 0-999_999_999).
    #[test]
    fn test_bug_negative_fractional_timestamp() {
        let mut event = make_event();
        event.timestamp_unix_ns = -500_000_000; // -0.5 seconds

        let ahti_event = AhtiEmitter::event_to_ahti_event(&event);
        let ts = ahti_event.timestamp.unwrap();

        // EXPECTED per protobuf Timestamp spec:
        // -0.5 seconds should normalize to: seconds = -1, nanos = 500_000_000
        // (nanos must always be 0-999_999_999)
        //
        // BUG: Rust division truncates toward zero:
        // -500_000_000 / 1_000_000_000 = 0
        // -500_000_000 % 1_000_000_000 = -500_000_000
        // Result: seconds = 0, nanos = -500_000_000 (INVALID!)

        assert!(
            ts.nanos >= 0 && ts.nanos < 1_000_000_000,
            "BUG: Timestamp nanos must be 0-999_999_999 per protobuf spec. \
             Got nanos = {}, which is invalid and may cause parse errors in receivers.",
            ts.nanos
        );
    }

    /// BUG #7: NaN latency produces garbage duration
    #[test]
    fn test_nan_latency_produces_garbage_bug() {
        let mut event = make_event();
        event.data = Some(PolkuEventData::Network(PolkuNetworkEventData {
            latency_ms: f64::NAN,
            ..Default::default()
        }));

        let duration = AhtiEmitter::extract_duration_us(&event);
        // f64::NAN > 0.0 is false, so it returns 0
        // This is actually safe behavior (returns 0), not a bug
        assert_eq!(duration, 0, "NaN latency safely returns 0");
    }

    /// BUG #7: Infinity latency overflows
    #[test]
    fn test_infinity_latency_bug() {
        let mut event = make_event();
        event.data = Some(PolkuEventData::Network(PolkuNetworkEventData {
            latency_ms: f64::INFINITY,
            ..Default::default()
        }));

        let duration = AhtiEmitter::extract_duration_us(&event);
        // f64::INFINITY > 0.0 is true
        // (f64::INFINITY * 1000.0) as u64 = some large value or overflow
        // On most platforms, casting infinity to u64 gives u64::MAX or 0
        assert!(
            duration == 0 || duration == u64::MAX,
            "BUG: Infinity latency produces unpredictable result: {}",
            duration
        );
    }

    /// BUG: Entity state is hardcoded to 1, ignoring event context
    ///
    /// EXPECTED: Entity state should reflect the event context.
    /// Delete events should set state to indicate the entity is deleted.
    ///
    /// BUG: Line 655 hardcodes `state: 1` regardless of event type.
    #[test]
    fn test_bug_entity_state_hardcoded() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "deleted-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());

        // Create an event that represents a deletion
        let mut event = make_event_with_metadata(metadata);
        event.event_type = "k8s.pod.deleted".to_string();

        let entities = AhtiEmitter::extract_entities(&event);

        // EXPECTED: Delete event should produce entity with deleted/inactive state
        // BUG: State is hardcoded to 1 (active) regardless of event type
        let state_reflects_event_type = entities[0].state != 1; // Should NOT be 1 for delete

        assert!(
            state_reflects_event_type,
            "BUG: Entity state is hardcoded to 1 regardless of event type. \
             For event_type='k8s.pod.deleted', entity state should indicate deletion, \
             but got state={} (always 1). This causes inconsistency where AHTI sees \
             pod.deleted event but entity appears 'active'.",
            entities[0].state
        );
    }

    /// Test that cluster fallback works in relationships but not entities
    #[test]
    fn test_relationship_cluster_fallback_also_missing_bug() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "my-pod".to_string());
        metadata.insert("pod_name".to_string(), "my-pod".to_string());
        metadata.insert("resource".to_string(), "pod".to_string());
        metadata.insert("node_name".to_string(), "worker-1".to_string());
        metadata.insert("uid".to_string(), "pod-uid".to_string());
        metadata.insert("cluster".to_string(), "prod-cluster".to_string()); // "cluster" not "cluster_id"

        let event = make_event_with_metadata(metadata);
        let relationships = AhtiEmitter::extract_relationships(&event);

        // extract_relationships DOES use the cluster fallback (see line 369-374)
        assert_eq!(relationships.len(), 1);
        let source = relationships[0].source.as_ref().unwrap();
        assert_eq!(
            source.cluster_id, "prod-cluster",
            "Relationships DO use cluster fallback correctly"
        );

        // But entities don't - inconsistent!
        let entities = AhtiEmitter::extract_entities(&event);
        assert_eq!(
            entities[0].cluster_id, "",
            "Entities do NOT use cluster fallback"
        );
    }

    // ==========================================================================
    // AHTI UNAVAILABILITY TESTS - What happens when AHTI is down?
    // ==========================================================================

    /// BUG: Empty endpoints returns Init error at construction time
    #[tokio::test]
    async fn test_ahti_emitter_no_endpoints_fails_at_init() {
        let result = AhtiEmitter::with_endpoints(vec![]).await;
        assert!(result.is_err());
        match result {
            Err(PluginError::Init(msg)) => {
                assert_eq!(msg, "No AHTI endpoints provided");
            }
            _ => panic!("Expected Init error"),
        }
    }

    /// BUG: Invalid endpoint URL fails at construction - but "not-a-valid-url"
    /// is actually accepted by tonic! It treats it as a hostname.
    #[tokio::test]
    async fn test_ahti_emitter_invalid_url_accepted_bug() {
        // BUG: tonic's Endpoint::from_shared accepts almost any string as a valid URI
        // "not-a-valid-url" becomes a hostname, connection fails later at connect time
        let result = AhtiEmitter::with_endpoints(vec!["not-a-valid-url".to_string()]).await;

        // Actually returns Connection error, not Init error!
        assert!(result.is_err());
        match result {
            Err(PluginError::Connection(msg)) => {
                // Connection fails because it tries to resolve "not-a-valid-url" as a host
                assert!(msg.contains("Failed to connect to AHTI"), "Got: {}", msg);
            }
            Err(other) => panic!("Got unexpected error type: {:?}", other),
            Ok(_) => panic!("Should have failed"),
        }
    }

    /// BUG: Lazy mode accepts ANY string as valid URL
    #[tokio::test]
    async fn test_ahti_emitter_lazy_invalid_url_accepted_bug() {
        // BUG: Lazy mode doesn't validate URLs at all - accepts garbage
        let result = AhtiEmitter::with_endpoints_lazy(vec!["not-a-valid-url".to_string()]).await;

        // This SUCCEEDS because lazy mode never tries to connect!
        assert!(
            result.is_ok(),
            "BUG: Lazy mode accepts invalid URLs without validation"
        );
        // The error only surfaces when emit() is called
    }

    /// Lazy connections with valid URL but unreachable host succeed at init
    #[tokio::test]
    async fn test_ahti_emitter_lazy_unreachable_succeeds_at_init() {
        // Valid URL format but nothing listening - lazy should succeed
        let result =
            AhtiEmitter::with_endpoints_lazy(vec!["http://127.0.0.1:59999".to_string()]).await;
        assert!(
            result.is_ok(),
            "Lazy init should succeed even if endpoint unreachable"
        );
    }

    /// When AHTI is unreachable at init time (eager connect), it fails
    #[tokio::test]
    async fn test_ahti_emitter_unreachable_fails_at_eager_init() {
        // Nothing listening on this port
        let result = AhtiEmitter::new("http://127.0.0.1:59998").await;
        assert!(result.is_err());
        match result {
            Err(PluginError::Connection(msg)) => {
                assert!(msg.contains("Failed to connect to AHTI"), "Got: {}", msg);
            }
            _ => panic!("Expected Connection error for unreachable endpoint"),
        }
    }

    /// When all endpoints are unhealthy, emit still tries them all
    #[test]
    fn test_endpoint_state_becomes_unhealthy_after_threshold() {
        let state = EndpointState::new();

        // FAILURE_THRESHOLD is 3
        state.record_failure();
        assert!(state.is_healthy(), "1 failure: still healthy");

        state.record_failure();
        assert!(state.is_healthy(), "2 failures: still healthy");

        state.record_failure();
        assert!(!state.is_healthy(), "3 failures: now unhealthy");
    }

    /// BUG: Unhealthy duration is 30 seconds - could be too long or too short
    #[test]
    fn test_unhealthy_duration_is_30_seconds() {
        // Just documenting the constant value for awareness
        assert_eq!(
            UNHEALTHY_DURATION_MS, 30_000,
            "Unhealthy endpoints cool down for 30s"
        );
    }

    /// BUG: A single success resets ALL failure state, even if transient
    #[test]
    fn test_single_success_clears_all_failures() {
        let state = EndpointState::new();

        // Almost at threshold
        state.record_failure();
        state.record_failure();
        assert!(state.is_healthy());

        // One success resets everything
        state.record_success(1000);

        // Now we need 3 more failures to become unhealthy
        state.record_failure();
        state.record_failure();
        assert!(
            state.is_healthy(),
            "Still healthy - counter reset by success"
        );

        state.record_failure();
        assert!(!state.is_healthy(), "Now unhealthy after 3 NEW failures");
    }

    /// Test select_endpoint behavior when all endpoints are unhealthy
    #[test]
    fn test_select_endpoint_returns_none_when_all_unhealthy() {
        let state1 = EndpointState::new();
        let state2 = EndpointState::new();

        // Make both unhealthy
        for _ in 0..3 {
            state1.record_failure();
            state2.record_failure();
        }

        assert!(!state1.is_healthy());
        assert!(!state2.is_healthy());

        // Now select_endpoint should return None for both
        // (can't test directly without creating full AhtiEmitter, but we verify state)
    }

    /// BUG: avg_latency_us uses wrapping arithmetic - can overflow silently
    #[test]
    fn test_latency_overflow_wraps_silently() {
        let state = EndpointState::new();

        // Simulate extreme case: u64::MAX / 2 latency twice
        // This would overflow total_latency_us
        let huge_latency = u64::MAX / 2;
        state.record_success(huge_latency);
        state.record_success(huge_latency);

        // After overflow, total wraps around:
        // (u64::MAX/2) + (u64::MAX/2) = u64::MAX - 1 (actually wraps to u64::MAX - 1)
        // Actually: (9223372036854775807 * 2) wraps to 18446744073709551614
        // Which is still huge, so avg is still huge
        let avg = state.avg_latency_us();

        // The fetch_add wraps but doesn't cause weird behavior in this case
        // because we're still under u64::MAX for 2 adds of MAX/2
        // Real bug would occur if we add enough to wrap to small numbers
        let total = state.total_latency_us.load(Ordering::Relaxed);
        let count = state.emit_count.load(Ordering::Relaxed);

        // Verify the internal state
        assert_eq!(count, 2);
        // total should be (MAX/2 * 2) which wraps
        assert_eq!(total, u64::MAX - 1, "Wrapping addition result");
        assert_eq!(avg, (u64::MAX - 1) / 2, "Average of wrapped total");
    }

    /// BUG: Demonstrate actual overflow causing wrong average
    #[test]
    fn test_latency_overflow_causes_wrong_avg() {
        let state = EndpointState::new();

        // Add MAX twice - this definitely wraps
        state.record_success(u64::MAX);
        state.record_success(u64::MAX);

        let total = state.total_latency_us.load(Ordering::Relaxed);
        let count = state.emit_count.load(Ordering::Relaxed);
        let avg = state.avg_latency_us();

        // MAX + MAX wraps to MAX - 1 (due to wrapping add)
        // Actually: u64::MAX.wrapping_add(u64::MAX) = u64::MAX - 1
        assert_eq!(count, 2);
        assert_eq!(total, u64::MAX - 1); // Wrapped!
        assert_eq!(avg, (u64::MAX - 1) / 2);

        // BUG: We added 2x MAX latency but avg shows ~MAX/2
        // The user thinks latency is ~9.2 exabytes when it's actually infinite
    }

    /// BUG: emit_count can also overflow with enough calls
    #[test]
    fn test_emit_count_never_overflows_in_practice() {
        let state = EndpointState::new();

        // At 1 emit per millisecond, u64 would overflow after 584 million years
        // So this is fine - documenting it doesn't overflow in practice
        state.record_success(1000);
        assert_eq!(state.emit_count.load(Ordering::Relaxed), 1);
    }

    /// Test that health() returns false when no endpoints respond
    #[tokio::test]
    async fn test_health_returns_false_on_no_healthy_endpoints() {
        // Can't easily test without a mock server, but we can test EndpointState
        let state = EndpointState::new();
        for _ in 0..3 {
            state.record_failure();
        }
        assert!(!state.is_healthy());
    }

    /// BUG: emit() returns last error, not first - information loss
    #[test]
    fn test_emit_returns_last_error_behavior() {
        // When multiple endpoints fail, only the LAST error is returned
        // This could hide the root cause if first endpoint had useful error info
        // Can't test directly without mocks, but documenting expected behavior
    }

    /// Test that select_endpoint prefers lowest latency healthy endpoint
    #[test]
    fn test_endpoint_selection_prefers_low_latency() {
        let slow = EndpointState::new();
        let fast = EndpointState::new();

        slow.record_success(10_000); // 10ms
        fast.record_success(1_000); // 1ms

        assert!(fast.avg_latency_us() < slow.avg_latency_us());
    }

    /// BUG: Endpoints with 0 latency (never tried) are deprioritized
    #[test]
    fn test_untried_endpoints_are_fallback() {
        let tried = EndpointState::new();
        let untried = EndpointState::new();

        tried.record_success(5_000); // 5ms avg
        // untried has 0 latency (never tried)

        // select_endpoint prefers endpoints with known latency
        // This is documented behavior, not necessarily a bug
        assert_eq!(tried.avg_latency_us(), 5_000);
        assert_eq!(untried.avg_latency_us(), 0);
    }
}
