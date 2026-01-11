//! AHTI Emitter - sends events to AHTI for analysis
//!
//! Converts polku_core::Event to ahti.v1.AhtiEvent and streams to AHTI.
//! Supports typed event data (network, kernel, container, k8s, process, resource)
//! which maps directly to Ahti's event model.

mod ahti_proto;

use super::{Emitter, Event, PluginError};
use ahti_proto::ahti::v1::{
    self as ahti_types, ahti_service_client::AhtiServiceClient, AhtiEvent, AhtiEventBatch,
    AhtiHealthRequest, Entity, EntityType, EventType,
};
// Import polku_core types for the Event.data oneof
use polku_core::proto::event::Data as PolkuEventData;
use polku_core::{Outcome as PolkuOutcome, Severity as PolkuSeverity};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const FAILURE_THRESHOLD: u32 = 3;
const UNHEALTHY_DURATION_MS: u64 = 30_000;

struct EndpointState {
    load: AtomicU32,
    unhealthy_until: AtomicU64,
    consecutive_failures: AtomicU32,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            load: AtomicU32::new(500),
            unhealthy_until: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
        }
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.unhealthy_until.store(0, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= FAILURE_THRESHOLD {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.unhealthy_until
                .store(now + UNHEALTHY_DURATION_MS, Ordering::Relaxed);
        }
    }

    fn is_healthy(&self) -> bool {
        let unhealthy_until = self.unhealthy_until.load(Ordering::Relaxed);
        if unhealthy_until == 0 {
            return true;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now >= unhealthy_until
    }

    fn get_load(&self) -> f64 {
        self.load.load(Ordering::Relaxed) as f64 / 1000.0
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

    fn select_endpoint(&self, exclude: &[bool]) -> Option<usize> {
        let mut best_idx = None;
        let mut best_load = f64::MAX;

        for (idx, state) in self.states.iter().enumerate() {
            if exclude[idx] || !state.is_healthy() {
                continue;
            }

            let load = state.get_load();
            if load < best_load {
                best_load = load;
                best_idx = Some(idx);
            }
        }

        best_idx
    }

    fn select_any_untried(&self, exclude: &[bool]) -> Option<usize> {
        for (idx, excluded) in exclude.iter().enumerate() {
            if !excluded {
                return Some(idx);
            }
        }
        None
    }

    async fn try_emit_to(&self, idx: usize, events: &[Event]) -> Result<(), PluginError> {
        let mut client = self.clients[idx].clone();
        let endpoint = &self.endpoints[idx];
        let state = &self.states[idx];

        let ahti_events: Vec<AhtiEvent> = events.iter().map(Self::event_to_ahti_event).collect();

        let batch = AhtiEventBatch {
            events: ahti_events,
        };

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
                        if ack.success {
                            debug!(endpoint = %endpoint, acked = ack.event_ids.len(), "Batch sent to AHTI");
                            state.record_success();
                            Ok(())
                        } else {
                            warn!(endpoint = %endpoint, error = %ack.error, "AHTI rejected batch");
                            state.record_failure();
                            Err(PluginError::Send(format!("AHTI rejected batch: {}", ack.error)))
                        }
                    }
                    Ok(None) => {
                        debug!(endpoint = %endpoint, "Stream ended normally");
                        state.record_success();
                        Ok(())
                    }
                    Err(e) => {
                        error!(endpoint = %endpoint, error = %e, "Stream error from AHTI");
                        state.record_failure();
                        Err(PluginError::Send(format!("Stream error from {}: {}", endpoint, e)))
                    }
                }
            }
            Err(e) => {
                error!(endpoint = %endpoint, error = %e, "Failed to open stream to AHTI");
                state.record_failure();
                Err(PluginError::Send(format!("Failed to send to {}: {}", endpoint, e)))
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
            parent_span_id: event.metadata.get("parent_span_id").cloned().unwrap_or_default(),
            duration_us: 0,
            entities,
            relationships: vec![],
            cluster,
            namespace,
            labels: event.metadata.clone(),
            error: None,
            data,
        }
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
                    start_time: c.start_time.clone(),
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
                    start_time: p.start_time.clone(),
                    end_time: p.end_time.clone(),
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
                    "deployment" | "replicaset" | "statefulset" | "daemonset" => EventType::Deployment,
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
            let resource = event.metadata.get("resource").map(|s| s.as_str()).unwrap_or("");
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
                cluster_id: event.metadata.get("cluster_id").cloned().unwrap_or_default(),
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

    async fn emit(&self, events: &[Event]) -> Result<(), PluginError> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tried = vec![false; self.clients.len()];
        let mut last_error = None;

        while let Some(idx) = self.select_endpoint(&tried) {
            tried[idx] = true;

            match self.try_emit_to(idx, events).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(endpoint = %self.endpoints[idx], error = %e, "Emit failed, trying next endpoint");
                    last_error = Some(e);
                }
            }
        }

        while let Some(idx) = self.select_any_untried(&tried) {
            tried[idx] = true;

            match self.try_emit_to(idx, events).await {
                Ok(()) => {
                    info!(endpoint = %self.endpoints[idx], "Unhealthy endpoint recovered");
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        error!(event_count = events.len(), "All AHTI endpoints failed");

        Err(last_error.unwrap_or_else(|| PluginError::Send("No AHTI endpoints available".to_string())))
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
