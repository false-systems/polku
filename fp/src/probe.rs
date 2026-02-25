//! Probe — self-aware pipeline observer
//!
//! A standalone async task that watches the pipeline via a [`MetricsSource`],
//! detects state transitions, and emits FALSE Protocol Occurrences about what it sees.
//!
//! The Probe is NOT part of the message pipeline — it's a side-channel observer
//! with its own Emitter. This avoids feedback loops.

use crate::occurrence::{Error, Occurrence, Outcome, Reasoning, Severity};
use bytes::Bytes;
use polku_core::Emitter;
use polku_gateway::message::Message;
use polku_gateway::metrics::Metrics;
use std::sync::Arc;
use tokio::time::{Duration, interval};

/// Point-in-time reading of the 5 values the Probe cares about
pub struct MetricsSnapshot {
    pub pipeline_pressure: f64,
    pub buffer_overflow_total: f64,
    pub emitter_healthy: usize,
    pub emitter_unhealthy: usize,
    pub any_circuit_open: bool,
    pub events_dropped_total: f64,
}

/// Source of metrics for the Probe — injectable for testing
pub trait MetricsSource: Send + Sync {
    /// Read current metrics. Returns `None` if metrics aren't initialized yet.
    fn snapshot(&self) -> Option<MetricsSnapshot>;
}

/// Reads from the global `Metrics::get()` singleton (production default)
pub struct LiveMetricsSource;

impl MetricsSource for LiveMetricsSource {
    fn snapshot(&self) -> Option<MetricsSnapshot> {
        let metrics = Metrics::get()?;

        let (emitter_healthy, emitter_unhealthy) = metrics.emitter_health_counts();

        Some(MetricsSnapshot {
            pipeline_pressure: metrics.pipeline_pressure.get(),
            buffer_overflow_total: metrics.buffer_overflow_total.get(),
            emitter_healthy,
            emitter_unhealthy,
            any_circuit_open: any_circuit_open(metrics),
            events_dropped_total: total_events_dropped(metrics),
        })
    }
}

/// Check if any circuit breaker is in Open state (value >= 1.0)
fn any_circuit_open(metrics: &Metrics) -> bool {
    use prometheus::core::Collector;

    let families = metrics.circuit_breaker_state.collect();
    for family in &families {
        for metric in family.get_metric() {
            if metric.get_gauge().get_value() >= 1.0 {
                return true;
            }
        }
    }
    false
}

/// Sum all events_dropped counters across all reason labels
fn total_events_dropped(metrics: &Metrics) -> f64 {
    use prometheus::core::Collector;

    let families = metrics.events_dropped.collect();
    let mut total = 0.0;
    for family in &families {
        for metric in family.get_metric() {
            total += metric.get_counter().get_value();
        }
    }
    total
}

/// Self-aware pipeline observer
///
/// Runs as an independent tokio task, reading metrics on a tick interval.
/// Detects state transitions (edge-triggered) and emits Occurrences via its own Emitter.
///
/// # Example
///
/// ```ignore
/// let probe = Probe::new("polku-prod")
///     .pressure_threshold(0.8)
///     .tick_secs(5)
///     .emitter(grpc_ahti);
/// tokio::spawn(probe.run());
/// ```
pub struct Probe {
    /// Instance name (appears in Occurrence source)
    instance: String,

    /// Tick interval
    tick: Duration,

    /// Pipeline pressure threshold (0.0 - 1.0)
    pressure_threshold: f64,

    /// Emitter for sending Occurrences (side-channel)
    emitter: Option<Arc<dyn Emitter>>,

    /// Metrics source (injectable for testing)
    metrics_source: Box<dyn MetricsSource>,

    /// Previous state for edge detection
    prev: PreviousState,
}

/// Tracked state from the previous tick (for edge detection)
#[derive(Default)]
struct PreviousState {
    pressure_high: bool,
    buffer_overflow_total: f64,
    emitter_unhealthy: bool,
    circuit_open: bool,
    events_dropped_total: f64,
}

impl Probe {
    pub fn new(instance: &str) -> Self {
        Self {
            instance: instance.to_string(),
            tick: Duration::from_secs(5),
            pressure_threshold: 0.8,
            emitter: None,
            metrics_source: Box::new(LiveMetricsSource),
            prev: PreviousState::default(),
        }
    }

    /// Set the pipeline pressure threshold (default: 0.8)
    pub fn pressure_threshold(mut self, threshold: f64) -> Self {
        self.pressure_threshold = threshold;
        self
    }

    /// Set the tick interval in seconds (default: 5)
    pub fn tick_secs(mut self, secs: u64) -> Self {
        self.tick = Duration::from_secs(secs);
        self
    }

    /// Set the Emitter for sending Occurrences
    pub fn emitter(mut self, emitter: impl Emitter + 'static) -> Self {
        self.emitter = Some(Arc::new(emitter));
        self
    }

    /// Override the metrics source (for testing)
    pub fn metrics_source(mut self, source: impl MetricsSource + 'static) -> Self {
        self.metrics_source = Box::new(source);
        self
    }

    /// Run the probe loop — call via `tokio::spawn(probe.run())`
    pub async fn run(mut self) {
        let Some(emitter) = self.emitter.take() else {
            tracing::error!("Probe started without an emitter — exiting");
            return;
        };

        tracing::info!(
            instance = %self.instance,
            tick_ms = self.tick.as_millis() as u64,
            pressure_threshold = self.pressure_threshold,
            "probe started"
        );

        let mut ticker = interval(self.tick);

        loop {
            ticker.tick().await;

            let Some(snapshot) = self.metrics_source.snapshot() else {
                tracing::debug!("Metrics not initialized yet, skipping tick");
                continue;
            };

            let occurrences = self.evaluate(&snapshot);

            // Emit all collected occurrences
            if !occurrences.is_empty() {
                let messages: Vec<Message> = occurrences
                    .into_iter()
                    .map(|occ| {
                        let json = serde_json::to_vec(&occ).unwrap_or_default();
                        Message::new(
                            occ.source.as_str(),
                            occ.occurrence_type.as_str(),
                            Bytes::from(json),
                        )
                    })
                    .collect();

                if let Err(e) = emitter.emit(&messages).await {
                    tracing::warn!(error = %e, "probe failed to emit occurrences");
                }
            }
        }
    }

    /// Evaluate a metrics snapshot and return any triggered Occurrences.
    ///
    /// Updates internal edge-detection state. This is the core logic,
    /// extracted so it can be tested without the async tick loop.
    fn evaluate(&mut self, snapshot: &MetricsSnapshot) -> Vec<Occurrence> {
        let mut occurrences = Vec::new();

        // 1. Pipeline pressure
        let pressure_high = snapshot.pipeline_pressure >= self.pressure_threshold;
        if pressure_high && !self.prev.pressure_high {
            occurrences.push(self.build_occurrence(
                "gateway.pipeline.pressure_high",
                Severity::Warning,
                Outcome::InProgress,
                Some(Error {
                    code: "PRESSURE_HIGH".into(),
                    what_failed: format!(
                        "Pipeline pressure {:.2} exceeds threshold {:.2}",
                        snapshot.pipeline_pressure, self.pressure_threshold
                    ),
                    why_it_matters: Some(
                        "High pressure indicates the pipeline is approaching overload".into(),
                    ),
                    possible_causes: vec![
                        "Downstream emitters are slow".into(),
                        "Ingestion rate spike".into(),
                        "Buffer approaching capacity".into(),
                    ],
                    suggested_fix: Some(
                        "Check emitter health and consider scaling downstream".into(),
                    ),
                }),
                Some(Reasoning {
                    summary: "Pipeline pressure exceeded threshold".into(),
                    explanation: format!(
                        "Composite pressure metric ({:.2}) crossed configured threshold ({:.2}). \
                         This is a weighted combination of buffer fill, emit failure rate, \
                         and channel utilization.",
                        snapshot.pipeline_pressure, self.pressure_threshold
                    ),
                    confidence: 0.9,
                    causal_chain: vec![],
                    patterns_matched: vec!["threshold_crossing".into()],
                }),
            ));
        }
        self.prev.pressure_high = pressure_high;

        // 2. Buffer overflow
        if snapshot.buffer_overflow_total > self.prev.buffer_overflow_total {
            let delta = snapshot.buffer_overflow_total - self.prev.buffer_overflow_total;
            occurrences.push(self.build_occurrence(
                "gateway.buffer.overflow",
                Severity::Error,
                Outcome::Failure,
                Some(Error {
                    code: "BUFFER_OVERFLOW".into(),
                    what_failed: format!("{delta:.0} messages dropped due to buffer overflow"),
                    why_it_matters: Some("Data loss — messages are being discarded".into()),
                    possible_causes: vec![
                        "Buffer capacity too small for ingestion rate".into(),
                        "Emitters not draining fast enough".into(),
                    ],
                    suggested_fix: Some("Increase buffer capacity or reduce ingestion rate".into()),
                }),
                None,
            ));
        }
        self.prev.buffer_overflow_total = snapshot.buffer_overflow_total;

        // 3. Emitter health
        let any_unhealthy = snapshot.emitter_unhealthy > 0;
        if any_unhealthy && !self.prev.emitter_unhealthy {
            occurrences.push(self.build_occurrence(
                "gateway.emitter.unhealthy",
                Severity::Error,
                Outcome::Failure,
                Some(Error {
                    code: "EMITTER_UNHEALTHY".into(),
                    what_failed: format!(
                        "{} of {} emitters are unhealthy",
                        snapshot.emitter_unhealthy,
                        snapshot.emitter_healthy + snapshot.emitter_unhealthy
                    ),
                    why_it_matters: Some("Messages may not reach their destinations".into()),
                    possible_causes: vec![
                        "Downstream service is down".into(),
                        "Network connectivity issues".into(),
                    ],
                    suggested_fix: Some("Check downstream service health".into()),
                }),
                None,
            ));
        }
        self.prev.emitter_unhealthy = any_unhealthy;

        // 4. Circuit breaker
        if snapshot.any_circuit_open && !self.prev.circuit_open {
            occurrences.push(self.build_occurrence(
                "gateway.emitter.circuit_open",
                Severity::Critical,
                Outcome::Failure,
                Some(Error {
                    code: "CIRCUIT_OPEN".into(),
                    what_failed: "Circuit breaker opened for one or more emitters".into(),
                    why_it_matters: Some(
                        "Messages to affected emitters are being rejected".into(),
                    ),
                    possible_causes: vec![
                        "Consecutive failures exceeded threshold".into(),
                        "Downstream service is unresponsive".into(),
                    ],
                    suggested_fix: Some(
                        "Investigate downstream service; circuit will auto-recover via half-open state".into(),
                    ),
                }),
                None,
            ));
        }
        self.prev.circuit_open = snapshot.any_circuit_open;

        // 5. Events dropped (rate increase)
        if snapshot.events_dropped_total > self.prev.events_dropped_total {
            let delta = snapshot.events_dropped_total - self.prev.events_dropped_total;
            occurrences.push(self.build_occurrence(
                "gateway.events.dropped",
                Severity::Warning,
                Outcome::Failure,
                Some(Error {
                    code: "EVENTS_DROPPED".into(),
                    what_failed: format!("{delta:.0} events dropped since last check"),
                    why_it_matters: Some("Data loss in the pipeline".into()),
                    possible_causes: vec![
                        "Buffer overflow".into(),
                        "Middleware filtering too aggressively".into(),
                        "Emitter failures".into(),
                    ],
                    suggested_fix: None,
                }),
                None,
            ));
        }
        self.prev.events_dropped_total = snapshot.events_dropped_total;

        occurrences
    }

    fn build_occurrence(
        &self,
        occurrence_type: &str,
        severity: Severity,
        outcome: Outcome,
        error: Option<Error>,
        reasoning: Option<Reasoning>,
    ) -> Occurrence {
        let id = ulid::Ulid::new().to_string();
        let timestamp = chrono::Utc::now().to_rfc3339();

        Occurrence {
            id,
            timestamp,
            source: self.instance.clone(),
            occurrence_type: occurrence_type.to_string(),
            severity,
            outcome,
            context: None,
            error,
            reasoning,
            data: None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use polku_core::PluginError;
    use std::sync::Mutex;

    // ── Mock MetricsSource ──────────────────────────────────────────────

    struct MockMetricsSource {
        snapshot: Arc<Mutex<Option<MetricsSnapshot>>>,
    }

    impl MockMetricsSource {
        fn new() -> (Self, Arc<Mutex<Option<MetricsSnapshot>>>) {
            let snapshot = Arc::new(Mutex::new(None));
            (
                Self {
                    snapshot: snapshot.clone(),
                },
                snapshot,
            )
        }
    }

    impl MetricsSource for MockMetricsSource {
        fn snapshot(&self) -> Option<MetricsSnapshot> {
            let guard = self.snapshot.lock().unwrap();
            let s = guard.as_ref()?;
            Some(MetricsSnapshot {
                pipeline_pressure: s.pipeline_pressure,
                buffer_overflow_total: s.buffer_overflow_total,
                emitter_healthy: s.emitter_healthy,
                emitter_unhealthy: s.emitter_unhealthy,
                any_circuit_open: s.any_circuit_open,
                events_dropped_total: s.events_dropped_total,
            })
        }
    }

    fn idle_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            pipeline_pressure: 0.0,
            buffer_overflow_total: 0.0,
            emitter_healthy: 2,
            emitter_unhealthy: 0,
            any_circuit_open: false,
            events_dropped_total: 0.0,
        }
    }

    // ── Mock Emitter ────────────────────────────────────────────────────

    struct CapturingEmitter {
        messages: Arc<Mutex<Vec<Message>>>,
    }

    impl CapturingEmitter {
        fn new() -> (Self, Arc<Mutex<Vec<Message>>>) {
            let messages = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    messages: messages.clone(),
                },
                messages,
            )
        }
    }

    #[async_trait::async_trait]
    impl Emitter for CapturingEmitter {
        fn name(&self) -> &str {
            "capturing"
        }

        async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
            self.messages
                .lock()
                .unwrap()
                .extend(messages.iter().cloned());
            Ok(())
        }

        async fn health(&self) -> bool {
            true
        }
    }

    // ── Unit tests on evaluate() ────────────────────────────────────────

    #[test]
    fn test_probe_builder() {
        let (emitter, _) = CapturingEmitter::new();
        let probe = Probe::new("test-instance")
            .pressure_threshold(0.9)
            .tick_secs(10)
            .emitter(emitter);

        assert_eq!(probe.instance, "test-instance");
        assert_eq!(probe.pressure_threshold, 0.9);
        assert_eq!(probe.tick, Duration::from_secs(10));
        assert!(probe.emitter.is_some());
    }

    #[test]
    fn test_pressure_above_threshold_emits() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            pipeline_pressure: 0.85,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].occurrence_type, "gateway.pipeline.pressure_high");
        assert_eq!(occs[0].severity, Severity::Warning);
        assert!(occs[0].error.as_ref().unwrap().what_failed.contains("0.85"));
    }

    #[test]
    fn test_pressure_below_threshold_no_emission() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            pipeline_pressure: 0.5,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert!(occs.is_empty());
    }

    #[test]
    fn test_pressure_edge_triggered_not_level() {
        let mut probe = Probe::new("test");

        let high = MetricsSnapshot {
            pipeline_pressure: 0.9,
            ..idle_snapshot()
        };

        // First crossing → emit
        let occs = probe.evaluate(&high);
        assert_eq!(occs.len(), 1);

        // Still high → no emit (edge-triggered)
        let occs = probe.evaluate(&high);
        assert!(occs.is_empty());

        // Back to normal
        let low = idle_snapshot();
        let occs = probe.evaluate(&low);
        assert!(occs.is_empty());

        // Cross again → emit again
        let occs = probe.evaluate(&high);
        assert_eq!(occs.len(), 1);
    }

    #[test]
    fn test_emitter_unhealthy_emits() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            emitter_healthy: 1,
            emitter_unhealthy: 1,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].occurrence_type, "gateway.emitter.unhealthy");
        assert_eq!(occs[0].severity, Severity::Error);
    }

    #[test]
    fn test_emitter_unhealthy_edge_triggered() {
        let mut probe = Probe::new("test");

        let unhealthy = MetricsSnapshot {
            emitter_unhealthy: 1,
            ..idle_snapshot()
        };

        // First → emit
        assert_eq!(probe.evaluate(&unhealthy).len(), 1);
        // Still unhealthy → no emit
        assert!(probe.evaluate(&unhealthy).is_empty());
        // Recovered
        assert!(probe.evaluate(&idle_snapshot()).is_empty());
        // Unhealthy again → emit
        assert_eq!(probe.evaluate(&unhealthy).len(), 1);
    }

    #[test]
    fn test_buffer_overflow_emits_on_increase() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            buffer_overflow_total: 5.0,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].occurrence_type, "gateway.buffer.overflow");
        assert!(occs[0].error.as_ref().unwrap().what_failed.contains("5"));

        // Same total → no emit
        assert!(probe.evaluate(&snapshot).is_empty());

        // More overflow → emit again
        let more = MetricsSnapshot {
            buffer_overflow_total: 12.0,
            ..idle_snapshot()
        };
        let occs = probe.evaluate(&more);
        assert_eq!(occs.len(), 1);
        assert!(occs[0].error.as_ref().unwrap().what_failed.contains("7"));
    }

    #[test]
    fn test_circuit_open_emits() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            any_circuit_open: true,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].occurrence_type, "gateway.emitter.circuit_open");
        assert_eq!(occs[0].severity, Severity::Critical);
    }

    #[test]
    fn test_events_dropped_emits_on_increase() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            events_dropped_total: 10.0,
            ..idle_snapshot()
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].occurrence_type, "gateway.events.dropped");
    }

    #[test]
    fn test_multiple_conditions_emit_multiple_occurrences() {
        let mut probe = Probe::new("test");

        let snapshot = MetricsSnapshot {
            pipeline_pressure: 0.95,
            buffer_overflow_total: 3.0,
            emitter_healthy: 0,
            emitter_unhealthy: 2,
            any_circuit_open: true,
            events_dropped_total: 50.0,
        };

        let occs = probe.evaluate(&snapshot);
        assert_eq!(occs.len(), 5);

        let types: Vec<&str> = occs.iter().map(|o| o.occurrence_type.as_str()).collect();
        assert!(types.contains(&"gateway.pipeline.pressure_high"));
        assert!(types.contains(&"gateway.buffer.overflow"));
        assert!(types.contains(&"gateway.emitter.unhealthy"));
        assert!(types.contains(&"gateway.emitter.circuit_open"));
        assert!(types.contains(&"gateway.events.dropped"));
    }

    #[test]
    fn test_idle_system_no_emissions() {
        let mut probe = Probe::new("test");
        let occs = probe.evaluate(&idle_snapshot());
        assert!(occs.is_empty());
    }

    // ── Async integration test with mock ────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn test_probe_tick_emits_via_emitter() {
        let (mock_source, mock_state) = MockMetricsSource::new();
        let (emitter, captured) = CapturingEmitter::new();

        let probe = Probe::new("test-prod")
            .tick_secs(1)
            .pressure_threshold(0.8)
            .metrics_source(mock_source)
            .emitter(emitter);

        let handle = tokio::spawn(probe.run());

        // Tick 1: no metrics → skip
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(captured.lock().unwrap().is_empty());

        // Set metrics: pressure high
        *mock_state.lock().unwrap() = Some(MetricsSnapshot {
            pipeline_pressure: 0.9,
            buffer_overflow_total: 0.0,
            emitter_healthy: 2,
            emitter_unhealthy: 0,
            any_circuit_open: false,
            events_dropped_total: 0.0,
        });

        // Tick 2: pressure high → emit
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        // Give the spawned task a chance to process
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let msgs = captured.lock().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].message_type, "gateway.pipeline.pressure_high");

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn test_probe_tick_interval_respected() {
        let (mock_source, mock_state) = MockMetricsSource::new();
        let (emitter, captured) = CapturingEmitter::new();

        let probe = Probe::new("test")
            .tick_secs(5)
            .metrics_source(mock_source)
            .emitter(emitter);

        let handle = tokio::spawn(probe.run());

        // interval fires immediately at t=0 — let first tick pass (no metrics yet)
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(captured.lock().unwrap().is_empty());

        // Set metrics with unhealthy emitter
        *mock_state.lock().unwrap() = Some(MetricsSnapshot {
            pipeline_pressure: 0.0,
            buffer_overflow_total: 0.0,
            emitter_healthy: 0,
            emitter_unhealthy: 1,
            any_circuit_open: false,
            events_dropped_total: 0.0,
        });

        // Advance 3s (less than 5s tick) → no emission yet
        tokio::time::advance(Duration::from_secs(3)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(captured.lock().unwrap().is_empty());

        // Advance 2 more seconds (total 5s) → tick fires, emits
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(captured.lock().unwrap().len(), 1);

        handle.abort();
    }
}
