//! Prometheus metrics for POLKU

use crate::error::{PolkuError, Result};
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder, register_counter,
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
};
use std::sync::OnceLock;

/// Global metrics instance
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// All POLKU metrics
pub struct Metrics {
    // ─────────────────────────────────────────────────────────────────────────
    // Event counters
    // ─────────────────────────────────────────────────────────────────────────
    /// Events received (by source, type)
    pub events_received: CounterVec,

    /// Events forwarded (by output, type)
    pub events_forwarded: CounterVec,

    /// Events dropped (by reason)
    pub events_dropped: CounterVec,

    // ─────────────────────────────────────────────────────────────────────────
    // Throughput & performance
    // ─────────────────────────────────────────────────────────────────────────
    /// Instantaneous throughput (events/sec) - updated each flush
    pub events_per_second: Gauge,

    /// Total flushes performed
    pub flush_total: Counter,

    /// Batch sizes (histogram of events per flush)
    pub batch_size: HistogramVec,

    /// Flush duration in seconds
    pub flush_duration_seconds: HistogramVec,

    // ─────────────────────────────────────────────────────────────────────────
    // Buffer metrics
    // ─────────────────────────────────────────────────────────────────────────
    /// Current buffer size
    pub buffer_size: Gauge,

    /// Buffer capacity
    pub buffer_capacity: Gauge,

    /// Buffer overflow events (when buffer is full)
    pub buffer_overflow_total: Counter,

    /// Bytes saved by tiered buffer compression
    pub tiered_buffer_compressed_bytes: Counter,

    /// Messages currently in secondary (compressed) tier
    pub tiered_buffer_secondary_size: Gauge,

    // ─────────────────────────────────────────────────────────────────────────
    // Emitter health
    // ─────────────────────────────────────────────────────────────────────────
    /// Per-emitter health (1 = healthy, 0 = unhealthy)
    pub emitter_health: GaugeVec,

    /// Circuit breaker state per emitter (0=closed, 1=open, 2=half-open)
    pub circuit_breaker_state: GaugeVec,

    // ─────────────────────────────────────────────────────────────────────────
    // Latency & streams
    // ─────────────────────────────────────────────────────────────────────────
    /// Event processing latency (by source)
    pub processing_latency: HistogramVec,

    /// Active streams (by source)
    pub active_streams: Gauge,

    /// Plugin health (1 = healthy, 0 = unhealthy)
    pub plugin_health: Gauge,
}

impl Metrics {
    /// Initialize metrics (call once at startup)
    ///
    /// Returns error if metric registration fails.
    #[allow(clippy::result_large_err)]
    pub fn init() -> Result<&'static Metrics> {
        if let Some(metrics) = METRICS.get() {
            return Ok(metrics);
        }

        let metrics = Metrics {
            // ─────────────────────────────────────────────────────────────────
            // Event counters
            // ─────────────────────────────────────────────────────────────────
            events_received: register_counter_vec!(
                "polku_events_received_total",
                "Total events received",
                &["source", "type"]
            )
            .map_err(|e| PolkuError::Metrics(format!("events_received: {e}")))?,

            events_forwarded: register_counter_vec!(
                "polku_events_forwarded_total",
                "Total events forwarded to outputs",
                &["output", "type"]
            )
            .map_err(|e| PolkuError::Metrics(format!("events_forwarded: {e}")))?,

            events_dropped: register_counter_vec!(
                "polku_events_dropped_total",
                "Total events dropped",
                &["reason"]
            )
            .map_err(|e| PolkuError::Metrics(format!("events_dropped: {e}")))?,

            // ─────────────────────────────────────────────────────────────────
            // Throughput & performance
            // ─────────────────────────────────────────────────────────────────
            events_per_second: register_gauge!(
                "polku_events_per_second",
                "Instantaneous throughput (events/sec)"
            )
            .map_err(|e| PolkuError::Metrics(format!("events_per_second: {e}")))?,

            flush_total: register_counter!("polku_flush_total", "Total number of flush operations")
                .map_err(|e| PolkuError::Metrics(format!("flush_total: {e}")))?,

            batch_size: register_histogram_vec!(
                "polku_batch_size",
                "Number of events per flush batch",
                &["emitter"],
                // Buckets: 1, 10, 50, 100, 500, 1000, 5000, 10000
                vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]
            )
            .map_err(|e| PolkuError::Metrics(format!("batch_size: {e}")))?,

            flush_duration_seconds: register_histogram_vec!(
                "polku_flush_duration_seconds",
                "Time spent flushing events to emitters",
                &["emitter"],
                // Buckets: 100us to 10s
                vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0
                ]
            )
            .map_err(|e| PolkuError::Metrics(format!("flush_duration_seconds: {e}")))?,

            // ─────────────────────────────────────────────────────────────────
            // Buffer metrics
            // ─────────────────────────────────────────────────────────────────
            buffer_size: register_gauge!("polku_buffer_size", "Current number of events in buffer")
                .map_err(|e| PolkuError::Metrics(format!("buffer_size: {e}")))?,

            buffer_capacity: register_gauge!("polku_buffer_capacity", "Maximum buffer capacity")
                .map_err(|e| PolkuError::Metrics(format!("buffer_capacity: {e}")))?,

            buffer_overflow_total: register_counter!(
                "polku_buffer_overflow_total",
                "Total events dropped due to buffer overflow"
            )
            .map_err(|e| PolkuError::Metrics(format!("buffer_overflow_total: {e}")))?,

            tiered_buffer_compressed_bytes: register_counter!(
                "polku_tiered_buffer_compressed_bytes_total",
                "Total bytes saved by tiered buffer compression"
            )
            .map_err(|e| PolkuError::Metrics(format!("tiered_buffer_compressed_bytes: {e}")))?,

            tiered_buffer_secondary_size: register_gauge!(
                "polku_tiered_buffer_secondary_size",
                "Number of messages in secondary (compressed) buffer tier"
            )
            .map_err(|e| PolkuError::Metrics(format!("tiered_buffer_secondary_size: {e}")))?,

            // ─────────────────────────────────────────────────────────────────
            // Emitter health
            // ─────────────────────────────────────────────────────────────────
            emitter_health: register_gauge_vec!(
                "polku_emitter_health",
                "Emitter health status (1 = healthy, 0 = unhealthy)",
                &["emitter"]
            )
            .map_err(|e| PolkuError::Metrics(format!("emitter_health: {e}")))?,

            circuit_breaker_state: register_gauge_vec!(
                "polku_circuit_breaker_state",
                "Circuit breaker state (0 = closed, 1 = open, 2 = half-open)",
                &["emitter"]
            )
            .map_err(|e| PolkuError::Metrics(format!("circuit_breaker_state: {e}")))?,

            // ─────────────────────────────────────────────────────────────────
            // Latency & streams
            // ─────────────────────────────────────────────────────────────────
            processing_latency: register_histogram_vec!(
                "polku_processing_latency_seconds",
                "Event processing latency",
                &["source"],
                vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
            )
            .map_err(|e| PolkuError::Metrics(format!("processing_latency: {e}")))?,

            active_streams: register_gauge!(
                "polku_active_streams",
                "Number of active gRPC streams"
            )
            .map_err(|e| PolkuError::Metrics(format!("active_streams: {e}")))?,

            plugin_health: register_gauge!(
                "polku_plugin_health",
                "Plugin health status (1 = healthy, 0 = unhealthy)"
            )
            .map_err(|e| PolkuError::Metrics(format!("plugin_health: {e}")))?,
        };

        // Set the metrics (only succeeds once)
        let _ = METRICS.set(metrics);

        METRICS
            .get()
            .ok_or_else(|| PolkuError::Metrics("Failed to initialize metrics".to_string()))
    }

    /// Get the global metrics instance
    ///
    /// Returns None if metrics haven't been initialized yet.
    pub fn get() -> Option<&'static Metrics> {
        METRICS.get()
    }

    /// Record event received
    pub fn record_received(&self, source: &str, event_type: &str, count: u64) {
        self.events_received
            .with_label_values(&[source, event_type])
            .inc_by(count as f64);
    }

    /// Record event forwarded
    pub fn record_forwarded(&self, output: &str, event_type: &str, count: u64) {
        self.events_forwarded
            .with_label_values(&[output, event_type])
            .inc_by(count as f64);
    }

    /// Record multiple forwarded events in batch (more efficient than per-event)
    ///
    /// This reduces Prometheus HashMap lookups from O(events) to O(unique label combos).
    /// Use this when processing a batch of events to avoid per-event overhead.
    ///
    /// Accepts a reference to a HashMap<(&str, &str), u64> for zero-copy iteration.
    pub fn record_forwarded_batch(&self, counts: &std::collections::HashMap<(&str, &str), u64>) {
        for ((output, event_type), count) in counts {
            self.events_forwarded
                .with_label_values(&[*output, *event_type])
                .inc_by(*count as f64);
        }
    }

    /// Record events dropped
    pub fn record_dropped(&self, reason: &str, count: u64) {
        self.events_dropped
            .with_label_values(&[reason])
            .inc_by(count as f64);
    }

    /// Update buffer size
    pub fn set_buffer_size(&self, size: usize) {
        self.buffer_size.set(size as f64);
    }

    /// Update buffer capacity
    pub fn set_buffer_capacity(&self, capacity: usize) {
        self.buffer_capacity.set(capacity as f64);
    }

    /// Record processing latency
    pub fn record_latency(&self, source: &str, seconds: f64) {
        self.processing_latency
            .with_label_values(&[source])
            .observe(seconds);
    }

    /// Increment active streams
    pub fn inc_streams(&self) {
        self.active_streams.inc();
    }

    /// Decrement active streams
    pub fn dec_streams(&self) {
        self.active_streams.dec();
    }

    /// Set plugin health
    pub fn set_plugin_health(&self, healthy: bool) {
        self.plugin_health.set(if healthy { 1.0 } else { 0.0 });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Throughput & performance helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Update instantaneous throughput (events/sec)
    pub fn set_events_per_second(&self, eps: f64) {
        self.events_per_second.set(eps);
    }

    /// Increment flush counter
    pub fn inc_flush(&self) {
        self.flush_total.inc();
    }

    /// Record batch size for an emitter
    pub fn record_batch_size(&self, emitter: &str, size: usize) {
        self.batch_size
            .with_label_values(&[emitter])
            .observe(size as f64);
    }

    /// Record flush duration for an emitter
    pub fn record_flush_duration(&self, emitter: &str, seconds: f64) {
        self.flush_duration_seconds
            .with_label_values(&[emitter])
            .observe(seconds);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Buffer helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Record buffer overflow (message dropped due to full buffer)
    pub fn record_buffer_overflow(&self, count: u64) {
        self.buffer_overflow_total.inc_by(count as f64);
    }

    /// Record bytes saved by tiered buffer compression
    pub fn record_compression_savings(&self, bytes_saved: u64) {
        self.tiered_buffer_compressed_bytes
            .inc_by(bytes_saved as f64);
    }

    /// Update secondary tier buffer size
    pub fn set_tiered_secondary_size(&self, size: usize) {
        self.tiered_buffer_secondary_size.set(size as f64);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Emitter health helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Set emitter health status
    pub fn set_emitter_health(&self, emitter: &str, healthy: bool) {
        self.emitter_health
            .with_label_values(&[emitter])
            .set(if healthy { 1.0 } else { 0.0 });
    }

    /// Set circuit breaker state for an emitter
    ///
    /// States: 0 = Closed (normal), 1 = Open (rejecting), 2 = HalfOpen (testing)
    pub fn set_circuit_state(&self, emitter: &str, state: CircuitState) {
        self.circuit_breaker_state
            .with_label_values(&[emitter])
            .set(state.as_metric_value());
    }
}

/// Circuit breaker states for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests pass through
    Closed = 0,
    /// Circuit tripped - requests rejected
    Open = 1,
    /// Testing recovery - limited requests allowed
    HalfOpen = 2,
}

impl CircuitState {
    /// Convert to metric value
    pub fn as_metric_value(self) -> f64 {
        self as i32 as f64
    }
}

/// Gather all metrics and encode as Prometheus text format
///
/// Returns the metrics as a String, ready to be served via HTTP.
pub fn gather() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_ok() {
        String::from_utf8(buffer).unwrap_or_default()
    } else {
        String::new()
    }
}

/// Helper to record metrics if initialized, otherwise log warning
pub fn try_record_received(source: &str, event_type: &str, count: u64) {
    if let Some(m) = Metrics::get() {
        m.record_received(source, event_type, count);
    }
}

/// Helper to record metrics if initialized, otherwise skip
pub fn try_record_dropped(reason: &str, count: u64) {
    if let Some(m) = Metrics::get() {
        m.record_dropped(reason, count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_init() {
        // Metrics::init() may fail if already initialized from another test
        // so we just check get() works after any successful init
        let _ = Metrics::init();
        if let Some(metrics) = Metrics::get() {
            metrics.record_received("tapio", "network", 10);
            metrics.set_buffer_size(100);
        }
    }
}
