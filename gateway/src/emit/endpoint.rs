//! Shared endpoint health tracking and channel construction
//!
//! Extracted from duplicated patterns in `grpc.rs` and `ahti/mod.rs`.
//! Both emitters use the same health-window algorithm (unhealthy_until + consecutive
//! failures) and the same gRPC channel construction chain.

use crate::error::PluginError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::debug;

/// Default connect timeout for gRPC endpoints
pub(crate) const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default request timeout for gRPC endpoints
pub(crate) const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Tracks endpoint health via a time-windowed unhealthy state.
///
/// After `failure_threshold` consecutive failures, the endpoint is marked
/// unhealthy for `unhealthy_duration_ms`. It becomes eligible again once
/// the wall-clock time exceeds the window.
///
/// Thread-safe — all fields are atomics.
pub(crate) struct HealthTracker {
    unhealthy_until: AtomicU64,
    consecutive_failures: AtomicU64,
    failure_threshold: u64,
    unhealthy_duration_ms: u64,
}

impl HealthTracker {
    /// Create a new health tracker
    ///
    /// # Arguments
    /// * `failure_threshold` - failures before marking unhealthy
    /// * `unhealthy_duration_ms` - how long (ms) to remain unhealthy
    pub(crate) fn new(failure_threshold: u64, unhealthy_duration_ms: u64) -> Self {
        Self {
            unhealthy_until: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            failure_threshold,
            unhealthy_duration_ms,
        }
    }

    /// Record a successful operation — resets failure count and health window
    pub(crate) fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.unhealthy_until.store(0, Ordering::SeqCst);
    }

    /// Record a failure, possibly marking the endpoint unhealthy
    ///
    /// Returns the new consecutive failure count.
    pub(crate) fn record_failure(&self) -> u64 {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= self.failure_threshold {
            let now = now_millis();
            self.unhealthy_until
                .store(now + self.unhealthy_duration_ms, Ordering::SeqCst);
        }
        failures
    }

    /// Check if the endpoint is currently healthy
    pub(crate) fn is_healthy(&self) -> bool {
        let unhealthy_until = self.unhealthy_until.load(Ordering::SeqCst);
        if unhealthy_until == 0 {
            return true;
        }
        now_millis() >= unhealthy_until
    }

    /// Get the current consecutive failure count
    #[cfg(test)]
    pub(crate) fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::SeqCst)
    }
}

/// Select the first untried index from an exclusion mask
///
/// Returns `None` if all entries are `true` (already tried).
pub(crate) fn select_any_untried(exclude: &[bool]) -> Option<usize> {
    for (idx, excluded) in exclude.iter().enumerate() {
        if !excluded {
            return Some(idx);
        }
    }
    None
}

/// Build gRPC channels for a list of endpoints
///
/// # Arguments
/// * `endpoints` - endpoint URLs (e.g. "http://host:50051")
/// * `lazy` - if true, uses `connect_lazy()` instead of `connect().await`
/// * `label` - short label for error messages (e.g. "AHTI", "gRPC")
pub(crate) async fn build_channels(
    endpoints: &[String],
    lazy: bool,
    label: &str,
) -> Result<Vec<Channel>, PluginError> {
    if endpoints.is_empty() {
        return Err(PluginError::Init(format!(
            "No {} endpoints provided",
            label
        )));
    }

    let mut channels = Vec::with_capacity(endpoints.len());

    for endpoint_str in endpoints {
        let ep = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| PluginError::Init(format!("Invalid {} endpoint URL: {}", label, e)))?
            .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS));

        let channel = if lazy {
            debug!(endpoint = %endpoint_str, "{} endpoint configured (lazy)", label);
            ep.connect_lazy()
        } else {
            let ch = ep.connect().await.map_err(|e| {
                PluginError::Connection(format!(
                    "Failed to connect to {} at {}: {}",
                    label, endpoint_str, e
                ))
            })?;
            debug!(endpoint = %endpoint_str, "{} endpoint connected", label);
            ch
        };

        channels.push(channel);
    }

    Ok(channels)
}

/// Current wall-clock time in milliseconds since UNIX epoch
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn healthy_until_threshold() {
        let tracker = HealthTracker::new(3, 5000);
        assert!(tracker.is_healthy());

        // First two failures — still healthy
        tracker.record_failure();
        tracker.record_failure();
        assert!(tracker.is_healthy());

        // Third failure — unhealthy for 5 seconds
        tracker.record_failure();
        assert!(!tracker.is_healthy());
    }

    #[test]
    fn success_resets_failures() {
        let tracker = HealthTracker::new(2, 60_000);
        tracker.record_failure();
        tracker.record_failure();
        assert!(!tracker.is_healthy());

        tracker.record_success();
        assert!(tracker.is_healthy());
        assert_eq!(tracker.consecutive_failures(), 0);
    }

    #[test]
    fn select_any_untried_finds_first_false() {
        assert_eq!(select_any_untried(&[true, true, false, true]), Some(2));
        assert_eq!(select_any_untried(&[false, true]), Some(0));
        assert_eq!(select_any_untried(&[true, true, true]), None);
        assert_eq!(select_any_untried(&[]), None);
    }
}
