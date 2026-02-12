//! HTTP Webhook emitter for POLKU
//!
//! POSTs events as JSON to HTTP endpoints.
//!
//! # Example
//!
//! ```ignore
//! let emitter = WebhookEmitter::new("https://api.example.com/events")
//!     .header("Authorization", "Bearer token123");
//! registry.register_emitter(Arc::new(emitter));
//! ```

use crate::emit::Emitter;
use crate::error::PluginError;
use crate::message::Message;
use async_trait::async_trait;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error};

/// JSON payload sent to webhook
#[derive(Serialize)]
struct WebhookPayload {
    events: Vec<EventJson>,
}

/// Event serialized as JSON
#[derive(Serialize)]
struct EventJson {
    id: String,
    timestamp_unix_ns: i64,
    source: String,
    event_type: String,
    metadata: HashMap<String, String>,
    #[serde(with = "base64_bytes")]
    payload: Vec<u8>,
    route_to: Vec<String>,
}

/// Base64 encoding for binary payload (serialization only).
///
/// Intentionally one-way: webhooks emit JSON payloads but never receive them.
/// Receivers should decode the base64 payload on their end.
mod base64_bytes {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::Serializer;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }
}

impl From<&Message> for EventJson {
    fn from(m: &Message) -> Self {
        Self {
            id: m.id.to_string(),
            timestamp_unix_ns: m.timestamp,
            source: m.source.to_string(),
            event_type: m.message_type.to_string(),
            metadata: m.metadata().clone(),
            payload: m.payload.to_vec(),
            route_to: m.route_to.to_vec(),
        }
    }
}

/// HTTP Webhook emitter - POSTs events as JSON
pub struct WebhookEmitter {
    client: Client,
    url: String,
    health_url: Option<String>,
    headers: HashMap<String, String>,
}

/// Default request timeout in seconds
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Default connection timeout in seconds
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

impl WebhookEmitter {
    /// Create a new WebhookEmitter for the given URL
    ///
    /// Uses default timeouts: 30s request timeout, 10s connection timeout
    ///
    /// # Errors
    /// Returns `PluginError::Init` if the HTTP client cannot be created
    pub fn new(url: impl Into<String>) -> Result<Self, PluginError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
            .build()
            .map_err(|e| PluginError::Init(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            client,
            url: url.into(),
            health_url: None,
            headers: HashMap::new(),
        })
    }

    /// Set a custom health check URL
    ///
    /// By default, health checks use HEAD request to the emit URL.
    /// Use this to specify a dedicated health endpoint (e.g., "/health").
    pub fn health_url(mut self, url: impl Into<String>) -> Self {
        self.health_url = Some(url.into());
        self
    }

    /// Add a custom header to all requests
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }
}

#[async_trait]
impl Emitter for WebhookEmitter {
    fn name(&self) -> &'static str {
        "webhook"
    }

    async fn emit(&self, messages: &[Message]) -> Result<(), PluginError> {
        if messages.is_empty() {
            return Ok(());
        }

        let payload = WebhookPayload {
            events: messages.iter().map(EventJson::from).collect(),
        };

        let mut request = self.client.post(&self.url).json(&payload);

        for (name, value) in &self.headers {
            request = request.header(name, value);
        }

        match request.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    debug!(
                        url = %self.url,
                        count = messages.len(),
                        status = %response.status(),
                        "Webhook delivered"
                    );
                    Ok(())
                } else {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    let truncated_body: String = {
                        const MAX_CHARS: usize = 200;
                        if body.chars().count() > MAX_CHARS {
                            let prefix: String = body.chars().take(MAX_CHARS).collect();
                            let total_chars = body.chars().count();
                            format!("{prefix}...[truncated; {total_chars} chars total]")
                        } else {
                            body.clone()
                        }
                    };
                    error!(
                        url = %self.url,
                        status = %status,
                        body = %truncated_body,
                        "Webhook request failed"
                    );
                    Err(PluginError::Send(format!(
                        "Webhook returned {}: {}",
                        status, truncated_body
                    )))
                }
            }
            Err(e) => {
                error!(url = %self.url, error = %e, "Webhook connection failed");
                Err(PluginError::Connection(format!(
                    "Failed to connect to {}: {}",
                    self.url, e
                )))
            }
        }
    }

    async fn health(&self) -> bool {
        // Use custom health URL if configured, otherwise HEAD to emit URL
        let url = self.health_url.as_ref().unwrap_or(&self.url);

        // HEAD is less invasive than GET for POST-only endpoints
        match self.client.head(url).send().await {
            Ok(response) => {
                let status = response.status();
                // Treat successful responses as healthy, and allow 405 for POST-only endpoints
                let healthy = status.is_success() || status.as_u16() == 405;
                if !healthy {
                    debug!(
                        url = %url,
                        status = %status,
                        "Health check returned unhealthy status"
                    );
                }
                healthy
            }
            Err(e) => {
                debug!(url = %url, error = %e, "Health check failed");
                false
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::StatusCode,
        routing::{get, post},
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    /// Payload structure for webhook (matches Event fields)
    #[derive(Debug, Serialize, Deserialize)]
    struct WebhookPayload {
        events: Vec<EventJson>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct EventJson {
        id: String,
        source: String,
        event_type: String,
    }

    /// Shared state for mock server
    #[derive(Default)]
    struct MockServerState {
        received_events: Mutex<Vec<EventJson>>,
        request_count: AtomicUsize,
    }

    /// Start a mock HTTP server, returns its address
    async fn start_mock_server() -> (SocketAddr, Arc<MockServerState>) {
        let state = Arc::new(MockServerState::default());

        let app = Router::new()
            .route("/events", post(handle_events).head(handle_head))
            .route("/health", get(handle_health).head(handle_head))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        (addr, state)
    }

    async fn handle_events(
        State(state): State<Arc<MockServerState>>,
        Json(payload): Json<WebhookPayload>,
    ) -> StatusCode {
        state.request_count.fetch_add(1, Ordering::Relaxed);
        let mut events = state.received_events.lock().await;
        events.extend(payload.events);
        StatusCode::OK
    }

    async fn handle_health() -> StatusCode {
        StatusCode::OK
    }

    async fn handle_head() -> StatusCode {
        StatusCode::OK
    }

    fn make_message(id: &str, event_type: &str) -> Message {
        Message::with_id(
            id,
            1234567890,
            "test-source",
            event_type,
            bytes::Bytes::from_static(&[1, 2, 3]),
        )
    }

    #[tokio::test]
    async fn test_webhook_emitter_creates() {
        let emitter = WebhookEmitter::new("http://localhost:8080/events").unwrap();
        assert_eq!(emitter.name(), "webhook");
    }

    #[tokio::test]
    async fn test_webhook_emitter_emits_events() {
        let (addr, state) = start_mock_server().await;
        let url = format!("http://{}/events", addr);

        let emitter = WebhookEmitter::new(&url).unwrap();
        let events = vec![
            make_message("e1", "user.created"),
            make_message("e2", "user.updated"),
        ];

        let result = emitter.emit(&events).await;
        assert!(result.is_ok(), "Should emit events successfully");

        // Verify events were received
        // Note: MessageId::to_string() returns ULID encoding, not the original string
        let received = state.received_events.lock().await;
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].id, events[0].id.to_string());
        assert_eq!(received[1].id, events[1].id.to_string());
    }

    #[tokio::test]
    async fn test_webhook_emitter_health_check() {
        let (addr, _state) = start_mock_server().await;
        let url = format!("http://{}/events", addr);

        let emitter = WebhookEmitter::new(&url).unwrap();
        assert!(emitter.health().await, "Health check should pass");
    }

    #[tokio::test]
    async fn test_webhook_emitter_handles_empty_events() {
        let (addr, state) = start_mock_server().await;
        let url = format!("http://{}/events", addr);

        let emitter = WebhookEmitter::new(&url).unwrap();
        let result = emitter.emit(&[]).await;
        assert!(result.is_ok(), "Should handle empty event slice");

        // No request should have been made
        assert_eq!(state.request_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_webhook_emitter_with_custom_headers() {
        let (addr, _state) = start_mock_server().await;
        let url = format!("http://{}/events", addr);

        let emitter = WebhookEmitter::new(&url)
            .unwrap()
            .header("Authorization", "Bearer test-token")
            .header("X-Custom", "value");

        // Should create successfully with headers
        assert_eq!(emitter.name(), "webhook");
    }

    #[tokio::test]
    async fn test_webhook_emitter_custom_health_url() {
        let (addr, _state) = start_mock_server().await;
        let emit_url = format!("http://{}/events", addr);
        let health_url = format!("http://{}/health", addr);

        let emitter = WebhookEmitter::new(&emit_url)
            .unwrap()
            .health_url(&health_url);

        // Health check should use custom URL
        assert!(emitter.health().await, "Health check should pass");
    }

    #[tokio::test]
    async fn test_webhook_emitter_failure_on_bad_url() {
        let emitter = WebhookEmitter::new("http://127.0.0.1:1/events").unwrap();
        let events = vec![make_message("e1", "test")];

        let result = emitter.emit(&events).await;
        assert!(result.is_err(), "Should fail on connection error");
    }

    #[tokio::test]
    async fn test_webhook_emitter_connection_reset() {
        // Create a server that accepts connection but immediately drops it
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn a task that accepts connections then drops them (connection reset)
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                // Drop immediately - causes connection reset, not timeout
                drop(socket);
            }
        });

        let url = format!("http://{}/events", addr);
        let emitter = WebhookEmitter::new(&url).unwrap();
        let events = vec![make_message("e1", "test")];

        // Should fail fast on connection reset (not hang)
        let start = std::time::Instant::now();
        let result = emitter.emit(&events).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "Should fail on connection reset");
        // Connection reset should be fast, not a 30s timeout
        assert!(
            elapsed < Duration::from_secs(5),
            "Connection reset should be fast, took {:?}",
            elapsed
        );
    }
}
