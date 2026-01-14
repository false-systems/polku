//! Plugin Registry gRPC server implementation
//!
//! Implements the PluginRegistry service from plugin.proto.
//! Plugins call these methods to register themselves with POLKU.

use crate::emit::ExternalEmitter;
use crate::ingest::ExternalIngestor;
use crate::proto::plugin_registry_server::PluginRegistry;
use crate::proto::{
    HeartbeatRequest, HeartbeatResponse, PluginInfo, PluginType, RegisterRequest,
    RegisterResponse, UnregisterRequest,
};
use crate::registry::PluginRegistry as StaticRegistry;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// Information about a registered plugin
#[derive(Debug, Clone)]
pub struct RegisteredPlugin {
    /// Unique plugin ID (ULID)
    pub id: String,
    /// Plugin metadata
    pub info: PluginInfo,
    /// Address where POLKU can reach the plugin
    pub address: String,
    /// When the plugin registered
    pub registered_at: Instant,
    /// Last heartbeat timestamp
    pub last_heartbeat: Instant,
}

/// Discovery server for dynamic plugin registration
///
/// Implements the `PluginRegistry` gRPC service, allowing plugins
/// to register themselves at runtime instead of being statically configured.
pub struct DiscoveryServer {
    /// Registered plugins by ID
    plugins: RwLock<HashMap<String, RegisteredPlugin>>,
    /// Static registry to add ingestors/emitters to
    registry: Arc<RwLock<StaticRegistry>>,
}

impl DiscoveryServer {
    /// Create a new discovery server
    pub fn new(registry: Arc<RwLock<StaticRegistry>>) -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            registry,
        }
    }

    /// Generate a unique plugin ID
    fn generate_plugin_id() -> String {
        ulid::Ulid::new().to_string()
    }

    /// Get all registered plugins
    pub fn all_plugins(&self) -> Vec<RegisteredPlugin> {
        self.plugins.read().values().cloned().collect()
    }

    /// Get a plugin by ID
    pub fn get_plugin(&self, id: &str) -> Option<RegisteredPlugin> {
        self.plugins.read().get(id).cloned()
    }

    /// Get plugin count
    pub fn plugin_count(&self) -> usize {
        self.plugins.read().len()
    }

    /// Check if a plugin is registered
    pub fn has_plugin(&self, id: &str) -> bool {
        self.plugins.read().contains_key(id)
    }
}

#[tonic::async_trait]
impl PluginRegistry for DiscoveryServer {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let info = req.info.ok_or_else(|| Status::invalid_argument("Missing plugin info"))?;
        let address = req.address;

        // Validate plugin info
        if info.name.is_empty() {
            return Err(Status::invalid_argument("Plugin name is required"));
        }
        if address.is_empty() {
            return Err(Status::invalid_argument("Plugin address is required"));
        }

        let plugin_type = PluginType::try_from(info.r#type)
            .unwrap_or(PluginType::Unspecified);

        // Generate unique ID
        let plugin_id = Self::generate_plugin_id();
        let now = Instant::now();

        // Create the appropriate external plugin based on type
        match plugin_type {
            PluginType::Ingestor => {
                // Validate ingestor has sources
                if info.sources.is_empty() {
                    return Err(Status::invalid_argument(
                        "Ingestor plugin must declare at least one source",
                    ));
                }

                // Create ExternalIngestor for each source
                for source in &info.sources {
                    let ingestor = ExternalIngestor::new(source.clone(), address.clone());
                    self.registry.write().add_ingestor(Arc::new(ingestor));
                }

                info!(
                    plugin_id = %plugin_id,
                    name = %info.name,
                    sources = ?info.sources,
                    address = %address,
                    "Registered ingestor plugin"
                );
            }
            PluginType::Emitter => {
                // Validate emitter has a name
                let emitter_name = if info.emitter_name.is_empty() {
                    &info.name
                } else {
                    &info.emitter_name
                };

                let emitter = ExternalEmitter::new(emitter_name.clone(), address.clone());
                self.registry.write().register_emitter(Arc::new(emitter));

                info!(
                    plugin_id = %plugin_id,
                    name = %info.name,
                    emitter_name = %emitter_name,
                    address = %address,
                    "Registered emitter plugin"
                );
            }
            PluginType::Unspecified => {
                return Err(Status::invalid_argument(
                    "Plugin type must be INGESTOR or EMITTER",
                ));
            }
        }

        // Store the registered plugin
        let registered = RegisteredPlugin {
            id: plugin_id.clone(),
            info,
            address,
            registered_at: now,
            last_heartbeat: now,
        };

        self.plugins.write().insert(plugin_id.clone(), registered);

        Ok(Response::new(RegisterResponse {
            accepted: true,
            message: "Plugin registered successfully".to_string(),
            plugin_id,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let plugin_id = req.plugin_id;

        if plugin_id.is_empty() {
            return Err(Status::invalid_argument("Plugin ID is required"));
        }

        let mut plugins = self.plugins.write();

        if let Some(plugin) = plugins.get_mut(&plugin_id) {
            plugin.last_heartbeat = Instant::now();

            Ok(Response::new(HeartbeatResponse { acknowledged: true }))
        } else {
            warn!(plugin_id = %plugin_id, "Heartbeat for unknown plugin");
            Ok(Response::new(HeartbeatResponse { acknowledged: false }))
        }
    }

    async fn unregister(
        &self,
        request: Request<UnregisterRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let plugin_id = req.plugin_id;

        if plugin_id.is_empty() {
            return Err(Status::invalid_argument("Plugin ID is required"));
        }

        let removed = self.plugins.write().remove(&plugin_id);

        if let Some(plugin) = removed {
            info!(
                plugin_id = %plugin_id,
                name = %plugin.info.name,
                "Unregistered plugin"
            );
            // Note: We don't remove from the static registry because:
            // 1. There's no remove API on PluginRegistry
            // 2. The external plugin client will fail on next call anyway
        } else {
            warn!(plugin_id = %plugin_id, "Unregister for unknown plugin");
        }

        Ok(Response::new(()))
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn create_test_server() -> DiscoveryServer {
        let registry = Arc::new(RwLock::new(StaticRegistry::new()));
        DiscoveryServer::new(registry)
    }

    fn make_ingestor_info(name: &str, sources: Vec<&str>) -> PluginInfo {
        PluginInfo {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            r#type: PluginType::Ingestor as i32,
            description: "Test ingestor".to_string(),
            sources: sources.into_iter().map(|s| s.to_string()).collect(),
            emitter_name: String::new(),
            capabilities: vec![],
        }
    }

    fn make_emitter_info(name: &str, emitter_name: &str) -> PluginInfo {
        PluginInfo {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            r#type: PluginType::Emitter as i32,
            description: "Test emitter".to_string(),
            sources: vec![],
            emitter_name: emitter_name.to_string(),
            capabilities: vec![],
        }
    }

    // =========================================================================
    // Registration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_register_ingestor_plugin() {
        let server = create_test_server();

        let request = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-ingestor", vec!["source-a", "source-b"])),
            address: "http://localhost:9001".to_string(),
        });

        let response = server.register(request).await.unwrap();
        let resp = response.into_inner();

        assert!(resp.accepted);
        assert!(!resp.plugin_id.is_empty());
        assert!(server.has_plugin(&resp.plugin_id));
    }

    #[tokio::test]
    async fn test_register_emitter_plugin() {
        let server = create_test_server();

        let request = Request::new(RegisterRequest {
            info: Some(make_emitter_info("my-emitter", "splunk")),
            address: "http://localhost:9002".to_string(),
        });

        let response = server.register(request).await.unwrap();
        let resp = response.into_inner();

        assert!(resp.accepted);
        assert!(!resp.plugin_id.is_empty());
    }

    #[tokio::test]
    async fn test_register_missing_info() {
        let server = create_test_server();

        let request = Request::new(RegisterRequest {
            info: None,
            address: "http://localhost:9001".to_string(),
        });

        let result = server.register(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_register_empty_name() {
        let server = create_test_server();

        let mut info = make_ingestor_info("", vec!["source"]);
        info.name = String::new();

        let request = Request::new(RegisterRequest {
            info: Some(info),
            address: "http://localhost:9001".to_string(),
        });

        let result = server.register(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_empty_address() {
        let server = create_test_server();

        let request = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-ingestor", vec!["source"])),
            address: String::new(),
        });

        let result = server.register(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_ingestor_no_sources() {
        let server = create_test_server();

        let request = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-ingestor", vec![])),
            address: "http://localhost:9001".to_string(),
        });

        let result = server.register(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_unspecified_type() {
        let server = create_test_server();

        let mut info = make_ingestor_info("my-plugin", vec!["source"]);
        info.r#type = PluginType::Unspecified as i32;

        let request = Request::new(RegisterRequest {
            info: Some(info),
            address: "http://localhost:9001".to_string(),
        });

        let result = server.register(request).await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Heartbeat Tests
    // =========================================================================

    #[tokio::test]
    async fn test_heartbeat_success() {
        let server = create_test_server();

        // First register
        let register_req = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-ingestor", vec!["source"])),
            address: "http://localhost:9001".to_string(),
        });
        let register_resp = server.register(register_req).await.unwrap().into_inner();
        let plugin_id = register_resp.plugin_id;

        // Then heartbeat
        let heartbeat_req = Request::new(HeartbeatRequest {
            plugin_id: plugin_id.clone(),
        });
        let heartbeat_resp = server.heartbeat(heartbeat_req).await.unwrap().into_inner();

        assert!(heartbeat_resp.acknowledged);
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_plugin() {
        let server = create_test_server();

        let request = Request::new(HeartbeatRequest {
            plugin_id: "unknown-id".to_string(),
        });

        let response = server.heartbeat(request).await.unwrap().into_inner();
        assert!(!response.acknowledged);
    }

    #[tokio::test]
    async fn test_heartbeat_empty_id() {
        let server = create_test_server();

        let request = Request::new(HeartbeatRequest {
            plugin_id: String::new(),
        });

        let result = server.heartbeat(request).await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Unregister Tests
    // =========================================================================

    #[tokio::test]
    async fn test_unregister_success() {
        let server = create_test_server();

        // First register
        let register_req = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-ingestor", vec!["source"])),
            address: "http://localhost:9001".to_string(),
        });
        let register_resp = server.register(register_req).await.unwrap().into_inner();
        let plugin_id = register_resp.plugin_id;

        assert!(server.has_plugin(&plugin_id));

        // Then unregister
        let unregister_req = Request::new(UnregisterRequest {
            plugin_id: plugin_id.clone(),
        });
        let result = server.unregister(unregister_req).await;

        assert!(result.is_ok());
        assert!(!server.has_plugin(&plugin_id));
    }

    #[tokio::test]
    async fn test_unregister_unknown_plugin() {
        let server = create_test_server();

        let request = Request::new(UnregisterRequest {
            plugin_id: "unknown-id".to_string(),
        });

        // Should succeed (idempotent) but not error
        let result = server.unregister(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_unregister_empty_id() {
        let server = create_test_server();

        let request = Request::new(UnregisterRequest {
            plugin_id: String::new(),
        });

        let result = server.unregister(request).await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Plugin Count and Listing Tests
    // =========================================================================

    #[tokio::test]
    async fn test_plugin_count() {
        let server = create_test_server();

        assert_eq!(server.plugin_count(), 0);

        // Register first plugin
        let req1 = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("plugin-1", vec!["source-1"])),
            address: "http://localhost:9001".to_string(),
        });
        server.register(req1).await.unwrap();

        assert_eq!(server.plugin_count(), 1);

        // Register second plugin
        let req2 = Request::new(RegisterRequest {
            info: Some(make_emitter_info("plugin-2", "emitter")),
            address: "http://localhost:9002".to_string(),
        });
        server.register(req2).await.unwrap();

        assert_eq!(server.plugin_count(), 2);
    }

    #[tokio::test]
    async fn test_all_plugins() {
        let server = create_test_server();

        let req1 = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("plugin-1", vec!["source"])),
            address: "http://localhost:9001".to_string(),
        });
        server.register(req1).await.unwrap();

        let req2 = Request::new(RegisterRequest {
            info: Some(make_emitter_info("plugin-2", "emitter")),
            address: "http://localhost:9002".to_string(),
        });
        server.register(req2).await.unwrap();

        let plugins = server.all_plugins();
        assert_eq!(plugins.len(), 2);

        let names: Vec<_> = plugins.iter().map(|p| p.info.name.as_str()).collect();
        assert!(names.contains(&"plugin-1"));
        assert!(names.contains(&"plugin-2"));
    }

    #[tokio::test]
    async fn test_get_plugin() {
        let server = create_test_server();

        let req = Request::new(RegisterRequest {
            info: Some(make_ingestor_info("my-plugin", vec!["source"])),
            address: "http://localhost:9001".to_string(),
        });
        let resp = server.register(req).await.unwrap().into_inner();
        let plugin_id = resp.plugin_id;

        let plugin = server.get_plugin(&plugin_id);
        assert!(plugin.is_some());

        let plugin = plugin.unwrap();
        assert_eq!(plugin.info.name, "my-plugin");
        assert_eq!(plugin.address, "http://localhost:9001");
    }

    // =========================================================================
    // Plugin ID Tests
    // =========================================================================

    #[test]
    fn test_generate_plugin_id_is_unique() {
        let id1 = DiscoveryServer::generate_plugin_id();
        let id2 = DiscoveryServer::generate_plugin_id();

        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
    }

    #[test]
    fn test_generate_plugin_id_is_ulid() {
        let id = DiscoveryServer::generate_plugin_id();

        // ULIDs are 26 characters
        assert_eq!(id.len(), 26);

        // Should be parseable as ULID
        let parsed = ulid::Ulid::from_string(&id);
        assert!(parsed.is_ok());
    }
}
