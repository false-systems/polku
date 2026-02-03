//! Discovery Server Integration Tests
//!
//! Tests for the plugin discovery/registration protocol.
//! These test the actual DiscoveryServer implementation.

use polku_gateway::discovery::DiscoveryServer;
use polku_gateway::proto::plugin_registry_client::PluginRegistryClient;
use polku_gateway::proto::plugin_registry_server::PluginRegistryServer;
use polku_gateway::proto::{
    HeartbeatRequest, PluginInfo, PluginType, RegisterRequest, UnregisterRequest,
};
use polku_gateway::registry::PluginRegistry as StaticRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

// =============================================================================
// TEST INFRASTRUCTURE
// =============================================================================

struct DiscoveryTestHandle {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
}

async fn start_discovery_server() -> DiscoveryTestHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let static_registry = Arc::new(parking_lot::RwLock::new(StaticRegistry::new()));
    let discovery = DiscoveryServer::new(static_registry);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PluginRegistryServer::new(discovery))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    DiscoveryTestHandle {
        addr,
        shutdown: shutdown_tx,
    }
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

// =============================================================================
// DISCOVERY TESTS
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_discovery_accepts_unreachable_plugin() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    // Register a plugin at an address that doesn't exist
    // Discovery doesn't validate plugin reachability - it just stores the address
    let resp = client
        .register(RegisterRequest {
            info: Some(make_ingestor_info("ghost-plugin", vec!["ghost-source"])),
            address: "http://127.0.0.1:39999".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.accepted);
    assert!(!resp.plugin_id.is_empty());

    // Heartbeat should work (plugin is registered)
    let hb = client
        .heartbeat(HeartbeatRequest {
            plugin_id: resp.plugin_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(hb.acknowledged);

    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_concurrent_registrations() {
    let handle = start_discovery_server().await;
    let addr = handle.addr;

    let mut task_handles = vec![];

    // Spawn 20 concurrent registrations
    for i in 0..20 {
        let addr = addr;
        task_handles.push(tokio::spawn(async move {
            let mut client = PluginRegistryClient::connect(format!("http://{}", addr))
                .await
                .unwrap();

            client
                .register(RegisterRequest {
                    info: Some(make_emitter_info(
                        &format!("plugin-{}", i),
                        &format!("emitter-{}", i),
                    )),
                    address: format!("http://127.0.0.1:{}", 10000 + i),
                })
                .await
        }));
    }

    let results: Vec<_> = futures::future::join_all(task_handles).await;

    let mut success_count = 0;
    for (i, result) in results.into_iter().enumerate() {
        let resp = result
            .expect("Task panicked")
            .unwrap_or_else(|_| panic!("Registration {} failed", i));
        if resp.into_inner().accepted {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 20);
    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_plugin_reregisters_new_address() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    // First registration
    let resp1 = client
        .register(RegisterRequest {
            info: Some(make_emitter_info("my-emitter", "slack")),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let id1 = resp1.plugin_id;
    assert!(resp1.accepted);

    // Second registration with same name, different address
    let resp2 = client
        .register(RegisterRequest {
            info: Some(make_emitter_info("my-emitter", "slack")),
            address: "http://127.0.0.1:9002".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let id2 = resp2.plugin_id;
    assert!(resp2.accepted);

    // Both IDs should be different (each registration gets unique ID)
    assert_ne!(id1, id2);

    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_heartbeat_stale_id() {
    let handle = start_discovery_server().await;
    let addr = handle.addr;

    let mut client = PluginRegistryClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Register plugin
    let resp = client
        .register(RegisterRequest {
            info: Some(make_ingestor_info("my-plugin", vec!["source"])),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let plugin_id = resp.plugin_id;

    // "Restart" discovery by shutting down and starting new one on same port
    drop(handle.shutdown);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start new server on same port
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let static_registry = Arc::new(parking_lot::RwLock::new(StaticRegistry::new()));
    let new_discovery = DiscoveryServer::new(static_registry);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PluginRegistryServer::new(new_discovery))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect client
    let mut client = PluginRegistryClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Heartbeat with old ID - should return acknowledged: false (not error)
    let heartbeat_resp = client
        .heartbeat(HeartbeatRequest {
            plugin_id: plugin_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(
        !heartbeat_resp.acknowledged,
        "Stale ID should not be acknowledged"
    );

    // Plugin should re-register to recover
    let rereg_resp = client
        .register(RegisterRequest {
            info: Some(make_ingestor_info("my-plugin", vec!["source"])),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(rereg_resp.accepted);
    assert_ne!(rereg_resp.plugin_id, plugin_id, "Should get new ID");
}

#[tokio::test]
async fn test_discovery_heartbeat_flood() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    let resp = client
        .register(RegisterRequest {
            info: Some(make_ingestor_info("heartbeat-plugin", vec!["source"])),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let plugin_id = resp.plugin_id;

    // Send 100 heartbeats rapidly
    for _ in 0..100 {
        let resp = client
            .heartbeat(HeartbeatRequest {
                plugin_id: plugin_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);
    }

    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_unregister_then_heartbeat() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    let resp = client
        .register(RegisterRequest {
            info: Some(make_emitter_info("temp-plugin", "temp")),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let plugin_id = resp.plugin_id;

    // Unregister
    client
        .unregister(UnregisterRequest {
            plugin_id: plugin_id.clone(),
        })
        .await
        .unwrap();

    // Heartbeat should fail (not acknowledged, not error)
    let heartbeat_resp = client
        .heartbeat(HeartbeatRequest { plugin_id })
        .await
        .unwrap()
        .into_inner();

    assert!(!heartbeat_resp.acknowledged);

    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_double_unregister() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    let resp = client
        .register(RegisterRequest {
            info: Some(make_emitter_info("double-unreg", "emitter")),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let plugin_id = resp.plugin_id;

    // First unregister
    client
        .unregister(UnregisterRequest {
            plugin_id: plugin_id.clone(),
        })
        .await
        .unwrap();

    // Second unregister - should be idempotent
    let result = client.unregister(UnregisterRequest { plugin_id }).await;

    assert!(result.is_ok(), "Double unregister should be idempotent");

    drop(handle.shutdown);
}

#[tokio::test]
async fn test_discovery_invalid_plugin_type() {
    let handle = start_discovery_server().await;

    let mut client = PluginRegistryClient::connect(format!("http://{}", handle.addr))
        .await
        .unwrap();

    // Register with invalid type (99)
    let mut info = make_emitter_info("bad-type", "emitter");
    info.r#type = 99;

    let result = client
        .register(RegisterRequest {
            info: Some(info),
            address: "http://127.0.0.1:9001".to_string(),
        })
        .await;

    assert!(result.is_err(), "Invalid plugin type should be rejected");

    drop(handle.shutdown);
}
