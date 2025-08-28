//! Integration tests that require a running FalkorDB instance

use std::collections::HashMap;

use serde_json::json;
use tyl_config::RedisConfig;
use tyl_db_core::DatabaseLifecycle;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphInfo, GraphNode, GraphRelationship, GraphStore, MultiGraphManager,
};

// Helper function to check if FalkorDB is available
async fn is_falkordb_available() -> bool {
    let config = get_test_config();
    match FalkorDBAdapter::new(config).await {
        Ok(adapter) => match adapter.health_check().await {
            Ok(health_result) => health_result.status.is_healthy(),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

// Helper function to get test configuration
fn get_test_config() -> RedisConfig {
    RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    }
}

#[tokio::test]
async fn test_full_graph_operations() {
    // Skip test if FalkorDB is not available
    if !is_falkordb_available().await {
        println!("Skipping integration test - FalkorDB not available");
        return;
    }

    let config = get_test_config();
    let adapter = FalkorDBAdapter::new(config)
        .await
        .expect("Failed to create adapter");

    // Create test graph
    let graph_info = GraphInfo {
        id: "integration_test".to_string(),
        name: "Integration Test Graph".to_string(),
        metadata: HashMap::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    let _ = adapter.create_graph(graph_info).await; // Ignore result in case already exists

    // Test node creation
    let mut user_node = GraphNode::new();
    user_node.id = "user_integration_test".to_string();
    user_node.labels.push("User".to_string());
    user_node
        .properties
        .insert("name".to_string(), json!("Test User"));
    user_node
        .properties
        .insert("email".to_string(), json!("test@example.com"));

    let node_id = adapter
        .create_node("integration_test", user_node.clone())
        .await
        .expect("Failed to create node");

    assert_eq!(node_id, "user_integration_test");

    // Test node retrieval
    let retrieved_node = adapter
        .get_node("integration_test", &node_id)
        .await
        .expect("Failed to retrieve node");

    assert!(retrieved_node.is_some());

    // Test creating another node for relationship
    let mut product_node = GraphNode::new();
    product_node.id = "product_integration_test".to_string();
    product_node.labels.push("Product".to_string());
    product_node
        .properties
        .insert("name".to_string(), json!("Test Product"));

    let product_id = adapter
        .create_node("integration_test", product_node)
        .await
        .expect("Failed to create product node");

    // Test relationship creation
    let mut relationship = GraphRelationship::new(
        "purchase_integration_test".to_string(),
        node_id.clone(),
        product_id.clone(),
    );
    relationship.relationship_type = "PURCHASED".to_string();
    relationship
        .properties
        .insert("date".to_string(), json!("2024-01-15"));
    relationship
        .properties
        .insert("amount".to_string(), json!(99.99));

    let rel_id = adapter
        .create_relationship("integration_test", relationship)
        .await
        .expect("Failed to create relationship");

    // FalkorDB adapter generates UUID for relationship IDs, so verify it's a valid UUID
    assert!(!rel_id.is_empty());
    assert!(rel_id.len() > 10); // UUID should be much longer than 10 chars
    println!("Created relationship with ID: {}", rel_id);
}

#[tokio::test]
async fn test_database_lifecycle_integration() {
    // Skip test if FalkorDB is not available
    if !is_falkordb_available().await {
        println!("Skipping database lifecycle test - FalkorDB not available");
        return;
    }

    let redis_config = get_test_config();

    // Test connect
    let adapter = FalkorDBAdapter::connect(redis_config)
        .await
        .expect("Failed to connect via DatabaseLifecycle");

    // Test health check using DatabaseLifecycle trait
    let health_result = DatabaseLifecycle::health_check(&adapter)
        .await
        .expect("Failed to perform health check");

    assert!(health_result.status.is_healthy());

    // Test connection info
    let info = adapter.connection_info();
    assert!(info.contains("FalkorDB"));
}

#[tokio::test]
async fn test_cypher_query_execution() {
    // Skip test if FalkorDB is not available
    if !is_falkordb_available().await {
        println!("Skipping Cypher test - FalkorDB not available");
        return;
    }

    let config = get_test_config();
    let adapter = FalkorDBAdapter::new(config)
        .await
        .expect("Failed to create adapter");

    // Test simple Cypher query
    let result = adapter
        .execute_cypher("cypher_test", "RETURN 1 as number")
        .await
        .expect("Failed to execute Cypher query");

    // Verify we got a result
    assert!(!result.is_null());
}

#[tokio::test]
async fn test_error_handling_with_invalid_queries() {
    // Skip test if FalkorDB is not available
    if !is_falkordb_available().await {
        println!("Skipping error handling test - FalkorDB not available");
        return;
    }

    let config = get_test_config();
    let adapter = FalkorDBAdapter::new(config)
        .await
        .expect("Failed to create adapter");

    // Test invalid Cypher query
    let result = adapter
        .execute_cypher("error_test", "INVALID CYPHER SYNTAX")
        .await;

    // Should return an error
    assert!(result.is_err());

    if let Err(error) = result {
        assert!(error.to_string().contains("failed"));
    }
}
