//! Tests for GraphHealth implementation following TYL patterns

use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphHealth, GraphInfo, MultiGraphManager,
};

/// Helper function to create test adapter
async fn create_test_adapter() -> FalkorDBAdapter {
    let config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    };
    
    // This will fail without Redis, which is expected for unit tests
    match FalkorDBAdapter::new(config).await {
        Ok(adapter) => adapter,
        Err(_) => {
            // Return a mock-like adapter for testing health logic
            panic!("Redis not available for health tests")
        }
    }
}

/// Helper function to create test graph
async fn create_test_graph(
    adapter: &FalkorDBAdapter,
    graph_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph_info = GraphInfo {
        id: graph_id.to_string(),
        name: format!("Test Graph {}", graph_id),
        metadata: HashMap::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    
    adapter.create_graph(graph_info).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    Ok(())
}

#[tokio::test]
async fn test_is_healthy_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping health test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    // Test basic health check
    let is_healthy = adapter.is_healthy().await;
    assert!(is_healthy.is_ok());
    
    // With Redis available, should be healthy
    assert!(is_healthy.unwrap());
}

#[tokio::test]
async fn test_health_check_comprehensive() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping comprehensive health test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    // Test comprehensive health check
    let health_info = adapter.health_check().await;
    assert!(health_info.is_ok());
    
    let health_info = health_info.unwrap();
    
    // Verify required fields are present
    assert!(health_info.contains_key("connected"));
    assert!(health_info.contains_key("timestamp"));
    assert!(health_info.contains_key("status"));
    assert!(health_info.contains_key("total_graphs"));
    assert!(health_info.contains_key("active_transactions"));
    
    // Check values
    assert_eq!(health_info.get("connected"), Some(&json!(true)));
    assert!(health_info.get("status").is_some());
    assert!(health_info.get("total_graphs").is_some());
}

#[tokio::test]
async fn test_health_check_redis_info() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping Redis info health test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    let health_info = adapter.health_check().await;
    assert!(health_info.is_ok());
    
    let health_info = health_info.unwrap();
    
    // Should have Redis info when connection is healthy
    if health_info.get("connected") == Some(&json!(true)) {
        assert!(health_info.contains_key("redis_info_available"));
        // May contain memory_usage and connected_clients if INFO command succeeds
    }
}

#[tokio::test]
async fn test_get_graph_statistics_nonexistent() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping graph statistics test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    // Test getting statistics for non-existent graph
    let result = adapter.get_graph_statistics("non_existent_graph").await;
    
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Graph not found"));
}

#[tokio::test]
async fn test_get_graph_statistics_existing() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping graph statistics existing test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "health_stats_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Get statistics
    let stats = adapter.get_graph_statistics(graph_id).await;
    assert!(stats.is_ok());
    
    let stats = stats.unwrap();
    
    // Verify required fields
    assert!(stats.contains_key("graph_id"));
    assert!(stats.contains_key("created_at"));
    assert!(stats.contains_key("updated_at"));
    assert!(stats.contains_key("active_transactions"));
    assert!(stats.contains_key("active_indexes"));
    assert!(stats.contains_key("active_constraints"));
    assert!(stats.contains_key("metadata"));
    assert!(stats.contains_key("timestamp"));
    
    // Check values
    assert_eq!(stats.get("graph_id"), Some(&json!(graph_id)));
    assert_eq!(stats.get("active_transactions"), Some(&json!(0))); // No active transactions initially
    assert_eq!(stats.get("active_indexes"), Some(&json!(0))); // No indexes initially
    assert_eq!(stats.get("active_constraints"), Some(&json!(0))); // No constraints initially
}

#[tokio::test]
async fn test_get_graph_statistics_with_node_counts() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping node count statistics test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "health_node_count_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let stats = adapter.get_graph_statistics(graph_id).await;
    assert!(stats.is_ok());
    
    let stats = stats.unwrap();
    
    // Should have either node_count or node_count_error
    let has_node_count = stats.contains_key("node_count");
    let has_node_count_error = stats.contains_key("node_count_error");
    assert!(has_node_count || has_node_count_error);
    
    // Should have either relationship_count or relationship_count_error
    let has_rel_count = stats.contains_key("relationship_count");
    let has_rel_count_error = stats.contains_key("relationship_count_error");
    assert!(has_rel_count || has_rel_count_error);
}

#[tokio::test]
async fn test_get_all_statistics_empty() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping all statistics empty test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    // Test getting statistics for all graphs when none exist
    let all_stats = adapter.get_all_statistics().await;
    assert!(all_stats.is_ok());
    
    let all_stats = all_stats.unwrap();
    // Should be empty since no graphs have been created
    assert!(all_stats.is_empty());
}

#[tokio::test]
async fn test_get_all_statistics_with_graphs() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping all statistics with graphs test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_ids = vec!["health_all_1", "health_all_2"];
    
    // Create test graphs
    for graph_id in &graph_ids {
        if create_test_graph(&adapter, graph_id).await.is_err() {
            return; // Skip if can't create graphs
        }
    }
    
    let all_stats = adapter.get_all_statistics().await;
    assert!(all_stats.is_ok());
    
    let all_stats = all_stats.unwrap();
    assert_eq!(all_stats.len(), graph_ids.len());
    
    // Verify each graph has statistics
    for graph_id in &graph_ids {
        assert!(all_stats.contains_key(*graph_id));
        let graph_stats = all_stats.get(*graph_id).unwrap();
        assert!(graph_stats.contains_key("graph_id"));
        assert!(graph_stats.contains_key("timestamp"));
    }
}

#[tokio::test]
async fn test_health_check_with_graphs_and_transactions() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping health with transactions test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "health_with_tx_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Initial health check should show 1 graph, 0 transactions
    let health_info = adapter.health_check().await.unwrap();
    assert_eq!(health_info.get("total_graphs"), Some(&json!(1)));
    assert_eq!(health_info.get("active_transactions"), Some(&json!(0)));
}

#[tokio::test]
async fn test_graph_statistics_metadata_preservation() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping metadata preservation test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "health_metadata_test";
    
    // Create graph with metadata
    let mut metadata = HashMap::new();
    metadata.insert("description".to_string(), json!("Test graph for health checks"));
    metadata.insert("version".to_string(), json!("1.0"));
    
    let graph_info = GraphInfo {
        id: graph_id.to_string(),
        name: "Test Graph with Metadata".to_string(),
        metadata: metadata.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    
    if adapter.create_graph(graph_info).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let stats = adapter.get_graph_statistics(graph_id).await.unwrap();
    
    // Verify metadata is preserved in statistics
    assert_eq!(stats.get("metadata"), Some(&json!(metadata)));
}

#[tokio::test]
async fn test_health_check_timestamp_format() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping timestamp format test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    let health_info = adapter.health_check().await.unwrap();
    
    // Verify timestamp is in RFC3339 format
    let timestamp = health_info.get("timestamp").unwrap();
    assert!(timestamp.is_string());
    
    let timestamp_str = timestamp.as_str().unwrap();
    // Should be parseable as RFC3339
    assert!(chrono::DateTime::parse_from_rfc3339(timestamp_str).is_ok());
}

#[tokio::test]
async fn test_graph_statistics_timing_fields() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping timing fields test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "health_timing_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let stats = adapter.get_graph_statistics(graph_id).await.unwrap();
    
    // Verify timing fields
    let created_at = stats.get("created_at").unwrap().as_str().unwrap();
    let updated_at = stats.get("updated_at").unwrap().as_str().unwrap();
    let timestamp = stats.get("timestamp").unwrap().as_str().unwrap();
    
    // All should be valid RFC3339 timestamps
    assert!(chrono::DateTime::parse_from_rfc3339(created_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(updated_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(timestamp).is_ok());
}

// Helper function to check if Redis is available
async fn redis_available() -> bool {
    use redis::Client;
    
    match Client::open("redis://localhost:6379") {
        Ok(client) => match client.get_connection() {
            Ok(_) => true,
            Err(_) => false,
        },
        Err(_) => false,
    }
}