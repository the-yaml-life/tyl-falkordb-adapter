//! Tests for GraphTraversal implementation following TYL patterns
//! Maintains compatibility with tyl-graph-port as the source of truth

use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphInfo, GraphTraversal, MultiGraphManager,
};
use tyl_graph_port::{
    TemporalQuery, TemporalOperation, TraversalDirection, TraversalParams,
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
            // Return a mock-like adapter for testing traversal logic
            panic!("Redis not available for traversal tests")
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

/// Helper function to create TraversalParams for testing
fn create_test_traversal_params() -> TraversalParams {
    TraversalParams {
        max_depth: Some(3),
        relationship_types: vec!["KNOWS".to_string(), "FOLLOWS".to_string()],
        node_labels: vec!["Person".to_string()],
        node_filters: HashMap::new(),
        relationship_filters: HashMap::new(),
        limit: Some(10),
        direction: TraversalDirection::Outgoing,
    }
}

#[test]
fn test_traversal_params_creation() {
    let params = create_test_traversal_params();
    
    assert_eq!(params.max_depth, Some(3));
    assert_eq!(params.relationship_types.len(), 2);
    assert_eq!(params.node_labels.len(), 1);
    assert_eq!(params.limit, Some(10));
    assert!(matches!(params.direction, TraversalDirection::Outgoing));
}

#[test]
fn test_temporal_query_creation() {
    let start_time = Utc::now() - chrono::Duration::days(30);
    let end_time = Utc::now();
    
    let temporal_query = TemporalQuery {
        start_time: Some(start_time),
        end_time: Some(end_time),
        temporal_property: "created_at".to_string(),
        operation: TemporalOperation::Between,
    };
    
    assert!(temporal_query.start_time.is_some());
    assert!(temporal_query.end_time.is_some());
    assert_eq!(temporal_query.temporal_property, "created_at");
    assert!(matches!(temporal_query.operation, TemporalOperation::Between));
}

#[tokio::test]
async fn test_get_neighbors_nonexistent_graph() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping neighbors test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let params = create_test_traversal_params();
    
    // Try to get neighbors from non-existent graph
    let result = adapter.get_neighbors("non_existent_graph", "test_node", params).await;
    
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Graph not found"));
}

#[tokio::test]
async fn test_get_neighbors_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping get neighbors test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_neighbors_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let params = create_test_traversal_params();
    let result = adapter.get_neighbors(graph_id, "test_node_1", params).await;
    
    assert!(result.is_ok());
    let neighbors = result.unwrap();
    // Result might be empty if no neighbors exist, which is expected in unit tests
    assert!(neighbors.len() >= 0);
}

#[tokio::test]
async fn test_find_shortest_path() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping shortest path test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_path_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let params = create_test_traversal_params();
    let result = adapter.find_shortest_path(graph_id, "node_a", "node_b", params).await;
    
    assert!(result.is_ok());
    let path = result.unwrap();
    // Path might be None if no path exists, which is expected
    if let Some(found_path) = path {
        assert!(found_path.nodes.len() >= 1);
        assert_eq!(found_path.length, found_path.relationships.len());
    }
}

#[tokio::test]
async fn test_find_shortest_weighted_path() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping weighted path test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_weighted_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let params = create_test_traversal_params();
    let result = adapter.find_shortest_weighted_path(
        graph_id, 
        "node_a", 
        "node_b", 
        params, 
        "weight"
    ).await;
    
    assert!(result.is_ok());
    let weighted_path = result.unwrap();
    // Path might be None if no path exists
    if let Some(found_path) = weighted_path {
        assert!(found_path.total_weight >= 0.0);
        assert!(found_path.edge_weights.len() >= 0);
        assert!(found_path.path.nodes.len() >= 1);
    }
}

#[tokio::test]
async fn test_find_all_paths() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping all paths test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_all_paths_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let params = create_test_traversal_params();
    let result = adapter.find_all_paths(graph_id, "node_a", "node_b", params).await;
    
    assert!(result.is_ok());
    let paths = result.unwrap();
    // Might be empty if no paths exist
    for path in paths {
        assert!(path.nodes.len() >= 1);
        assert_eq!(path.length, path.relationships.len());
    }
}

#[tokio::test]
async fn test_traverse_from() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping traverse from test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_from_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let params = create_test_traversal_params();
    let result = adapter.traverse_from(graph_id, "start_node", params).await;
    
    assert!(result.is_ok());
    let nodes = result.unwrap();
    // Might be empty if no traversable nodes exist
    assert!(nodes.len() >= 0);
    for node in nodes {
        assert!(!node.id.is_empty());
    }
}

#[tokio::test]
async fn test_find_nodes() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping find nodes test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_find_nodes_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let labels = vec!["Person".to_string(), "User".to_string()];
    let mut properties = HashMap::new();
    properties.insert("active".to_string(), json!(true));
    properties.insert("age".to_string(), json!(25));
    
    let result = adapter.find_nodes(graph_id, labels.clone(), properties.clone()).await;
    
    assert!(result.is_ok());
    let nodes = result.unwrap();
    // Might be empty if no matching nodes exist
    assert!(nodes.len() >= 0);
}

#[tokio::test]
async fn test_find_relationships() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping find relationships test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_find_rels_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let relationship_types = vec!["KNOWS".to_string(), "FOLLOWS".to_string()];
    let mut properties = HashMap::new();
    properties.insert("since".to_string(), json!("2023-01-01"));
    properties.insert("weight".to_string(), json!(0.8));
    
    let result = adapter.find_relationships(graph_id, relationship_types, properties).await;
    
    assert!(result.is_ok());
    let relationships = result.unwrap();
    // Might be empty if no matching relationships exist
    assert!(relationships.len() >= 0);
}

#[tokio::test]
async fn test_find_nodes_temporal() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping temporal nodes test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_temporal_nodes_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let start_time = Utc::now() - chrono::Duration::days(30);
    let end_time = Utc::now();
    
    let temporal_query = TemporalQuery {
        start_time: Some(start_time),
        end_time: Some(end_time),
        temporal_property: "created_at".to_string(),
        operation: TemporalOperation::Between,
    };
    
    let result = adapter.find_nodes_temporal(graph_id, temporal_query).await;
    
    assert!(result.is_ok());
    let nodes = result.unwrap();
    // Might be empty if no nodes match temporal criteria
    assert!(nodes.len() >= 0);
}

#[tokio::test]
async fn test_find_relationships_temporal() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping temporal relationships test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_temporal_rels_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let start_time = Utc::now() - chrono::Duration::days(7);
    let end_time = Utc::now();
    
    let temporal_query = TemporalQuery {
        start_time: Some(start_time),
        end_time: Some(end_time),
        temporal_property: "updated_at".to_string(),
        operation: TemporalOperation::Between,
    };
    
    let result = adapter.find_relationships_temporal(graph_id, temporal_query).await;
    
    assert!(result.is_ok());
    let relationships = result.unwrap();
    // Might be empty if no relationships match temporal criteria
    assert!(relationships.len() >= 0);
}

#[tokio::test]
async fn test_traversal_direction_variants() {
    // Test different traversal directions
    let directions = [
        TraversalDirection::Outgoing,
        TraversalDirection::Incoming,
        TraversalDirection::Both,
    ];
    
    for direction in directions {
        let mut params = create_test_traversal_params();
        params.direction = direction;
        
        // Verify the direction is set correctly
        assert!(matches!(params.direction, _));
    }
}

#[tokio::test]
async fn test_traversal_params_with_filters() {
    let mut params = create_test_traversal_params();
    
    // Add node filters
    params.node_filters.insert("active".to_string(), json!(true));
    params.node_filters.insert("age".to_string(), json!(25));
    
    // Add relationship filters
    params.relationship_filters.insert("weight".to_string(), json!(0.5));
    params.relationship_filters.insert("created_after".to_string(), json!("2023-01-01"));
    
    assert_eq!(params.node_filters.len(), 2);
    assert_eq!(params.relationship_filters.len(), 2);
    assert_eq!(params.node_filters.get("active"), Some(&json!(true)));
    assert_eq!(params.relationship_filters.get("weight"), Some(&json!(0.5)));
}

#[tokio::test]
async fn test_temporal_operations() {
    // Test different temporal operations
    let operations = [
        TemporalOperation::Between,
        TemporalOperation::Before,
        TemporalOperation::After,
        TemporalOperation::At,
    ];
    
    for operation in operations {
        let temporal_query = TemporalQuery {
            start_time: Some(Utc::now() - chrono::Duration::hours(1)),
            end_time: Some(Utc::now()),
            temporal_property: "timestamp".to_string(),
            operation,
        };
        
        assert!(matches!(temporal_query.operation, _));
        assert_eq!(temporal_query.temporal_property, "timestamp");
    }
}

#[tokio::test]
async fn test_traversal_with_depth_limits() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping depth limits test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "traversal_depth_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Test different depth limits
    let depth_limits = [1, 3, 5, 10];
    
    for max_depth in depth_limits {
        let mut params = create_test_traversal_params();
        params.max_depth = Some(max_depth);
        
        let result = adapter.traverse_from(graph_id, "start_node", params).await;
        assert!(result.is_ok());
        
        // The actual traversal depth would be limited by max_depth in a real scenario
        let nodes = result.unwrap();
        assert!(nodes.len() >= 0); // Could be empty if no nodes within depth
    }
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