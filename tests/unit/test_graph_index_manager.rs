//! Tests for GraphIndexManager implementation following TYL patterns

use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphIndexManager, GraphInfo, IndexConfig, IndexType, MultiGraphManager,
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
            // Return a mock-like adapter for testing index logic
            panic!("Redis not available for index tests")
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

#[test]
fn test_index_config_node_property() {
    let index = IndexConfig::node_property(
        "person_name_idx",
        vec!["Person".to_string()],
        vec!["name".to_string()],
    );

    assert_eq!(index.name, "person_name_idx");
    assert!(matches!(index.index_type, IndexType::NodeProperty));
    assert_eq!(index.labels_or_types, vec!["Person"]);
    assert_eq!(index.properties, vec!["name"]);
}

#[test]
fn test_index_config_relationship_property() {
    let index = IndexConfig::relationship_property(
        "follows_since_idx",
        vec!["FOLLOWS".to_string()],
        vec!["since".to_string()],
    );

    assert_eq!(index.name, "follows_since_idx");
    assert!(matches!(index.index_type, IndexType::RelationshipProperty));
    assert_eq!(index.labels_or_types, vec!["FOLLOWS"]);
    assert_eq!(index.properties, vec!["since"]);
}

#[test]
fn test_index_config_fulltext() {
    let index = IndexConfig::fulltext(
        "article_content_idx",
        vec!["Article".to_string()],
        vec!["title".to_string(), "content".to_string()],
    );

    assert_eq!(index.name, "article_content_idx");
    assert!(matches!(index.index_type, IndexType::Fulltext));
    assert_eq!(index.labels_or_types, vec!["Article"]);
    assert_eq!(index.properties, vec!["title", "content"]);
}

#[test]
fn test_index_config_with_options() {
    let index = IndexConfig::node_property(
        "vector_idx",
        vec!["Document".to_string()],
        vec!["embedding".to_string()],
    )
    .with_option("dimensions", json!(256))
    .with_option("similarity", json!("cosine"));

    assert_eq!(index.options.get("dimensions"), Some(&json!(256)));
    assert_eq!(index.options.get("similarity"), Some(&json!("cosine")));
}

#[tokio::test]
async fn test_create_index_graph_not_found() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping index test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    let index = IndexConfig::node_property(
        "test_idx",
        vec!["Person".to_string()],
        vec!["name".to_string()],
    );
    
    // Try to create index on non-existent graph
    let result = adapter.create_index("non_existent_graph", index).await;
    
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Graph not found"));
}

#[tokio::test]
async fn test_index_lifecycle() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping index lifecycle test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "index_lifecycle_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Create node property index
    let index = IndexConfig::node_property(
        "person_name_idx",
        vec!["Person".to_string()],
        vec!["name".to_string()],
    );
    
    // Create index
    let create_result = adapter.create_index(graph_id, index.clone()).await;
    assert!(create_result.is_ok());
    
    // List indexes
    let indexes = adapter.list_indexes(graph_id).await;
    assert!(indexes.is_ok());
    let indexes = indexes.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "person_name_idx");
    
    // Get specific index
    let retrieved_index = adapter.get_index(graph_id, "person_name_idx").await;
    assert!(retrieved_index.is_ok());
    assert!(retrieved_index.unwrap().is_some());
    
    // Drop index
    let drop_result = adapter.drop_index(graph_id, "person_name_idx").await;
    assert!(drop_result.is_ok());
    
    // Verify index is dropped
    let indexes_after_drop = adapter.list_indexes(graph_id).await;
    assert!(indexes_after_drop.is_ok());
    assert!(indexes_after_drop.unwrap().is_empty());
}

#[tokio::test]
async fn test_different_index_types() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping different index types test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "index_types_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Test different index types
    let indexes = vec![
        IndexConfig::node_property(
            "node_prop_idx",
            vec!["Person".to_string()],
            vec!["age".to_string()],
        ),
        IndexConfig::relationship_property(
            "rel_prop_idx",
            vec!["KNOWS".to_string()],
            vec!["since".to_string()],
        ),
        IndexConfig::fulltext(
            "fulltext_idx",
            vec!["Article".to_string()],
            vec!["title".to_string(), "content".to_string()],
        ),
        // Vector index with options
        IndexConfig::node_property(
            "vector_idx",
            vec!["Document".to_string()],
            vec!["embedding".to_string()],
        )
        .with_option("dimensions", json!(128)),
    ];
    
    // Create all indexes
    for index in &indexes {
        let result = adapter.create_index(graph_id, index.clone()).await;
        assert!(result.is_ok(), "Failed to create index: {}", index.name);
    }
    
    // Verify all indexes exist
    let all_indexes = adapter.list_indexes(graph_id).await;
    assert!(all_indexes.is_ok());
    let all_indexes = all_indexes.unwrap();
    assert_eq!(all_indexes.len(), indexes.len());
    
    // Verify each index can be retrieved
    for index in &indexes {
        let retrieved = adapter.get_index(graph_id, &index.name).await;
        assert!(retrieved.is_ok());
        assert!(retrieved.unwrap().is_some());
    }
}

#[tokio::test]
async fn test_index_already_exists_error() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping index already exists test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "index_exists_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let index = IndexConfig::node_property(
        "duplicate_idx",
        vec!["Person".to_string()],
        vec!["name".to_string()],
    );
    
    // Create index first time
    let first_result = adapter.create_index(graph_id, index.clone()).await;
    assert!(first_result.is_ok());
    
    // Try to create same index again
    let second_result = adapter.create_index(graph_id, index).await;
    assert!(second_result.is_err());
    assert!(second_result
        .unwrap_err()
        .to_string()
        .contains("Index already exists"));
}

#[tokio::test]
async fn test_drop_nonexistent_index() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping drop nonexistent index test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "drop_nonexistent_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Try to drop non-existent index
    let result = adapter.drop_index(graph_id, "nonexistent_index").await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not found"));
}

#[tokio::test]
async fn test_rebuild_index() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping rebuild index test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "rebuild_index_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let index = IndexConfig::node_property(
        "rebuild_test_idx",
        vec!["Person".to_string()],
        vec!["name".to_string()],
    );
    
    // Create index
    let create_result = adapter.create_index(graph_id, index).await;
    assert!(create_result.is_ok());
    
    // Rebuild index
    let rebuild_result = adapter.rebuild_index(graph_id, "rebuild_test_idx").await;
    assert!(rebuild_result.is_ok());
    
    // Verify index still exists after rebuild
    let retrieved = adapter.get_index(graph_id, "rebuild_test_idx").await;
    assert!(retrieved.is_ok());
    assert!(retrieved.unwrap().is_some());
}

#[tokio::test]
async fn test_composite_index() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping composite index test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "composite_index_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Create composite index with multiple properties
    let mut composite_index = IndexConfig::node_property(
        "person_composite_idx",
        vec!["Person".to_string()],
        vec!["firstName".to_string(), "lastName".to_string(), "age".to_string()],
    );
    composite_index.index_type = IndexType::Composite;
    
    // Create composite index
    let result = adapter.create_index(graph_id, composite_index.clone()).await;
    assert!(result.is_ok());
    
    // Verify composite index was created
    let retrieved = adapter.get_index(graph_id, "person_composite_idx").await;
    assert!(retrieved.is_ok());
    let retrieved = retrieved.unwrap();
    assert!(retrieved.is_some());
    
    let retrieved = retrieved.unwrap();
    assert!(matches!(retrieved.index_type, IndexType::Composite));
    assert_eq!(retrieved.properties.len(), 3);
}

#[tokio::test]
async fn test_vector_index_with_dimensions() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping vector index test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "vector_index_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Create vector index with dimensions
    let mut vector_index = IndexConfig::node_property(
        "embedding_idx",
        vec!["Document".to_string()],
        vec!["vector".to_string()],
    )
    .with_option("dimensions", json!(512));
    
    vector_index.index_type = IndexType::Vector;
    
    // Create vector index
    let result = adapter.create_index(graph_id, vector_index).await;
    assert!(result.is_ok());
    
    // Verify vector index was created with correct options
    let retrieved = adapter.get_index(graph_id, "embedding_idx").await;
    assert!(retrieved.is_ok());
    let retrieved = retrieved.unwrap();
    assert!(retrieved.is_some());
    
    let retrieved = retrieved.unwrap();
    assert!(matches!(retrieved.index_type, IndexType::Vector));
    assert_eq!(retrieved.options.get("dimensions"), Some(&json!(512)));
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