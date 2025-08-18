//! Tests for FalkorDB adapter creation following TYL patterns

use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{falkordb_errors, FalkorDBAdapter};

#[tokio::test]
async fn test_adapter_creation_with_valid_config() {
    // Test that adapter can be created with valid configuration
    let config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    };

    let graph_name = "test_graph".to_string();

    // This will fail without a running Redis instance, which is expected
    let result = FalkorDBAdapter::new(config, graph_name).await;

    // Verify error is properly formatted with TYL patterns
    if let Err(error) = result {
        assert!(error.to_string().contains("FalkorDB connection failed"));
    }
}

#[test]
fn test_falkordb_error_helpers() {
    // Test connection error helper
    let conn_error = falkordb_errors::connection_failed("timeout");
    assert!(conn_error
        .to_string()
        .contains("FalkorDB connection failed: timeout"));

    // Test query error helper
    let query_error = falkordb_errors::query_execution_failed("CREATE (n)", "syntax error");
    assert!(query_error
        .to_string()
        .contains("Query 'CREATE (n)' failed: syntax error"));

    // Test not found errors
    let node_error = falkordb_errors::node_not_found("node_123");
    assert!(node_error.to_string().contains("graph_node"));

    let rel_error = falkordb_errors::relationship_not_found("rel_456");
    assert!(rel_error.to_string().contains("graph_relationship"));

    // Test validation error
    let validation_error = falkordb_errors::invalid_graph_data("node_id", "cannot be empty");
    assert!(validation_error.to_string().contains("cannot be empty"));
}

#[test]
fn test_graph_node_creation() {
    use tyl_falkordb_adapter::GraphNode;

    let node = GraphNode::new("test_node_123".to_string());

    assert_eq!(node.id, "test_node_123");
    assert!(node.labels.is_empty());
    assert!(node.properties.is_empty());
    assert!(node.created_at <= chrono::Utc::now());
    assert_eq!(node.created_at, node.updated_at);
}

#[test]
fn test_graph_relationship_creation() {
    use tyl_falkordb_adapter::GraphRelationship;

    let relationship = GraphRelationship::new(
        "rel_123".to_string(),
        "node_1".to_string(),
        "node_2".to_string(),
        "CONNECTS".to_string(),
    );

    assert_eq!(relationship.id, "rel_123");
    assert_eq!(relationship.from_node_id, "node_1");
    assert_eq!(relationship.to_node_id, "node_2");
    assert_eq!(relationship.relationship_type, "CONNECTS");
    assert!(relationship.properties.is_empty());
    assert!(relationship.created_at <= chrono::Utc::now());
}
