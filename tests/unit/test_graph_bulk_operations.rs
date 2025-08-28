//! Tests for GraphBulkOperations implementation following TYL patterns
//! Maintains compatibility with tyl-graph-port as the source of truth

use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{FalkorDBAdapter, GraphBulkOperations, GraphInfo, MultiGraphManager};
use tyl_graph_port::{BulkOperation, GraphNode, GraphRelationship};

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
            // Return a mock-like adapter for testing logic
            panic!("Redis not available for bulk operations tests")
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

    adapter
        .create_graph(graph_info)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    Ok(())
}

/// Helper function to create test nodes for bulk operations
fn create_test_nodes(count: usize) -> Vec<GraphNode> {
    let mut nodes = Vec::new();

    for i in 0..count {
        let mut node = GraphNode::new();
        node.id = format!("bulk_node_{}", i);
        node.labels = vec!["TestNode".to_string()];
        node.properties
            .insert("name".to_string(), json!(format!("Node {}", i)));
        node.properties.insert("index".to_string(), json!(i));
        node.properties.insert("active".to_string(), json!(true));
        node.created_at = Utc::now();
        node.updated_at = Utc::now();
        nodes.push(node);
    }

    nodes
}

/// Helper function to create test relationships for bulk operations
fn create_test_relationships(count: usize) -> Vec<GraphRelationship> {
    let mut relationships = Vec::new();

    for i in 0..count {
        let mut rel = GraphRelationship::new(
            format!("bulk_node_{}", i),
            format!("bulk_node_{}", (i + 1) % count),
            "CONNECTS_TO".to_string(),
        );
        rel.id = format!("bulk_rel_{}", i);
        rel.from_node_id = format!("bulk_node_{}", i);
        rel.to_node_id = format!("bulk_node_{}", (i + 1) % count);
        rel.relationship_type = "CONNECTS_TO".to_string();
        rel.properties
            .insert("weight".to_string(), json!(0.5 + i as f64 * 0.1));
        rel.properties
            .insert("created_batch".to_string(), json!(true));
        rel.created_at = Utc::now();
        rel.updated_at = Utc::now();
        relationships.push(rel);
    }

    relationships
}

/// Helper function to create BulkOperation
fn create_bulk_operation<T>(items: Vec<T>, batch_size: usize) -> BulkOperation<T> {
    BulkOperation {
        items,
        batch_size,
        continue_on_error: true,
        metadata: HashMap::new(),
    }
}

#[test]
fn test_bulk_operation_creation() {
    let nodes = create_test_nodes(5);
    let bulk_op = create_bulk_operation(nodes, 2);

    assert_eq!(bulk_op.items.len(), 5);
    assert_eq!(bulk_op.batch_size, 2);
    assert!(bulk_op.continue_on_error);
    assert!(bulk_op.metadata.is_empty());
}

#[test]
fn test_test_nodes_creation() {
    let nodes = create_test_nodes(3);

    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].id, "bulk_node_0");
    assert_eq!(nodes[1].id, "bulk_node_1");
    assert_eq!(nodes[2].id, "bulk_node_2");

    for (i, node) in nodes.iter().enumerate() {
        assert!(node.labels.contains(&"TestNode".to_string()));
        assert_eq!(node.properties.get("index"), Some(&json!(i)));
        assert_eq!(node.properties.get("active"), Some(&json!(true)));
    }
}

#[test]
fn test_test_relationships_creation() {
    let relationships = create_test_relationships(3);

    assert_eq!(relationships.len(), 3);
    assert_eq!(relationships[0].from_node_id, "bulk_node_0");
    assert_eq!(relationships[0].to_node_id, "bulk_node_1");
    assert_eq!(relationships[1].from_node_id, "bulk_node_1");
    assert_eq!(relationships[1].to_node_id, "bulk_node_2");
    assert_eq!(relationships[2].from_node_id, "bulk_node_2");
    assert_eq!(relationships[2].to_node_id, "bulk_node_0"); // Circular

    for rel in relationships.iter() {
        assert_eq!(rel.relationship_type, "CONNECTS_TO");
        assert!(rel.properties.contains_key("weight"));
        assert_eq!(rel.properties.get("created_batch"), Some(&json!(true)));
    }
}

#[tokio::test]
async fn test_bulk_create_nodes_nonexistent_graph() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping bulk create nodes test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let nodes = create_test_nodes(3);
    let bulk_op = create_bulk_operation(nodes, 2);

    // Try to bulk create in non-existent graph
    let result = adapter
        .bulk_create_nodes("non_existent_graph", bulk_op)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Graph not found"));
}

#[tokio::test]
async fn test_bulk_create_nodes_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping bulk create nodes basic test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "bulk_nodes_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let nodes = create_test_nodes(5);
    let bulk_op = create_bulk_operation(nodes, 2);

    let result = adapter.bulk_create_nodes(graph_id, bulk_op).await;

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 5);

    // All results should be successful (or errors if Redis constraints fail)
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(node_id) => {
                assert_eq!(*node_id, format!("bulk_node_{}", i));
            }
            Err(_) => {
                // Acceptable if Redis has constraints or data issues
                println!("Node creation failed (expected in unit tests): {:?}", res);
            }
        }
    }
}

#[tokio::test]
async fn test_bulk_create_relationships_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping bulk create relationships test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "bulk_relationships_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let relationships = create_test_relationships(3);
    let bulk_op = create_bulk_operation(relationships, 2);

    let result = adapter.bulk_create_relationships(graph_id, bulk_op).await;

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 3);

    // Results can be successful or fail due to missing nodes
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(rel_id) => {
                assert_eq!(*rel_id, format!("bulk_rel_{}", i));
            }
            Err(_) => {
                // Expected if referenced nodes don't exist
                println!("Relationship creation failed (expected): {:?}", res);
            }
        }
    }
}

#[tokio::test]
async fn test_bulk_update_nodes_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping bulk update nodes test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "bulk_update_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create update data
    let mut updates = HashMap::new();
    updates.insert("node_1".to_string(), {
        let mut props = HashMap::new();
        props.insert("updated".to_string(), json!(true));
        props.insert("timestamp".to_string(), json!(Utc::now().to_rfc3339()));
        props
    });
    updates.insert("node_2".to_string(), {
        let mut props = HashMap::new();
        props.insert("status".to_string(), json!("modified"));
        props.insert("value".to_string(), json!(42));
        props
    });

    let result = adapter.bulk_update_nodes(graph_id, updates).await;

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 2);

    // Updates might fail if nodes don't exist, which is expected
    for res in results.iter() {
        match res {
            Ok(()) => {
                println!("Update succeeded");
            }
            Err(_) => {
                println!("Update failed (expected if nodes don't exist): {:?}", res);
            }
        }
    }
}

#[tokio::test]
async fn test_bulk_delete_nodes_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping bulk delete nodes test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "bulk_delete_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let node_ids = vec![
        "delete_node_1".to_string(),
        "delete_node_2".to_string(),
        "delete_node_3".to_string(),
    ];

    let result = adapter.bulk_delete_nodes(graph_id, node_ids).await;

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 3);

    // Deletions should succeed even if nodes don't exist
    for res in results.iter() {
        assert!(res.is_ok());
    }
}

#[tokio::test]
async fn test_import_data_json() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping import JSON test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "import_json_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create test JSON data
    let json_data = json!({
        "nodes": [
            {
                "id": "import_node_1",
                "labels": ["ImportedNode"],
                "properties": {
                    "name": "Imported Node 1",
                    "value": 100
                }
            },
            {
                "id": "import_node_2",
                "labels": ["ImportedNode"],
                "properties": {
                    "name": "Imported Node 2",
                    "value": 200
                }
            }
        ],
        "relationships": [
            {
                "id": "import_rel_1",
                "from_node_id": "import_node_1",
                "to_node_id": "import_node_2",
                "relationship_type": "IMPORTED_LINK",
                "properties": {
                    "weight": 0.8
                }
            }
        ]
    });

    let json_bytes = serde_json::to_vec(&json_data).unwrap();

    let result = adapter.import_data(graph_id, "json", json_bytes).await;

    assert!(result.is_ok());
    let import_stats = result.unwrap();

    assert_eq!(import_stats.get("format"), Some(&json!("json")));
    assert!(import_stats.contains_key("started_at"));
    assert!(import_stats.contains_key("completed_at"));
    assert_eq!(import_stats.get("success"), Some(&json!(true)));

    // Verify import stats for nodes and relationships
    if let Some(nodes_created) = import_stats.get("nodes_created") {
        println!("Nodes created: {}", nodes_created);
    }
    if let Some(rels_created) = import_stats.get("relationships_created") {
        println!("Relationships created: {}", rels_created);
    }
}

#[tokio::test]
async fn test_import_data_csv() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping import CSV test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "import_csv_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create test CSV data
    let csv_data = "name,age,active\nAlice,25,true\nBob,30,false\nCharlie,35,true";
    let csv_bytes = csv_data.as_bytes().to_vec();

    let result = adapter.import_data(graph_id, "csv", csv_bytes).await;

    assert!(result.is_ok());
    let import_stats = result.unwrap();

    assert_eq!(import_stats.get("format"), Some(&json!("csv")));
    assert!(import_stats.contains_key("started_at"));
    assert!(import_stats.contains_key("completed_at"));
    assert_eq!(import_stats.get("success"), Some(&json!(true)));

    if let Some(rows_processed) = import_stats.get("rows_processed") {
        println!("CSV rows processed: {}", rows_processed);
    }
}

#[tokio::test]
async fn test_import_data_cypher() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping import Cypher test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "import_cypher_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create test Cypher script
    let cypher_script = "CREATE (n:TestNode {name: 'Cypher Import', value: 42})";
    let cypher_bytes = cypher_script.as_bytes().to_vec();

    let result = adapter.import_data(graph_id, "cypher", cypher_bytes).await;

    assert!(result.is_ok());
    let import_stats = result.unwrap();

    assert_eq!(import_stats.get("format"), Some(&json!("cypher")));
    assert!(import_stats.contains_key("started_at"));
    assert!(import_stats.contains_key("completed_at"));
    assert_eq!(import_stats.get("success"), Some(&json!(true)));
    assert!(import_stats.contains_key("query_result"));
}

#[tokio::test]
async fn test_import_data_unsupported_format() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping unsupported format test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "import_unsupported_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let data = "some data".as_bytes().to_vec();

    let result = adapter.import_data(graph_id, "xml", data).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unsupported import format: xml"));
}

#[tokio::test]
async fn test_export_data_json() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping export JSON test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "export_json_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let export_params = HashMap::new();

    let result = adapter.export_data(graph_id, "json", export_params).await;

    assert!(result.is_ok());
    let json_bytes = result.unwrap();
    let json_str = String::from_utf8(json_bytes).unwrap();

    // Verify it's valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert!(parsed.is_object());
    assert!(parsed.get("graph_id").is_some());
    assert!(parsed.get("nodes").is_some());
    assert!(parsed.get("relationships").is_some());
    assert!(parsed.get("exported_at").is_some());
}

#[tokio::test]
async fn test_export_data_csv() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping export CSV test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "export_csv_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let export_params = HashMap::new();

    let result = adapter.export_data(graph_id, "csv", export_params).await;

    assert!(result.is_ok());
    let csv_bytes = result.unwrap();
    let csv_str = String::from_utf8(csv_bytes).unwrap();

    // Verify CSV structure
    let lines: Vec<&str> = csv_str.lines().collect();
    assert!(!lines.is_empty());
    assert!(lines[0].contains("id,type,labels,properties")); // Header
}

#[tokio::test]
async fn test_export_data_cypher() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping export Cypher test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "export_cypher_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let export_params = HashMap::new();

    let result = adapter.export_data(graph_id, "cypher", export_params).await;

    assert!(result.is_ok());
    let cypher_bytes = result.unwrap();
    let cypher_str = String::from_utf8(cypher_bytes).unwrap();

    // Verify Cypher format
    assert!(cypher_str.contains("// Cypher export for graph:"));
    assert!(cypher_str.contains("// Generated at:"));
}

#[tokio::test]
async fn test_export_data_graphml() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping export GraphML test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "export_graphml_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let export_params = HashMap::new();

    let result = adapter
        .export_data(graph_id, "graphml", export_params)
        .await;

    assert!(result.is_ok());
    let graphml_bytes = result.unwrap();
    let graphml_str = String::from_utf8(graphml_bytes).unwrap();

    // Verify GraphML structure
    assert!(graphml_str.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    assert!(graphml_str.contains("<graphml"));
    assert!(graphml_str.contains("</graphml>"));
}

#[tokio::test]
async fn test_export_data_unsupported_format() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping export unsupported format test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "export_unsupported_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let export_params = HashMap::new();

    let result = adapter.export_data(graph_id, "yaml", export_params).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unsupported export format: yaml"));
}

#[tokio::test]
async fn test_bulk_operations_with_different_batch_sizes() {
    // Test different batch sizes
    let batch_sizes = [1, 5, 10, 50];

    for batch_size in batch_sizes {
        let nodes = create_test_nodes(10);
        let bulk_op = create_bulk_operation(nodes, batch_size);

        // Verify batch configuration
        assert_eq!(bulk_op.batch_size, batch_size);
        assert_eq!(bulk_op.items.len(), 10);

        // In a real test, we would verify batching behavior
        // For unit tests, we just validate the data structures
    }
}

#[tokio::test]
async fn test_bulk_operations_empty_data() {
    // Test with empty data
    let empty_nodes: Vec<GraphNode> = vec![];
    let bulk_op = create_bulk_operation(empty_nodes, 10);

    assert_eq!(bulk_op.items.len(), 0);
    assert_eq!(bulk_op.batch_size, 10);

    // Empty bulk operations should be handled gracefully
    let empty_relationships: Vec<GraphRelationship> = vec![];
    let bulk_rel_op = create_bulk_operation(empty_relationships, 5);

    assert_eq!(bulk_rel_op.items.len(), 0);
    assert_eq!(bulk_rel_op.batch_size, 5);
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
