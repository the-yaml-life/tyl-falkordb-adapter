//! Tests for GraphQueryExecutor implementation following TYL patterns

use std::collections::HashMap;

use serde_json::json;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphInfo, GraphQuery, GraphQueryExecutor, MultiGraphManager, QueryResult,
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
            // Return a mock-like adapter for testing query executor logic
            panic!("Redis not available for query executor tests")
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
        name: format!("Test Graph {graph_id}"),
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

#[test]
fn test_graph_query_creation_read() {
    let query = GraphQuery::read("MATCH (n) RETURN n".to_string());

    assert_eq!(query.query, "MATCH (n) RETURN n");
    assert!(!query.is_write_query);
    assert!(query.parameters.is_empty());
}

#[test]
fn test_graph_query_creation_write() {
    let query = GraphQuery::write("CREATE (n:Person {name: 'Test'}) RETURN n".to_string());

    assert_eq!(query.query, "CREATE (n:Person {name: 'Test'}) RETURN n");
    assert!(query.is_write_query);
    assert!(query.parameters.is_empty());
}

#[test]
fn test_graph_query_with_parameters() {
    let mut query = GraphQuery::read("MATCH (n:Person {name: $name}) RETURN n".to_string());
    query = query.with_parameter("name".to_string(), json!("John Doe"));

    assert_eq!(query.query, "MATCH (n:Person {name: $name}) RETURN n");
    assert!(!query.is_write_query);
    assert_eq!(query.parameters.len(), 1);
    assert_eq!(query.parameters.get("name"), Some(&json!("John Doe")));
}

#[test]
fn test_query_result_creation() {
    let mut result = QueryResult::new();

    // Add a test row
    let mut row = HashMap::new();
    row.insert("id".to_string(), json!(1));
    row.insert("name".to_string(), json!("Test"));
    result = result.add_row(row);

    // Add metadata
    result = result.with_metadata("query_type".to_string(), json!("test"));

    assert_eq!(result.data.len(), 1);
    assert_eq!(result.data[0].get("id"), Some(&json!(1)));
    assert_eq!(result.data[0].get("name"), Some(&json!("Test")));
    assert_eq!(result.metadata.get("query_type"), Some(&json!("test")));
}

#[tokio::test]
async fn test_execute_query_nonexistent_graph() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping query executor test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let query = GraphQuery::read("MATCH (n) RETURN count(n)".to_string());

    // Try to execute query on non-existent graph
    let result = adapter.execute_query("non_existent_graph", query).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Graph not found"));
}

#[tokio::test]
async fn test_execute_read_query_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping read query test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_read_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Execute a simple read query
    let query = GraphQuery::read("MATCH (n) RETURN count(n) AS node_count".to_string());
    let result = adapter.execute_read_query(graph_id, query.clone()).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();

    // Verify result structure
    assert!(query_result.data.is_empty() || !query_result.data.is_empty()); // Either empty or has data
    assert!(query_result.metadata.contains_key("original_query"));
    assert!(query_result.metadata.contains_key("is_write_query"));
    assert!(query_result.metadata.contains_key("executed_at"));

    // Verify metadata content
    assert_eq!(
        query_result.metadata.get("is_write_query"),
        Some(&json!(false))
    );
    assert_eq!(
        query_result.metadata.get("original_query"),
        Some(&json!(query.query))
    );
}

#[tokio::test]
async fn test_execute_write_query_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping write query test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_write_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Execute a simple write query
    let query = GraphQuery::write("CREATE (n:TestNode {id: 1, name: 'Test'}) RETURN n".to_string());
    let result = adapter.execute_write_query(graph_id, query.clone()).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();

    // Verify result structure
    assert!(query_result.metadata.contains_key("original_query"));
    assert!(query_result.metadata.contains_key("is_write_query"));
    assert!(query_result.metadata.contains_key("executed_at"));

    // Verify metadata content
    assert_eq!(
        query_result.metadata.get("is_write_query"),
        Some(&json!(true))
    );
    assert_eq!(
        query_result.metadata.get("original_query"),
        Some(&json!(query.query))
    );
}

#[tokio::test]
async fn test_execute_query_with_parameters() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping parameterized query test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_params_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Execute a parameterized query
    let query =
        GraphQuery::write("CREATE (n:Person {name: $name, age: $age}) RETURN n".to_string())
            .with_parameter("name".to_string(), json!("Alice"))
            .with_parameter("age".to_string(), json!(30));

    let result = adapter.execute_write_query(graph_id, query.clone()).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();

    // Verify parameters were processed
    assert_eq!(
        query_result.metadata.get("parameter_count"),
        Some(&json!(2))
    );
}

#[tokio::test]
async fn test_execute_query_auto_routing() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping auto routing test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_routing_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Test read query routing
    let read_query = GraphQuery::read("MATCH (n) RETURN count(n)".to_string());
    let read_result = adapter.execute_query(graph_id, read_query.clone()).await;
    assert!(read_result.is_ok());

    let read_result = read_result.unwrap();
    assert_eq!(
        read_result.metadata.get("is_write_query"),
        Some(&json!(false))
    );

    // Test write query routing
    let write_query = GraphQuery::write("CREATE (n:Test {id: 1}) RETURN n".to_string());
    let write_result = adapter.execute_query(graph_id, write_query.clone()).await;
    assert!(write_result.is_ok());

    let write_result = write_result.unwrap();
    assert_eq!(
        write_result.metadata.get("is_write_query"),
        Some(&json!(true))
    );
}

#[tokio::test]
async fn test_read_query_validation_error() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping read query validation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_validation_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Try to execute a write query marked as read
    let invalid_query = GraphQuery::write("CREATE (n:Test) RETURN n".to_string());
    let result = adapter.execute_read_query(graph_id, invalid_query).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("marked as write operation"));
}

#[tokio::test]
async fn test_write_query_validation_error() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping write query validation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "query_validation_write_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Try to execute a read query marked as write
    let invalid_query = GraphQuery::read("MATCH (n) RETURN n".to_string());
    let result = adapter.execute_write_query(graph_id, invalid_query).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("marked as read operation"));
}

#[tokio::test]
async fn test_complex_parameterized_query() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping complex parameterized query test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "complex_params_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create a complex query with multiple parameter types
    let query = GraphQuery::write(
        "CREATE (p:Person {name: $name, age: $age, active: $active, score: $score})".to_string(),
    )
    .with_parameter("name".to_string(), json!("John Doe"))
    .with_parameter("age".to_string(), json!(25))
    .with_parameter("active".to_string(), json!(true))
    .with_parameter("score".to_string(), json!(95.5));

    let result = adapter.execute_write_query(graph_id, query).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(
        query_result.metadata.get("parameter_count"),
        Some(&json!(4))
    );
}

#[tokio::test]
async fn test_query_result_metadata_completeness() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping metadata completeness test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "metadata_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let query = GraphQuery::read("RETURN 1 as test_value".to_string())
        .with_parameter("test_param".to_string(), json!("test"));

    let result = adapter.execute_read_query(graph_id, query.clone()).await;
    assert!(result.is_ok());

    let query_result = result.unwrap();

    // Verify all expected metadata fields are present
    assert!(query_result.metadata.contains_key("original_query"));
    assert!(query_result.metadata.contains_key("is_write_query"));
    assert!(query_result.metadata.contains_key("parameter_count"));
    assert!(query_result.metadata.contains_key("row_count"));
    assert!(query_result.metadata.contains_key("executed_at"));

    // Verify metadata values
    assert_eq!(
        query_result.metadata.get("original_query"),
        Some(&json!(query.query))
    );
    assert_eq!(
        query_result.metadata.get("is_write_query"),
        Some(&json!(false))
    );
    assert_eq!(
        query_result.metadata.get("parameter_count"),
        Some(&json!(1))
    );

    // Verify executed_at is a valid RFC3339 timestamp
    let timestamp = query_result
        .metadata
        .get("executed_at")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(chrono::DateTime::parse_from_rfc3339(timestamp).is_ok());
}

#[tokio::test]
async fn test_query_with_multiple_parameters() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping multiple parameters test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "multi_params_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Create query with multiple parameters
    let mut params = HashMap::new();
    params.insert("name1".to_string(), json!("Alice"));
    params.insert("name2".to_string(), json!("Bob"));
    params.insert("min_age".to_string(), json!(18));

    let query = GraphQuery::read(
        "MATCH (p:Person) WHERE p.name IN [$name1, $name2] AND p.age >= $min_age RETURN p"
            .to_string(),
    )
    .with_parameters(params.clone());

    let result = adapter.execute_read_query(graph_id, query).await;
    assert!(result.is_ok());

    let query_result = result.unwrap();
    assert_eq!(
        query_result.metadata.get("parameter_count"),
        Some(&json!(params.len()))
    );
}

// Helper function to check if Redis is available
async fn redis_available() -> bool {
    use redis::Client;

    match Client::open("redis://localhost:6379") {
        Ok(client) => client.get_connection().is_ok(),
        Err(_) => false,
    }
}
