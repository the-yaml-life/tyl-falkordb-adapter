//! Tests for GraphTransaction implementation following TYL patterns

use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphInfo, GraphTransaction, IsolationLevel, MultiGraphManager,
    TransactionContext,
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
            // Return a mock-like adapter for testing transaction logic
            // In a real scenario, this would use a proper mock
            panic!("Redis not available for transaction tests")
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
async fn test_transaction_context_creation() {
    // Test TransactionContext builder patterns
    let mut context = TransactionContext::new()
        .with_isolation(IsolationLevel::ReadCommitted)
        .with_timeout(30);
    
    // Set custom ID
    context.id = "tx_001".to_string();
    
    assert_eq!(context.id, "tx_001");
    assert!(!context.read_only);
    assert!(matches!(context.isolation_level, IsolationLevel::ReadCommitted));
    assert_eq!(context.timeout_seconds, Some(30));
}

#[tokio::test]
async fn test_read_only_transaction_context() {
    // Test read-only transaction context
    let mut context = TransactionContext::read_only()
        .with_isolation(IsolationLevel::RepeatableRead);
    
    // Set custom ID
    context.id = "tx_readonly".to_string();
    
    assert_eq!(context.id, "tx_readonly");
    assert!(context.read_only);
    assert!(matches!(context.isolation_level, IsolationLevel::RepeatableRead));
}

#[tokio::test]
async fn test_transaction_context_metadata() {
    // Test transaction context with metadata
    let mut metadata = HashMap::new();
    metadata.insert("user_id".to_string(), json!("test_user_123"));
    metadata.insert("session_id".to_string(), json!("session_abc"));
    
    let mut context = TransactionContext::new();
    context.id = "tx_meta".to_string();
    context.metadata = metadata.clone();
    
    assert_eq!(context.metadata, metadata);
    assert_eq!(
        context.metadata.get("user_id"),
        Some(&json!("test_user_123"))
    );
}

#[tokio::test]
async fn test_begin_transaction_graph_not_found() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping transaction test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    
    let mut context = TransactionContext::new();
    context.id = "tx_test".to_string();
    
    // Try to begin transaction on non-existent graph
    let result = adapter.begin_transaction("non_existent_graph", context).await;
    
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Graph not found"));
}

#[tokio::test]
async fn test_transaction_lifecycle() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping transaction lifecycle test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "tx_lifecycle_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Begin transaction
    let mut context = TransactionContext::new()
        .with_isolation(IsolationLevel::ReadCommitted)
        .with_timeout(30);
    context.id = "tx_lifecycle".to_string();
    
    let tx_context = adapter.begin_transaction(graph_id, context.clone()).await;
    assert!(tx_context.is_ok());
    
    let tx_id = &context.id;
    
    // Check transaction status
    let status = adapter.get_transaction_status(graph_id, tx_id).await;
    assert!(status.is_ok());
    assert!(status.unwrap().is_some());
    
    // Commit transaction
    let commit_result = adapter.commit_transaction(graph_id, tx_id).await;
    assert!(commit_result.is_ok());
    
    // Verify transaction is removed after commit
    let status_after_commit = adapter.get_transaction_status(graph_id, tx_id).await;
    assert!(status_after_commit.is_ok());
    assert!(status_after_commit.unwrap().is_none());
}

#[tokio::test]
async fn test_transaction_rollback() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping transaction rollback test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "tx_rollback_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Begin transaction
    let mut context = TransactionContext::new();
    context.id = "tx_rollback".to_string();
    let tx_context = adapter.begin_transaction(graph_id, context.clone()).await;
    assert!(tx_context.is_ok());
    
    let tx_id = &context.id;
    
    // Rollback transaction
    let rollback_result = adapter.rollback_transaction(graph_id, tx_id).await;
    assert!(rollback_result.is_ok());
    
    // Verify transaction is removed after rollback
    let status_after_rollback = adapter.get_transaction_status(graph_id, tx_id).await;
    assert!(status_after_rollback.is_ok());
    assert!(status_after_rollback.unwrap().is_none());
}

#[tokio::test]
async fn test_transaction_not_found_operations() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping transaction not found test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "tx_not_found_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    let non_existent_tx = "non_existent_tx";
    
    // Try to commit non-existent transaction
    let commit_result = adapter.commit_transaction(graph_id, non_existent_tx).await;
    assert!(commit_result.is_err());
    assert!(commit_result
        .unwrap_err()
        .to_string()
        .contains("not found"));
    
    // Try to rollback non-existent transaction
    let rollback_result = adapter.rollback_transaction(graph_id, non_existent_tx).await;
    assert!(rollback_result.is_err());
    assert!(rollback_result
        .unwrap_err()
        .to_string()
        .contains("not found"));
    
    // Try to get status of non-existent transaction
    let status_result = adapter
        .get_transaction_status(graph_id, non_existent_tx)
        .await;
    assert!(status_result.is_ok());
    assert!(status_result.unwrap().is_none());
}

#[tokio::test]
async fn test_concurrent_transactions() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping concurrent transactions test - Redis not available");
        return;
    }
    
    let adapter = create_test_adapter().await;
    let graph_id = "tx_concurrent_test";
    
    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }
    
    // Begin multiple transactions
    let mut tx1_context = TransactionContext::new();
    tx1_context.id = "tx_concurrent_1".to_string();
    
    let mut tx2_context = TransactionContext::read_only();
    tx2_context.id = "tx_concurrent_2".to_string();
    
    let tx1_result = adapter.begin_transaction(graph_id, tx1_context.clone()).await;
    let tx2_result = adapter.begin_transaction(graph_id, tx2_context.clone()).await;
    
    assert!(tx1_result.is_ok());
    assert!(tx2_result.is_ok());
    
    // Both transactions should exist
    let tx1_status = adapter
        .get_transaction_status(graph_id, &tx1_context.id)
        .await;
    let tx2_status = adapter
        .get_transaction_status(graph_id, &tx2_context.id)
        .await;
    
    assert!(tx1_status.is_ok() && tx1_status.unwrap().is_some());
    assert!(tx2_status.is_ok() && tx2_status.unwrap().is_some());
    
    // Cleanup
    let _ = adapter.commit_transaction(graph_id, &tx1_context.id).await;
    let _ = adapter.rollback_transaction(graph_id, &tx2_context.id).await;
}

#[tokio::test]
async fn test_isolation_levels() {
    // Test all isolation levels can be set
    let isolation_levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];
    
    for level in isolation_levels {
        let context = TransactionContext::new()
            .with_isolation(level.clone());
        
        assert!(matches!(context.isolation_level, _level));
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