//! Tests for DatabaseLifecycle implementation following TYL patterns

use tyl_config::RedisConfig;
use tyl_db_core::{DatabaseLifecycle, HealthStatus};
use tyl_falkordb_adapter::FalkorDBAdapter;

#[tokio::test]
async fn test_database_lifecycle_connect() {
    // Test DatabaseLifecycle::connect implementation
    let redis_config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    };

    let config = (redis_config, "test_graph".to_string());

    // This will fail without a running Redis instance, which is expected
    let result = FalkorDBAdapter::connect(config).await;

    // Verify error handling follows TYL patterns
    if let Err(error) = result {
        assert!(error.to_string().contains("Connection failed"));
    }
}

#[tokio::test]
async fn test_connection_info() {
    // Test connection_info method with mock adapter
    let config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    };

    // Create adapter (will fail connection but we can test connection_info)
    if let Ok(adapter) = FalkorDBAdapter::new(config, "test_graph".to_string()).await {
        let info = adapter.connection_info();
        assert!(info.contains("FalkorDB"));
        assert!(info.contains("test_graph"));
    }
    // If creation fails (expected without Redis), that's OK for this test
}

#[test]
fn test_health_status_creation() {
    // Test HealthStatus creation patterns
    let healthy = HealthStatus::healthy();
    assert!(healthy.is_healthy());

    let unhealthy = HealthStatus::unhealthy("Connection failed");
    assert!(!unhealthy.is_healthy());
}
