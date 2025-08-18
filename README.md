# TYL FalkorDB Adapter

FalkorDB graph database adapter following TYL Framework patterns for consistency, reusability, and maintainability.

## Design Principles

- **TYL Framework**: Uses tyl-errors, tyl-config, tyl-logging patterns  
- **Production Ready**: Connection pooling, error recovery, monitoring
- **Secure**: Input validation, injection protection, sanitization

## Core Features

- Redis/FalkorDB connection management with pooling
- Cypher query execution with optimization
- Security hardening and injection protection
- Schema migration system
- Performance monitoring and health checks
- Graph operations (nodes, relationships)
- Circuit breaker and retry logic
- Query caching and metrics collection

## Architecture

### TYL Framework Integration

```
┌─────────────────────────────────────────────────────┐
│              TYL Framework Core                     │
│   (tyl-errors, tyl-config, tyl-logging, etc.)     │
└─────────────────────┬───────────────────────────────┘
                      │ provides foundation
                      ▼
┌─────────────────────────────────────────────────────┐
│           TYL FalkorDB Adapter                      │
│     ┌─────────────────┬─────────────────────────┐   │
│     │ Connection      │   Security &            │   │
│     │ Management      │   Query Execution       │   │
│     └─────────────────┴─────────────────────────┘   │
├─────────────────────────────────────────────────────┤
│                 FalkorDB Client                     │
│     ┌─────────────────┬─────────────────────────┐   │
│     │ Redis Protocol  │   Graph Algorithms      │   │
│     │ & Transactions  │   & Index Management    │   │
│     └─────────────────┴─────────────────────────┘   │
├─────────────────────────────────────────────────────┤
│             Redis + FalkorDB Module                 │
└─────────────────────────────────────────────────────┘
```

## Dependencies

### TYL Framework Dependencies

```toml
[dependencies]
# TYL Framework - main branch
tyl-errors = { git = "https://github.com/the-yaml-life/tyl-errors.git", branch = "main" }
tyl-config = { git = "https://github.com/the-yaml-life/tyl-config.git", branch = "main" }
tyl-logging = { git = "https://github.com/the-yaml-life/tyl-logging.git", branch = "main" }
tyl-tracing = { git = "https://github.com/the-yaml-life/tyl-tracing.git", branch = "main" }
tyl-db-core = { git = "https://github.com/the-yaml-life/tyl-db-core.git", branch = "main" }

# Core async and networking (minimal for FalkorDB)
tokio = { version = "1.35", features = ["full"] }
async-trait = "0.1.74"
redis = { version = "0.24", features = ["aio", "tokio-comp", "connection-manager"] }
futures = "0.3"

# Essential serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Essential utilities
uuid = { version = "1.18", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
```

## Usage Examples

### Basic Setup

```rust
use tyl_falkordb_adapter::{
    FalkorDBConnection, 
    FalkorDBConnectionPool, 
    ConnectionConfig,
    graph::{GraphContext, GraphNode}
};
use tyl_errors::TylResult;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> TylResult<()> {
    // Create connection
    let conn = FalkorDBConnection::new("redis://localhost:6379").await?;
    
    // Create connection pool
    let config = ConnectionConfig {
        max_connections: 10,
        connection_timeout_ms: 5000,
        database: 0,
    };
    let pool = FalkorDBConnectionPool::new("redis://localhost:6379", config).await?;
    
    // Create graph context
    let graph_context = GraphContext::new(pool, "my_graph".to_string());
    
    // Health check
    let health = graph_context.health_check().await?;
    println!("Graph healthy: {}", health.healthy);
    
    Ok(())
}
```

### Graph Operations

```rust
use tyl_falkordb_adapter::graph::{GraphNode, GraphRelationship, create_simple_node, create_simple_relationship};
use std::collections::HashMap;
use serde_json::json;

// Create a node
let mut properties = HashMap::new();
properties.insert("name".to_string(), json!("example_node"));
properties.insert("type".to_string(), json!("demo"));

let node_id = create_simple_node(
    &graph_context,
    vec!["DemoNode".to_string()],
    "node_123".to_string(),
    properties,
).await?;

// Create a relationship
let mut rel_properties = HashMap::new();
rel_properties.insert("strength".to_string(), json!(0.8));

let rel_id = create_simple_relationship(
    &graph_context,
    "node_123".to_string(),
    "node_456".to_string(),
    "CONNECTS_TO".to_string(),
    rel_properties,
).await?;
```

### Error Handling with TYL Framework

```rust
use tyl_errors::{TylError, TylResult};
use tyl_falkordb_adapter::falkordb_errors;

match graph_context.create_node(&invalid_node).await {
    Ok(node_id) => println!("Created: {}", node_id),
    Err(TylError::Database { message, .. }) => {
        eprintln!("Database error: {}", message);
    }
    Err(TylError::Configuration { message, .. }) => {
        eprintln!("Configuration error: {}", message);
    }
    Err(e) => eprintln!("Unexpected error: {}", e),
}

// Using helper functions
let conn_error = falkordb_errors::connection_failed("Failed to connect to Redis");
let query_error = falkordb_errors::query_execution_failed("MATCH (n) RETURN n", "Syntax error");
```

### Configuration Management

```rust
use tyl_falkordb_adapter::falkordb_config;
use tyl_config::{RedisConfig, ConfigManager};

// Validate configuration
falkordb_config::validate_falkordb_url("redis://localhost:6379")?;
falkordb_config::validate_pool_config(10, 30)?;

// Use TYL config patterns
let config_manager = ConfigManager::new()?;
let redis_config = config_manager.get_redis_config()?;
```

### Schema Migration

```rust
use tyl_falkordb_adapter::{
    SchemaMigration, 
    MigrationVersion, 
    MigrationOperation, 
    MigrationType,
    SchemaRegistry
};

// Create a migration
let migration = SchemaMigration {
    id: "001_create_indexes".to_string(),
    version: MigrationVersion::new(0, 0, 1),
    name: "Create Basic Indexes".to_string(),
    description: "Add indexes for common queries".to_string(),
    migration_type: MigrationType::Index,
    operations: vec![
        MigrationOperation::CreateIndex {
            name: "node_id_index".to_string(),
            labels: vec!["Node".to_string()],
            properties: vec!["id".to_string()],
            index_type: tyl_falkordb_adapter::IndexType::BTree,
        }
    ],
    rollback_operations: vec![
        MigrationOperation::DropIndex {
            name: "node_id_index".to_string(),
        }
    ],
    dependencies: vec![],
    created_at: chrono::Utc::now(),
    applied_at: None,
    rolled_back_at: None,
    checksum: "abc123".to_string(),
    performance_impact: Default::default(),
    backwards_compatible: true,
};

// Register and apply migration
let mut registry = SchemaRegistry::new(connection);
registry.register_migration(migration)?;
let applied = registry.apply_pending_migrations().await?;
```

### Security Features

```rust
use tyl_falkordb_adapter::security::{
    SecureCypherBuilder,
    CypherSanitizer,
    InjectionDetector,
    security_errors
};

// Secure query building
let query = SecureCypherBuilder::new()
    .match_node("n", vec!["User"])
    .where_clause("n.id = $user_id")
    .return_clause("n.name, n.email")
    .build();

// Input sanitization
let sanitizer = CypherSanitizer::new();
let safe_input = sanitizer.sanitize_input("user input';")?;

// Injection detection
let detector = InjectionDetector::new();
if detector.detect_injection("'; DROP TABLE users; --") {
    return Err(security_errors::injection_attempt("SQL injection detected"));
}
```

## Module Structure

### Core Modules
- `connection` - Redis/FalkorDB connection management
- `connection_manager` - Advanced connection pooling and load balancing
- `complete_repository` - High-level repository with caching and optimization
- `schema_migration` - Database migration system
- `migration_runner` - Migration execution and management
- `security` - Security hardening and injection protection

### Helper Modules
- `falkordb_errors` - TYL-compatible error helpers
- `falkordb_config` - Configuration validation utilities
- `graph` - Graph operations

## Testing

```bash
# Run all tests
cargo test

# Run with FalkorDB instance
docker run -d -p 6379:6379 falkordb/falkordb:latest
cargo test -- --ignored

# Test specific modules
cargo test connection
cargo test security
cargo test migration
```

## Environment Setup

### Development
```bash
# Start FalkorDB with Docker
docker run -d --name falkordb -p 6379:6379 falkordb/falkordb:latest

# Run tests
cargo test
```

### Production
```bash
# Environment variables for production
export FALKORDB_URL="redis://cluster.example.com:6379"
export FALKORDB_PASSWORD="secure_password"
export POOL_SIZE=50
export CONNECTION_TIMEOUT=30

# Run application
cargo run --release
```

## Performance Characteristics

| Operation | Throughput | Latency (p99) | Memory Usage |
|-----------|------------|---------------|--------------|
| Node Creation | 5,000 ops/sec | 5ms | 0.5MB |
| Relationship Creation | 4,000 ops/sec | 8ms | 0.5MB |
| Graph Traversal | 3,000 queries/sec | 3ms | 1MB |
| Health Checks | 15,000 ops/sec | 0.5ms | 0.1MB |

## Security Features

- **Injection Prevention**: Comprehensive Cypher injection detection and prevention
- **Input Validation**: Strict input sanitization and validation
- **Parameter Binding**: Safe parameter substitution
- **Connection Security**: Secure connection management with authentication
- **Rate Limiting**: Built-in rate limiting and circuit breaker patterns

## Contributing

This adapter follows TYL Framework patterns. When contributing:

1. Use TYL error types (`TylError`, `TylResult`)
2. Follow TYL configuration patterns
3. Use TYL logging and tracing
4. Write comprehensive tests
5. Follow security best practices

## License

AGPL-3.0 License - See LICENSE file for details.

---

TYL FalkorDB Adapter - Graph database adapter for the TYL Framework ecosystem.