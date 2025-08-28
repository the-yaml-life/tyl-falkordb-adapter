//! Basic usage example for TYL FalkorDB Adapter
//!
//! This example demonstrates basic graph operations using the FalkorDB adapter
//! following TYL framework patterns.

use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_db_core::DatabaseLifecycle;
use tyl_errors::TylResult;
use tyl_falkordb_adapter::{
    FalkorDBAdapter, GraphInfo, GraphNode, GraphRelationship, GraphStore, MultiGraphManager,
};

#[tokio::main]
async fn main() -> TylResult<()> {
    println!("🚀 TYL FalkorDB Adapter - Basic Usage Example");
    println!("===============================================\n");

    // Initialize FalkorDB adapter with TYL configuration
    let config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 5,
        timeout_seconds: 10,
    };

    println!("🔗 Connecting to FalkorDB...");
    let adapter = match FalkorDBAdapter::new(config).await {
        Ok(adapter) => {
            println!("✅ Connected successfully!");
            adapter
        }
        Err(e) => {
            println!("❌ Failed to connect: {}", e);
            println!(
                "💡 Make sure FalkorDB (Redis with FalkorDB module) is running on localhost:6379"
            );
            return Err(e);
        }
    };

    // Create graph using MultiGraphManager
    println!("🗂️  Creating graph...");
    let graph_info = GraphInfo {
        id: "example_graph".to_string(),
        name: "Example Graph".to_string(),
        metadata: HashMap::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    match adapter.create_graph(graph_info).await {
        Ok(_) => println!("✅ Graph created successfully!"),
        Err(e) => println!("⚠️  Graph creation failed (might already exist): {}", e),
    }

    // Test health check using DatabaseLifecycle trait
    println!("\n🏥 Checking adapter health...");
    match adapter.health_check().await {
        Ok(health_result) => {
            if health_result.status.is_healthy() {
                println!("✅ Adapter is healthy!");
            } else {
                println!("⚠️  Adapter health check failed");
            }
        }
        Err(e) => println!("❌ Health check error: {}", e),
    }

    // Create example nodes
    println!("\n👤 Creating user node...");
    let mut user_node = GraphNode::new();
    user_node.id = "user_alice".to_string();
    user_node.labels.push("User".to_string());
    user_node.labels.push("Person".to_string());
    user_node
        .properties
        .insert("name".to_string(), json!("Alice Johnson"));
    user_node.properties.insert("age".to_string(), json!(28));
    user_node
        .properties
        .insert("email".to_string(), json!("alice@example.com"));

    match adapter.create_node("example_graph", user_node).await {
        Ok(node_id) => println!("✅ Created user node: {}", node_id),
        Err(e) => println!("❌ Failed to create user node: {}", e),
    }

    println!("\n📦 Creating product node...");
    let mut product_node = GraphNode::new();
    product_node.id = "product_laptop".to_string();
    product_node.labels.push("Product".to_string());
    product_node
        .properties
        .insert("name".to_string(), json!("Gaming Laptop"));
    product_node
        .properties
        .insert("price".to_string(), json!(1299.99));
    product_node
        .properties
        .insert("category".to_string(), json!("Electronics"));

    match adapter.create_node("example_graph", product_node).await {
        Ok(node_id) => println!("✅ Created product node: {}", node_id),
        Err(e) => println!("❌ Failed to create product node: {}", e),
    }

    // Create relationship
    println!("\n🔗 Creating purchase relationship...");
    let mut purchase_rel = GraphRelationship::new(
        "purchase_001".to_string(),
        "user_alice".to_string(),
        "product_laptop".to_string(),
    );
    purchase_rel.relationship_type = "PURCHASED".to_string();
    purchase_rel
        .properties
        .insert("date".to_string(), json!("2024-01-15"));
    purchase_rel
        .properties
        .insert("amount".to_string(), json!(1299.99));
    purchase_rel
        .properties
        .insert("payment_method".to_string(), json!("credit_card"));

    match adapter
        .create_relationship("example_graph", purchase_rel)
        .await
    {
        Ok(rel_id) => println!("✅ Created relationship: {}", rel_id),
        Err(e) => println!("❌ Failed to create relationship: {}", e),
    }

    // Query nodes
    println!("\n🔍 Querying user node...");
    match adapter.get_node("example_graph", "user_alice").await {
        Ok(Some(_node)) => println!("✅ Found user node: user_alice"),
        Ok(None) => println!("⚠️  User node not found"),
        Err(e) => println!("❌ Query failed: {}", e),
    }

    // Execute custom Cypher query
    println!("\n📊 Executing custom Cypher query...");
    let cypher_query =
        "MATCH (u:User)-[p:PURCHASED]->(pr:Product) RETURN u.name, pr.name, p.amount";
    match adapter.execute_cypher("example_graph", cypher_query).await {
        Ok(result) => {
            println!("✅ Query executed successfully!");
            println!(
                "📋 Result: {}",
                serde_json::to_string_pretty(&result)
                    .unwrap_or_else(|_| "Could not format result".to_string())
            );
        }
        Err(e) => println!("❌ Query failed: {}", e),
    }

    // Test DatabaseLifecycle trait
    println!("\n🔄 Testing DatabaseLifecycle integration...");
    let lifecycle_config = RedisConfig {
        url: Some("redis://localhost:6379".to_string()),
        host: "localhost".to_string(),
        port: 6379,
        password: None,
        database: 0,
        pool_size: 3,
        timeout_seconds: 5,
    };

    match FalkorDBAdapter::connect(lifecycle_config).await {
        Ok(lifecycle_adapter) => {
            println!("✅ DatabaseLifecycle::connect() successful!");

            // Test health check via DatabaseLifecycle trait
            match DatabaseLifecycle::health_check(&lifecycle_adapter).await {
                Ok(health_result) => {
                    if health_result.status.is_healthy() {
                        println!("✅ DatabaseLifecycle health check passed!");
                    } else {
                        println!("⚠️  DatabaseLifecycle health check shows unhealthy");
                    }
                }
                Err(e) => println!("❌ DatabaseLifecycle health check failed: {}", e),
            }

            // Show connection info
            let info = lifecycle_adapter.connection_info();
            println!("📄 Connection info: {}", info);
        }
        Err(e) => println!("❌ DatabaseLifecycle::connect() failed: {}", e),
    }

    println!("\n🎉 Example completed!");
    println!("💡 This example demonstrates TYL Framework integration with FalkorDB.");
    println!("📚 Check the tests/ directory for more detailed usage examples.");

    Ok(())
}
