//! Tests for graph operations following TYL patterns

use serde_json::json;
use tyl_falkordb_adapter::{GraphNode, GraphRelationship};

#[test]
fn test_graph_node_with_properties() {
    let mut node = GraphNode::new();
    node.id = "user_123".to_string();

    // Add labels
    node.labels.push("User".to_string());
    node.labels.push("Person".to_string());

    // Add properties
    node.properties
        .insert("name".to_string(), json!("John Doe"));
    node.properties.insert("age".to_string(), json!(30));
    node.properties.insert("active".to_string(), json!(true));

    assert_eq!(node.labels.len(), 2);
    assert!(node.labels.contains(&"User".to_string()));
    assert!(node.labels.contains(&"Person".to_string()));

    assert_eq!(node.properties.len(), 3);
    assert_eq!(node.properties.get("name"), Some(&json!("John Doe")));
    assert_eq!(node.properties.get("age"), Some(&json!(30)));
    assert_eq!(node.properties.get("active"), Some(&json!(true)));
}

#[test]
fn test_graph_relationship_with_properties() {
    let mut relationship = GraphRelationship::new(
        "follows_123".to_string(),
        "user_1".to_string(),
        "user_2".to_string(),
    );
    relationship.relationship_type = "FOLLOWS".to_string();

    // Add properties
    relationship
        .properties
        .insert("since".to_string(), json!("2024-01-15"));
    relationship
        .properties
        .insert("weight".to_string(), json!(0.8));
    relationship
        .properties
        .insert("verified".to_string(), json!(true));

    assert_eq!(relationship.properties.len(), 3);
    assert_eq!(
        relationship.properties.get("since"),
        Some(&json!("2024-01-15"))
    );
    assert_eq!(relationship.properties.get("weight"), Some(&json!(0.8)));
    assert_eq!(relationship.properties.get("verified"), Some(&json!(true)));
}

#[test]
fn test_node_serialization() {
    let mut node = GraphNode::new();
    node.id = "test_node".to_string();
    node.labels.push("TestLabel".to_string());
    node.properties
        .insert("test_prop".to_string(), json!("test_value"));

    // Test that node can be serialized (important for Redis storage)
    let serialized = serde_json::to_string(&node);
    assert!(serialized.is_ok());

    // Test deserialization
    let json_str = serialized.unwrap();
    let deserialized: Result<GraphNode, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let recovered_node = deserialized.unwrap();
    assert_eq!(recovered_node.id, node.id);
    assert_eq!(recovered_node.labels, node.labels);
}

#[test]
fn test_relationship_serialization() {
    let mut relationship = GraphRelationship::new(
        "test_rel".to_string(),
        "node_a".to_string(),
        "node_b".to_string(),
    );
    relationship.relationship_type = "TEST_TYPE".to_string();
    relationship
        .properties
        .insert("test_prop".to_string(), json!(42));

    // Test serialization
    let serialized = serde_json::to_string(&relationship);
    assert!(serialized.is_ok());

    // Test deserialization
    let json_str = serialized.unwrap();
    let deserialized: Result<GraphRelationship, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let recovered_rel = deserialized.unwrap();
    assert_eq!(recovered_rel.id, relationship.id);
    assert_eq!(recovered_rel.from_node_id, relationship.from_node_id);
    assert_eq!(recovered_rel.to_node_id, relationship.to_node_id);
    assert_eq!(
        recovered_rel.relationship_type,
        relationship.relationship_type
    );
}
