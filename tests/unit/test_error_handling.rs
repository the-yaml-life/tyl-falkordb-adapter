//! Tests for error handling following TYL patterns

use tyl_errors::TylError;
use tyl_falkordb_adapter::falkordb_errors;

#[test]
fn test_connection_error_categorization() {
    let error = falkordb_errors::connection_failed("timeout");

    // Verify it's a database error (retriable category)
    assert!(error.to_string().contains("FalkorDB connection failed"));

    // Test error formatting
    let error_string = format!("{error}");
    assert!(error_string.contains("timeout"));
}

#[test]
fn test_query_error_with_context() {
    let cypher_query = "CREATE (n:User {name: 'John'}) RETURN n";
    let error_msg = "syntax error at position 15";

    let error = falkordb_errors::query_execution_failed(cypher_query, error_msg);

    let error_string = error.to_string();
    assert!(error_string.contains("CREATE (n:User"));
    assert!(error_string.contains("syntax error"));
}

#[test]
fn test_not_found_errors() {
    let node_error = falkordb_errors::node_not_found("missing_node_123");
    assert!(node_error.to_string().contains("graph_node"));
    assert!(node_error.to_string().contains("missing_node_123"));

    let rel_error = falkordb_errors::relationship_not_found("missing_rel_456");
    assert!(rel_error.to_string().contains("graph_relationship"));
    assert!(rel_error.to_string().contains("missing_rel_456"));
}

#[test]
fn test_validation_errors() {
    let validation_error = falkordb_errors::invalid_graph_data("node_id", "cannot be empty");

    // Verification that it follows TYL validation patterns
    assert!(validation_error.to_string().contains("cannot be empty"));
}

#[test]
fn test_error_chain_compatibility() {
    // Test that our errors can be chained with other TYL errors
    let base_error = falkordb_errors::connection_failed("Redis unavailable");

    // This should work with TYL error handling patterns
    let wrapped_error = TylError::internal(format!("Adapter initialization failed: {base_error}"));

    assert!(wrapped_error
        .to_string()
        .contains("Adapter initialization failed"));
    assert!(wrapped_error.to_string().contains("Redis unavailable"));
}
