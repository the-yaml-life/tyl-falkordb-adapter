//! Integration tests for TYL FalkorDB Adapter
//!
//! This module contains integration tests that validate the adapter's behavior
//! in different scenarios:
//!
//! - Compatibility with MockGraphStore from tyl-graph-port
//! - Full integration tests with real FalkorDB via docker-compose
//! - End-to-end functionality validation

pub mod test_falkordb_integration;
pub mod test_mock_compatibility;
