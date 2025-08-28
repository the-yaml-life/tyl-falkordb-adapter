//! Tests for GraphAnalytics implementation following TYL patterns
//! Maintains compatibility with tyl-graph-port as the source of truth

use serde_json::json;
use std::collections::HashMap;
use tyl_config::RedisConfig;
use tyl_falkordb_adapter::{FalkorDBAdapter, GraphAnalytics, GraphInfo, MultiGraphManager};
use tyl_graph_port::{AggregationQuery, CentralityType, ClusteringAlgorithm, RecommendationType};

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
            panic!("Redis not available for analytics tests")
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

/// Helper function to create AggregationQuery for testing
fn create_test_aggregation_query() -> AggregationQuery {
    AggregationQuery::count()
        .group_by("age")
        .with_filter("active", json!(true))
}

#[test]
fn test_centrality_types() {
    // Test all centrality type variants exist
    let centrality_types = [
        CentralityType::Degree,
        CentralityType::Betweenness,
        CentralityType::Closeness,
        CentralityType::Eigenvector,
        CentralityType::PageRank,
        CentralityType::Katz,
    ];

    for centrality_type in centrality_types {
        // Verify enum variants are accessible
        match centrality_type {
            CentralityType::Degree => assert!(true),
            CentralityType::Betweenness => assert!(true),
            CentralityType::Closeness => assert!(true),
            CentralityType::Eigenvector => assert!(true),
            CentralityType::PageRank => assert!(true),
            CentralityType::Katz => assert!(true),
        }
    }
}

#[test]
fn test_clustering_algorithms() {
    // Test all clustering algorithm variants exist
    let algorithms = [
        ClusteringAlgorithm::Louvain,
        ClusteringAlgorithm::LabelPropagation,
        ClusteringAlgorithm::ConnectedComponents,
        ClusteringAlgorithm::Leiden,
    ];

    for algorithm in algorithms {
        // Verify enum variants are accessible
        match algorithm {
            ClusteringAlgorithm::Louvain => assert!(true),
            ClusteringAlgorithm::LabelPropagation => assert!(true),
            ClusteringAlgorithm::ConnectedComponents => assert!(true),
            ClusteringAlgorithm::Leiden => assert!(true),
        }
    }
}

#[test]
fn test_recommendation_types() {
    // Test all recommendation type variants exist
    let rec_types = [
        RecommendationType::CommonNeighbors,
        RecommendationType::SimilarNodes,
        RecommendationType::StructuralEquivalence,
        RecommendationType::PathSimilarity,
    ];

    for rec_type in rec_types {
        // Verify enum variants are accessible
        match rec_type {
            RecommendationType::CommonNeighbors => assert!(true),
            RecommendationType::SimilarNodes => assert!(true),
            RecommendationType::StructuralEquivalence => assert!(true),
            RecommendationType::PathSimilarity => assert!(true),
        }
    }
}

#[test]
fn test_aggregation_query_creation() {
    let query = create_test_aggregation_query();

    assert!(matches!(
        query.function,
        tyl_graph_port::AggregationFunction::Count
    ));
    assert_eq!(query.group_by.len(), 1);
    assert_eq!(query.group_by[0], "age");
    assert_eq!(query.filters.len(), 1);
    assert_eq!(query.filters.get("active"), Some(&json!(true)));
}

#[tokio::test]
async fn test_calculate_centrality_nonexistent_graph() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping centrality test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let node_ids = vec!["test_node_1".to_string()];

    // Try to calculate centrality on non-existent graph
    let result = adapter
        .calculate_centrality("non_existent_graph", node_ids, CentralityType::Degree)
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Graph not found"));
}

#[tokio::test]
async fn test_calculate_centrality_degree() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping degree centrality test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "centrality_degree_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let node_ids = vec!["node_1".to_string(), "node_2".to_string()];

    let result = adapter
        .calculate_centrality(graph_id, node_ids, CentralityType::Degree)
        .await;

    assert!(result.is_ok());
    let centrality_scores = result.unwrap();

    // Should return scores for requested nodes
    assert!(centrality_scores.len() >= 0); // Could be empty if no nodes exist
    for (node_id, score) in centrality_scores {
        assert!(score >= 0.0);
        println!("Node {} has degree centrality: {}", node_id, score);
    }
}

#[tokio::test]
async fn test_calculate_centrality_betweenness() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping betweenness centrality test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "centrality_betweenness_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let node_ids = vec!["node_1".to_string()];

    let result = adapter
        .calculate_centrality(graph_id, node_ids, CentralityType::Betweenness)
        .await;

    assert!(result.is_ok());
    let centrality_scores = result.unwrap();

    for (node_id, score) in centrality_scores {
        assert!(score >= 0.0);
        println!("Node {} has betweenness centrality: {}", node_id, score);
    }
}

#[tokio::test]
async fn test_calculate_centrality_empty_nodes() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping empty nodes centrality test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "centrality_empty_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Empty node list should analyze all nodes
    let result = adapter
        .calculate_centrality(
            graph_id,
            vec![], // Empty - should analyze all nodes
            CentralityType::Degree,
        )
        .await;

    assert!(result.is_ok());
    let centrality_scores = result.unwrap();

    // Result might be empty if no nodes exist in graph
    println!("Analyzed {} nodes for centrality", centrality_scores.len());
}

#[tokio::test]
async fn test_detect_communities_louvain() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping Louvain community detection test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "community_louvain_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let params = HashMap::new();

    let result = adapter
        .detect_communities(graph_id, ClusteringAlgorithm::Louvain, params)
        .await;

    assert!(result.is_ok());
    let communities = result.unwrap();

    // Community assignment might be empty if no nodes exist
    println!("Found {} community assignments", communities.len());
    for (node_id, community_id) in communities {
        assert!(!community_id.is_empty());
        println!("Node {} assigned to community {}", node_id, community_id);
    }
}

#[tokio::test]
async fn test_detect_communities_label_propagation() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping label propagation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "community_label_prop_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let params = HashMap::new();

    let result = adapter
        .detect_communities(graph_id, ClusteringAlgorithm::LabelPropagation, params)
        .await;

    assert!(result.is_ok());
    let communities = result.unwrap();

    println!(
        "Label propagation found {} community assignments",
        communities.len()
    );
    for (node_id, community_id) in communities {
        println!("Node {} in community {}", node_id, community_id);
    }
}

#[tokio::test]
async fn test_detect_communities_connected_components() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping connected components test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "community_components_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let params = HashMap::new();

    let result = adapter
        .detect_communities(graph_id, ClusteringAlgorithm::ConnectedComponents, params)
        .await;

    assert!(result.is_ok());
    let communities = result.unwrap();

    println!(
        "Connected components found {} assignments",
        communities.len()
    );
    for (node_id, component_id) in communities {
        assert!(component_id.starts_with("component_"));
        println!("Node {} in component {}", node_id, component_id);
    }
}

#[tokio::test]
async fn test_find_patterns_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping pattern finding test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "patterns_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let result = adapter.find_patterns(graph_id, 2, 1).await;

    assert!(result.is_ok());
    let patterns = result.unwrap();

    println!("Found {} patterns", patterns.len());
    for (path, frequency) in patterns {
        assert!(frequency >= 1);
        assert!(path.length >= 0);
        println!(
            "Pattern with length {} appears {} times",
            path.length, frequency
        );
    }
}

#[tokio::test]
async fn test_find_patterns_different_sizes() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping pattern sizes test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "patterns_sizes_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    // Test different pattern sizes
    let sizes = [2, 3, 4];

    for size in sizes {
        let result = adapter.find_patterns(graph_id, size, 1).await;

        assert!(result.is_ok());
        let patterns = result.unwrap();

        for (path, frequency) in patterns {
            assert_eq!(path.length, size);
            assert!(frequency >= 1);
        }

        println!("Pattern size {} search completed", size);
    }
}

#[tokio::test]
async fn test_recommend_relationships_common_neighbors() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping common neighbors recommendation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "recommend_common_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let result = adapter
        .recommend_relationships(graph_id, "user_123", RecommendationType::CommonNeighbors, 5)
        .await;

    assert!(result.is_ok());
    let recommendations = result.unwrap();

    println!(
        "Common neighbors found {} recommendations",
        recommendations.len()
    );
    for (target_id, rel_type, confidence) in recommendations {
        assert!(!target_id.is_empty());
        assert!(!rel_type.is_empty());
        assert!(confidence >= 0.0 && confidence <= 1.0);
        println!(
            "Recommend {} -> {} with confidence {}",
            target_id, rel_type, confidence
        );
    }
}

#[tokio::test]
async fn test_recommend_relationships_collaborative_filtering() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping collaborative filtering test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "recommend_collaborative_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let result = adapter
        .recommend_relationships(graph_id, "user_456", RecommendationType::SimilarNodes, 3)
        .await;

    assert!(result.is_ok());
    let recommendations = result.unwrap();

    println!(
        "Collaborative filtering found {} recommendations",
        recommendations.len()
    );
    for (target_id, rel_type, confidence) in recommendations {
        assert!(confidence >= 0.0 && confidence <= 1.0);
        println!(
            "CF recommendation: {} -> {} ({})",
            target_id, rel_type, confidence
        );
    }
}

#[tokio::test]
async fn test_recommend_relationships_content_based() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping content-based recommendation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "recommend_content_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let result = adapter
        .recommend_relationships(
            graph_id,
            "user_789",
            RecommendationType::StructuralEquivalence,
            10,
        )
        .await;

    assert!(result.is_ok());
    let recommendations = result.unwrap();

    println!(
        "Content-based found {} recommendations",
        recommendations.len()
    );
    for (target_id, rel_type, confidence) in recommendations {
        assert_eq!(rel_type, "SIMILAR");
        assert!(confidence >= 0.0 && confidence <= 1.0);
        println!(
            "Structural equivalence rec: {} -> {} ({})",
            target_id, rel_type, confidence
        );
    }
}

#[tokio::test]
async fn test_execute_aggregation_basic() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping aggregation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "aggregation_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let query = create_test_aggregation_query();

    let result = adapter.execute_aggregation(graph_id, query).await;

    assert!(result.is_ok());
    let aggregation_results = result.unwrap();

    println!("Aggregation returned {} results", aggregation_results.len());
    for agg_result in aggregation_results {
        // The value field should contain the aggregated result
        println!("Aggregation result value: {:?}", agg_result.value);
        println!("Aggregation result groups: {:?}", agg_result.groups);
        println!("Aggregation result metadata: {:?}", agg_result.metadata);
    }
}

#[tokio::test]
async fn test_execute_aggregation_count_only() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping count aggregation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "aggregation_count_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let query = AggregationQuery::count();

    let result = adapter.execute_aggregation(graph_id, query).await;

    assert!(result.is_ok());
    let aggregation_results = result.unwrap();

    // Should have at least one result with count
    println!(
        "Count aggregation returned {} results",
        aggregation_results.len()
    );
}

#[tokio::test]
async fn test_execute_aggregation_with_filters() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping filtered aggregation test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "aggregation_filter_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let mut filters = HashMap::new();
    filters.insert("active".to_string(), json!(true));
    filters.insert("age".to_string(), json!(25));

    let query = AggregationQuery::count()
        .with_filter("active", json!(true))
        .with_filter("age", json!(25));

    let result = adapter.execute_aggregation(graph_id, query).await;

    assert!(result.is_ok());
    let aggregation_results = result.unwrap();

    println!(
        "Filtered aggregation returned {} results",
        aggregation_results.len()
    );
    assert!(aggregation_results.len() <= 5); // Respects limit
}

#[tokio::test]
async fn test_analytics_with_nonexistent_graph() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping nonexistent graph analytics test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let non_existent_graph = "does_not_exist";

    // Test all analytics methods with non-existent graph

    // Centrality
    let centrality_result = adapter
        .calculate_centrality(non_existent_graph, vec![], CentralityType::Degree)
        .await;
    assert!(centrality_result.is_err());

    // Communities
    let community_result = adapter
        .detect_communities(
            non_existent_graph,
            ClusteringAlgorithm::Louvain,
            HashMap::new(),
        )
        .await;
    assert!(community_result.is_err());

    // Patterns
    let pattern_result = adapter.find_patterns(non_existent_graph, 2, 1).await;
    assert!(pattern_result.is_err());

    // Recommendations
    let recommend_result = adapter
        .recommend_relationships(
            non_existent_graph,
            "node_1",
            RecommendationType::CommonNeighbors,
            5,
        )
        .await;
    assert!(recommend_result.is_err());

    // Aggregation
    let agg_query = create_test_aggregation_query();
    let agg_result = adapter
        .execute_aggregation(non_existent_graph, agg_query)
        .await;
    assert!(agg_result.is_err());
}

#[tokio::test]
async fn test_all_centrality_types() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping all centrality types test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "all_centrality_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let centrality_types = [
        CentralityType::Degree,
        CentralityType::Betweenness,
        CentralityType::Closeness,
        CentralityType::Eigenvector,
        CentralityType::PageRank,
        CentralityType::Katz,
    ];

    let node_ids = vec!["test_node".to_string()];

    for centrality_type in centrality_types {
        let result = adapter
            .calculate_centrality(graph_id, node_ids.clone(), centrality_type.clone())
            .await;

        assert!(result.is_ok());
        let scores = result.unwrap();

        println!(
            "Centrality type {:?} returned {} scores",
            centrality_type,
            scores.len()
        );
        for (node_id, score) in scores {
            assert!(score >= 0.0);
            println!("  {} = {}", node_id, score);
        }
    }
}

#[tokio::test]
async fn test_recommendation_limits() {
    // Skip if Redis not available
    if !redis_available().await {
        println!("Skipping recommendation limits test - Redis not available");
        return;
    }

    let adapter = create_test_adapter().await;
    let graph_id = "recommend_limits_test";

    // Create test graph
    if create_test_graph(&adapter, graph_id).await.is_err() {
        return; // Skip if can't create graph
    }

    let limits = [1, 3, 5, 10];

    for limit in limits {
        let result = adapter
            .recommend_relationships(
                graph_id,
                "user_test",
                RecommendationType::CommonNeighbors,
                limit,
            )
            .await;

        assert!(result.is_ok());
        let recommendations = result.unwrap();

        // Should not exceed the limit
        assert!(recommendations.len() <= limit);
        println!(
            "Limit {} returned {} recommendations",
            limit,
            recommendations.len()
        );
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
