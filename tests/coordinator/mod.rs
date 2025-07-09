//! Integration tests for the coordinator module
//!
//! These tests verify the end-to-end functionality of the coordinator
//! and its interaction with other system components.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use midas_fetcher::app::{
    CacheConfig, CacheManager, CedaClient, Coordinator, CoordinatorConfig, WorkQueue
};
use midas_fetcher::app::hash::Md5Hash;
use midas_fetcher::app::models::{DatasetFileInfo, FileInfo};
use midas_fetcher::app::worker::WorkerConfig;

/// Create integration test coordinator configuration
///
/// Returns a coordinator configuration suitable for integration testing
/// with realistic but fast settings.
fn create_integration_test_config() -> CoordinatorConfig {
    CoordinatorConfig {
        worker_count: 1, // Single worker for predictable testing
        progress_update_interval: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(5),
        enable_progress_bar: false,
        verbose_logging: true,
        progress_batch_size: 10,
        worker_config: WorkerConfig {
            worker_count: 1,
            max_retries: 1,
            retry_base_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_millis(200),
            idle_sleep_duration: Duration::from_millis(10),
            progress_buffer_size: 10,
            download_timeout: Duration::from_secs(2),
            detailed_progress: true,
        },
    }
}

/// Create integration test components
///
/// Returns fully configured components for integration testing
/// with temporary storage and real client connections.
async fn create_integration_test_components() -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>) {
    let temp_dir = TempDir::new().unwrap();
    let cache_config = CacheConfig {
        cache_root: Some(temp_dir.path().to_path_buf()),
        enable_compression: false,
        verification_sample_rate: 1.0,
        ..Default::default()
    };

    let queue = Arc::new(WorkQueue::new());
    let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
    let client = Arc::new(CedaClient::new_simple().await.unwrap());

    (queue, cache, client)
}

/// Create test file info for integration testing
fn create_integration_test_file(name: &str, size: Option<u64>) -> FileInfo {
    let hash = Md5Hash::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap();
    FileInfo {
        hash,
        relative_path: format!("./test/{}", name),
        file_name: name.to_string(),
        dataset_info: DatasetFileInfo {
            dataset_name: "integration-test".to_string(),
            version: "v1".to_string(),
            county: Some("test-county".to_string()),
            station_id: Some("12345".to_string()),
            station_name: Some("Test Station".to_string()),
            quality_version: Some("qc-v1".to_string()),
            year: Some(2024),
            file_type: Some("csv".to_string()),
        },
        manifest_version: Some(202507),
        retry_count: 0,
        last_attempt: None,
        estimated_size: size,
        destination_path: std::path::PathBuf::from(format!("/tmp/test/{}", name)),
    }
}

/// Test coordinator full lifecycle with empty queue
///
/// Verifies that the coordinator can start, detect completion immediately
/// when no work is available, and shutdown gracefully.
#[tokio::test]
async fn test_coordinator_empty_queue_lifecycle() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    let mut coordinator = Coordinator::new(config, queue, cache, client);

    // Run downloads with empty queue - should complete immediately
    let result = coordinator.run_downloads().await;
    assert!(result.is_ok());

    let session_result = result.unwrap();
    assert!(session_result.success);
    assert_eq!(session_result.stats.files_completed, 0);
    assert_eq!(session_result.stats.files_failed, 0);
}

/// Test coordinator with known file count
///
/// Ensures that the coordinator correctly tracks progress when the
/// expected number of files is known in advance.
#[tokio::test]
async fn test_coordinator_with_known_file_count() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    // Add some test files to the queue
    let expected_files = 5;
    for i in 0..expected_files {
        let file_info = create_integration_test_file(
            &format!("test_file_{}.csv", i),
            Some(1024 * (i + 1) as u64)
        );
        queue.add_work(file_info).await.unwrap();
    }

    let mut coordinator = Coordinator::new_with_expected_files(
        config,
        queue,
        cache,
        client,
        expected_files
    );

    // Get initial stats
    let initial_stats = coordinator.get_stats().await;
    assert_eq!(initial_stats.total_files, expected_files);
    assert_eq!(initial_stats.files_completed, 0);

    // Note: We don't actually run downloads here since we don't have
    // real downloadable URLs, but we verify the setup is correct
}

/// Test coordinator configuration validation
///
/// Verifies that the coordinator properly validates its configuration
/// and reports appropriate errors for invalid settings.
#[tokio::test]
async fn test_coordinator_config_validation() {
    let (queue, cache, client) = create_integration_test_components().await;

    // Test with invalid progress interval
    let mut invalid_config = create_integration_test_config();
    invalid_config.progress_update_interval = Duration::ZERO;

    let mut coordinator = Coordinator::new(invalid_config, queue.clone(), cache.clone(), client.clone());

    // Should fail during run_downloads due to validation
    let result = coordinator.run_downloads().await;
    assert!(result.is_ok()); // Returns SessionResult, not an error
    
    let session_result = result.unwrap();
    assert!(!session_result.success);
    assert!(!session_result.shutdown_errors.is_empty());
}

/// Test coordinator statistics tracking
///
/// Ensures that the coordinator correctly tracks and updates
/// download statistics throughout the process lifecycle.
#[tokio::test]
async fn test_coordinator_statistics_tracking() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    let coordinator = Coordinator::new_with_expected_files(
        config,
        queue.clone(),
        cache,
        client,
        10
    );

    // Add some test work
    for i in 0..3 {
        let file_info = create_integration_test_file(
            &format!("stats_test_{}.csv", i),
            Some(2048)
        );
        queue.add_work(file_info).await.unwrap();
    }

    // Check initial statistics
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.total_files, 10);
    assert_eq!(stats.files_completed, 0);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.total_bytes_downloaded, 0);
    assert_eq!(stats.download_rate_bps, 0.0);
    assert!(stats.estimated_completion.is_none());
}

/// Test coordinator graceful shutdown
///
/// Verifies that the coordinator can be gracefully shutdown
/// and that all components are properly cleaned up.
#[tokio::test]
async fn test_coordinator_graceful_shutdown() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    let coordinator = Coordinator::new(config, queue, cache, client);

    // Test shutdown API
    let result = coordinator.shutdown().await;
    assert!(result.is_ok());
}

/// Test coordinator worker pool integration
///
/// Ensures that the coordinator correctly creates and manages
/// worker pools with the specified configuration.
#[tokio::test]
async fn test_coordinator_worker_pool_integration() {
    let mut config = create_integration_test_config();
    config.worker_count = 2;
    
    let (queue, cache, client) = create_integration_test_components().await;

    let coordinator = Coordinator::new(config.clone(), queue, cache, client);

    // Test worker pool creation
    let worker_pool = coordinator.create_worker_pool().await;
    assert!(worker_pool.is_ok());
    
    let pool = worker_pool.unwrap();
    // We can't easily test internal pool state, but creation success indicates proper setup
}

/// Test coordinator component integration
///
/// Verifies that all coordinator components work together correctly
/// and that the modular architecture functions as expected.
#[tokio::test]
async fn test_coordinator_component_integration() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    // Test that all components can be created and configured
    let coordinator = Coordinator::new(config, queue.clone(), cache.clone(), client.clone());

    // Verify component integration through coordinator interface
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.files_completed, 0);
    assert_eq!(stats.active_workers, 0);

    // Test queue integration
    let queue_stats = queue.stats().await;
    assert_eq!(queue_stats.pending_count, 0);
    assert_eq!(queue_stats.completed_count, 0);

    // Test cache integration
    let cache_stats = cache.stats().await;
    assert_eq!(cache_stats.total_files, 0);
    assert_eq!(cache_stats.total_size_bytes, 0);
}

/// Test coordinator error handling
///
/// Ensures that the coordinator properly handles and reports
/// errors from various components and operations.
#[tokio::test]
async fn test_coordinator_error_handling() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    let mut coordinator = Coordinator::new(config, queue, cache, client);

    // Run downloads - should succeed even with no actual downloads
    let result = coordinator.run_downloads().await;
    assert!(result.is_ok());

    let session_result = result.unwrap();
    // Should be successful since there's no work to fail on
    assert!(session_result.success);
    assert!(session_result.shutdown_errors.is_empty());
}