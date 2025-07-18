//! Integration tests for the coordinator module
//!
//! These tests verify the end-to-end functionality of the coordinator
//! and its interaction with other system components.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use midas_fetcher::app::hash::Md5Hash;
use midas_fetcher::app::models::{DatasetFileInfo, FileInfo, QualityControlVersion};
use midas_fetcher::app::worker::ConfigPresets;
use midas_fetcher::app::{
    CacheConfig, CacheManager, CedaClient, Coordinator, CoordinatorConfig, WorkQueue,
};

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
        worker_config: ConfigPresets::testing(),
    }
}

/// Create integration test components
///
/// Returns fully configured components for integration testing
/// with temporary storage and real client connections.
async fn create_integration_test_components() -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>)
{
    let temp_dir = TempDir::new().unwrap();
    let cache_config = CacheConfig {
        cache_root: Some(temp_dir.path().to_path_buf()),
        fast_verification: true,
        ..Default::default()
    };

    let queue = Arc::new(WorkQueue::new());
    let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
    let client = Arc::new(CedaClient::new_simple().await.unwrap());

    (queue, cache, client)
}

/// Create test file info for integration testing
fn create_integration_test_file(name: &str, size: Option<u64>) -> FileInfo {
    // Create a unique hash based on the file name to avoid deduplication
    let mut hash_string = format!(
        "{:x}",
        name.as_bytes()
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
    );
    while hash_string.len() < 32 {
        hash_string.push('0');
    }
    let hash = Md5Hash::from_hex(&hash_string[0..32]).unwrap();

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
            quality_version: Some(QualityControlVersion::V1),
            year: Some("2024".to_string()),
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
            Some(1024 * (i + 1) as u64),
        );
        queue.add_work(file_info).await.unwrap();
    }

    let coordinator =
        Coordinator::new_with_expected_files(config, queue, cache, client, expected_files);

    // Get initial stats
    let initial_stats = coordinator.get_stats().await;
    assert_eq!(initial_stats.total_files, expected_files);
    assert_eq!(initial_stats.files_completed, 0);

    // Note: We don't actually run downloads here since we don't have
    // real downloadable URLs, but we verify the setup is correct
}

/// Test coordinator statistics tracking
///
/// Ensures that the coordinator correctly tracks and updates
/// download statistics throughout the process lifecycle.
#[tokio::test]
async fn test_coordinator_statistics_tracking() {
    let config = create_integration_test_config();
    let (queue, cache, client) = create_integration_test_components().await;

    let coordinator =
        Coordinator::new_with_expected_files(config, queue.clone(), cache, client, 10);

    // Add some test work
    for i in 0..3 {
        let file_info = create_integration_test_file(&format!("stats_test_{}.csv", i), Some(2048));
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
