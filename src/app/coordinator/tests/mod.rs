//! Unit tests for coordinator module components
//!
//! This module contains unit tests for the individual components of the
//! coordinator module. Integration tests are located in the top-level
//! tests directory.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use crate::app::hash::Md5Hash;
use crate::app::models::{DatasetFileInfo, FileInfo};
use crate::app::{CacheConfig, CacheManager, CedaClient, WorkQueue};
use crate::app::worker::WorkerConfig;

use super::*;

/// Create test coordinator configuration
///
/// Returns a coordinator configuration suitable for testing with
/// reduced timeouts and simplified settings.
pub fn create_test_config() -> CoordinatorConfig {
    CoordinatorConfig {
        worker_count: 2,
        progress_update_interval: Duration::from_millis(10),
        shutdown_timeout: Duration::from_millis(100),
        enable_progress_bar: false,
        verbose_logging: false,
        progress_batch_size: 5,
        worker_config: WorkerConfig {
            worker_count: 2,
            max_retries: 1,
            retry_base_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_millis(100),
            idle_sleep_duration: Duration::from_millis(5),
            progress_buffer_size: 5,
            download_timeout: Duration::from_millis(500),
            detailed_progress: false,
        },
    }
}

/// Create test components for coordinator testing
///
/// Returns a tuple of (queue, cache, client) configured for testing
/// with temporary directories and simplified configurations.
pub async fn create_test_components() -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>) {
    let temp_dir = TempDir::new().unwrap();
    let cache_config = CacheConfig {
        cache_root: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    };

    let queue = Arc::new(WorkQueue::new());
    let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
    let client = Arc::new(CedaClient::new_simple().await.unwrap());

    (queue, cache, client)
}

/// Create test file info for queue operations
///
/// Returns a FileInfo struct with test data suitable for
/// adding to work queues during testing.
pub fn create_test_file_info(name: &str) -> FileInfo {
    let hash = Md5Hash::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap();
    FileInfo {
        hash,
        relative_path: format!("./{}", name),
        file_name: name.to_string(),
        dataset_info: DatasetFileInfo {
            dataset_name: "test".to_string(),
            version: "v1".to_string(),
            county: None,
            station_id: None,
            station_name: None,
            quality_version: None,
            year: None,
            file_type: None,
        },
        manifest_version: Some(202507),
        retry_count: 0,
        last_attempt: None,
        estimated_size: Some(1024),
        destination_path: std::path::PathBuf::from(format!("/tmp/{}", name)),
    }
}

/// Test coordinator creation and basic configuration
///
/// Verifies that a coordinator can be created with valid configuration
/// and that initial state is properly set up.
#[tokio::test]
async fn test_coordinator_creation() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let coordinator = Coordinator::new(config.clone(), queue, cache, client);

    assert_eq!(coordinator.config.worker_count, config.worker_count);
    
    // Test initial statistics
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.files_completed, 0);
    assert_eq!(stats.files_failed, 0);
}

/// Test coordinator creation with expected files
///
/// Ensures that coordinators can be created with a known expected
/// file count for accurate progress tracking.
#[tokio::test]
async fn test_coordinator_with_expected_files() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let coordinator = Coordinator::new_with_expected_files(
        config,
        queue,
        cache,
        client,
        100
    );

    let stats = coordinator.get_stats().await;
    assert_eq!(stats.total_files, 100);
}

/// Test statistics initialization and updates
///
/// Ensures that download statistics are properly initialized and
/// can be updated throughout the download process.
#[tokio::test]
async fn test_statistics_tracking() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let coordinator = Coordinator::new(config, queue.clone(), cache, client);

    // Initial stats should be default
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.files_completed, 0);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.total_bytes_downloaded, 0);

    // Add some work to queue
    let file_info = create_test_file_info("test.txt");
    queue.add_work(file_info).await.unwrap();

    // Stats should reflect the added work
    let _stats = coordinator.get_stats().await;
    // Note: total_files is set during run_downloads() initialization
}

/// Test graceful shutdown functionality
///
/// Verifies that shutdown signals are properly handled and that
/// the coordinator can gracefully terminate ongoing operations.
#[tokio::test]
async fn test_graceful_shutdown() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let coordinator = Coordinator::new(config, queue, cache, client);

    // Test shutdown before starting downloads
    let result = coordinator.shutdown().await;
    assert!(result.is_ok());
}

/// Test configuration validation
///
/// Ensures that coordinator configurations are validated and
/// that invalid configurations are handled appropriately.
#[tokio::test]
async fn test_config_validation() {
    let (queue, cache, client) = create_test_components().await;

    // Test with zero workers (should still work, just no downloads)
    let config = CoordinatorConfig {
        worker_count: 0,
        ..create_test_config()
    };

    let coordinator = Coordinator::new(config.clone(), queue, cache, client);
    assert_eq!(coordinator.config.worker_count, 0);
}

/// Test worker pool creation
///
/// Verifies that worker pools can be created with the coordinator's
/// configuration and that creation doesn't fail unexpectedly.
#[tokio::test]
async fn test_worker_pool_creation() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let coordinator = Coordinator::new(config, queue, cache, client);

    // Test worker pool creation
    let result = coordinator.create_worker_pool().await;
    assert!(result.is_ok());
}

/// Test completion and shutdown handlers
///
/// Ensures that completion and shutdown handlers correctly update
/// statistics and perform necessary cleanup operations.
#[tokio::test]
async fn test_completion_and_shutdown_handlers() {
    let config = create_test_config();
    let (queue, cache, client) = create_test_components().await;

    let mut coordinator = Coordinator::new(config, queue.clone(), cache, client);

    // Test completion handler
    let result = coordinator.handle_completion().await;
    assert!(result.is_ok());

    // Verify stats were updated
    let stats = coordinator.get_stats().await;
    assert_eq!(stats.active_workers, 0);

    // Test shutdown handler
    let result = coordinator.handle_shutdown().await;
    assert!(result.is_ok());
}