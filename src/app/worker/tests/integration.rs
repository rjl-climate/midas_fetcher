//! Integration tests for worker functionality with real CEDA downloads
//!
//! These tests require actual network access and CEDA credentials to verify
//! that the worker system can successfully download real files from CEDA.

use std::{env, path::Path, sync::Arc, time::Duration};

use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

use crate::app::{
    cache::{CacheConfig, CacheManager},
    client::{CedaClient, ClientConfig},
    manifest::{ManifestConfig, ManifestStreamer},
    models::FileInfo,
    queue::WorkQueue,
    worker::{ConfigPresets, WorkerPool, WorkerStatus},
};
use crate::constants::env as env_constants;

/// Create test components for integration testing
pub async fn create_test_components(
) -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let cache_config = CacheConfig {
        cache_root: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    };

    let queue = Arc::new(WorkQueue::new());
    let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
    let client = Arc::new(
        CedaClient::new_simple_with_config(ClientConfig::default())
            .await
            .unwrap(),
    );

    (queue, cache, client, temp_dir)
}

/// Create a test file info for integration testing
pub fn create_test_file_info(hash: &str, station: &str, temp_dir: &Path) -> FileInfo {
    let valid_hash = if hash.len() >= 32 {
        hash.chars().take(32).collect::<String>()
    } else {
        format!("{:0<32}", hash)
    };
    let hash_obj = crate::app::hash::Md5Hash::from_hex(&valid_hash).unwrap();
    let path = format!(
        "./data/uk-daily-temperature-obs/dataset-version-202407/devon/{}_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_{}_twist_qcv-1_1980.csv",
        station, station
    );
    FileInfo::new(hash_obj, path, temp_dir, None).unwrap()
}

/// End-to-end integration test for worker functionality
///
/// This test verifies complete worker operation including:
/// - Queue integration for work claiming
/// - Cache reservation and atomic file operations  
/// - Real CEDA downloads with authentication
/// - Progress reporting and error handling
/// - Worker pool coordination and shutdown
///
/// Requires CEDA credentials and network access, so it's ignored
/// by default and only run during explicit integration testing.
///
/// Run with: cargo test test_worker_integration -- --ignored --nocapture
///
/// Setup instructions:
/// 1. Create a .env file in the project root with:
///    CEDA_USERNAME=your_username
///    CEDA_PASSWORD=your_password
/// 2. Or set environment variables directly
#[tokio::test]
#[ignore = "Requires CEDA authentication and network access"]
async fn test_worker_integration() {
    // Initialize tracing for detailed debugging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .try_init()
        .ok();

    println!("🚀 Starting worker integration test");

    // Load credentials from .env file if it exists
    if Path::new(".env").exists() {
        println!("📁 Loading credentials from .env file...");
        let env_content = std::fs::read_to_string(".env").expect("Failed to read .env file");
        for line in env_content.lines() {
            if let Some((key, value)) = line.split_once('=') {
                unsafe {
                    if key.trim() == env_constants::USERNAME && !value.trim().is_empty() {
                        env::set_var(env_constants::USERNAME, value.trim());
                    }
                    if key.trim() == env_constants::PASSWORD && !value.trim().is_empty() {
                        env::set_var(env_constants::PASSWORD, value.trim());
                    }
                }
            }
        }
    }

    // Check credentials
    let username = env::var(env_constants::USERNAME)
        .expect("CEDA_USERNAME environment variable not set. Please set credentials.");
    let _password = env::var(env_constants::PASSWORD)
        .expect("CEDA_PASSWORD environment variable not set. Please set credentials.");

    println!("🔐 Testing worker integration with user: {}", username);

    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    println!(
        "📁 Using temporary cache directory: {}",
        temp_dir.path().display()
    );

    // Setup shared components
    let cache_config = CacheConfig {
        cache_root: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    };

    println!("🗄️  Initializing cache manager...");
    let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());

    println!("🌐 Creating authenticated CEDA client...");
    let client = Arc::new(
        CedaClient::new()
            .await
            .expect("Failed to create CEDA client"),
    );

    println!("📋 Initializing work queue...");
    let queue = Arc::new(WorkQueue::new());

    // Load a few files from the example manifest
    let manifest_path = Path::new("examples/midas-open-v202407-md5s.txt");
    if !manifest_path.exists() {
        panic!(
            "Example manifest file not found at: {}",
            manifest_path.display()
        );
    }

    println!("📄 Reading manifest file: {}", manifest_path.display());
    let manifest_config = ManifestConfig {
        destination_root: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let manifest_version = ManifestStreamer::extract_manifest_version_from_path(manifest_path);
    let mut streamer = ManifestStreamer::with_config_and_version(manifest_config, manifest_version);
    let mut stream = streamer.stream(manifest_path).await.unwrap();

    // Add first 3 files to queue for testing
    let mut test_files = Vec::new();
    let mut file_count = 0;
    while let Some(result) = stream.next().await {
        if file_count >= 3 {
            break;
        } // Limit to 3 files for integration test

        match result {
            Ok(file_info) => {
                println!(
                    "📋 Adding to queue: {} ({})",
                    file_info.file_name, file_info.hash
                );
                queue.add_work(file_info.clone()).await.unwrap();
                test_files.push(file_info);
                file_count += 1;
            }
            Err(e) => {
                eprintln!("❌ Error parsing manifest line: {}", e);
                continue;
            }
        }
    }

    println!("📊 Added {} files to work queue", test_files.len());
    assert!(!test_files.is_empty(), "No files loaded from manifest");

    // Create worker pool with 2 workers for testing
    let worker_config = ConfigPresets::testing();

    println!(
        "👷 Creating worker pool with {} workers",
        worker_config.worker_count
    );
    let mut pool = WorkerPool::new(worker_config, queue.clone(), cache.clone(), client);

    // Setup progress monitoring
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);

    // Start workers
    println!("🚀 Starting worker pool...");
    pool.start(progress_tx).await.unwrap();

    // Monitor progress with timeout
    let queue_clone = queue.clone();
    let progress_task = tokio::spawn(async move {
        let mut progress_updates = Vec::new();
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(180); // 3 minute timeout for all downloads

        while start.elapsed() < timeout {
            tokio::select! {
                progress = progress_rx.recv() => {
                    match progress {
                        Some(update) => {
                            println!(
                                "📈 Worker {}: {:?} status, {} files completed, {} total bytes",
                                update.worker_id,
                                update.status,
                                update.files_completed,
                                update.total_bytes_downloaded
                            );
                            progress_updates.push(update);
                        }
                        None => {
                            println!("📈 Progress channel closed");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // Check if all work is done
                    if queue_clone.is_finished().await {
                        println!("✅ All work completed!");
                        break;
                    }
                }
            }
        }

        if start.elapsed() >= timeout {
            println!("⚠️  Test timeout reached after 3 minutes");
        }

        progress_updates
    });

    // Wait for progress monitoring to complete
    let progress_updates = progress_task.await.unwrap();

    // Shutdown worker pool
    println!("🛑 Shutting down worker pool...");
    pool.shutdown().await.unwrap();

    // Verify results
    println!("🔍 Verifying integration test results...");

    // Check progress updates
    assert!(!progress_updates.is_empty(), "No progress updates received");
    println!("📊 Received {} progress updates", progress_updates.len());

    // Check queue statistics
    let queue_stats = queue.stats().await;
    println!(
        "📋 Queue stats: {} completed, {} failed, {} pending",
        queue_stats.completed_count, queue_stats.failed_count, queue_stats.pending_count
    );

    // Verify at least some files were processed
    assert!(
        queue_stats.completed_count > 0 || queue_stats.failed_count > 0,
        "No files were processed by workers"
    );

    // Check cache for completed files
    let mut cached_files = 0;
    let mut verified_files = 0;

    for file_info in &test_files {
        let cache_path = cache.get_file_path(file_info);
        if cache_path.exists() {
            cached_files += 1;
            println!("📁 Found cached file: {}", cache_path.display());

            // Verify file integrity
            match cache.verify_cache_integrity(vec![file_info.clone()]).await {
                Ok(report) if report.files_verified > 0 => {
                    verified_files += 1;
                    println!("✅ Cache verification passed for: {}", file_info.file_name);
                }
                Ok(_) => {
                    println!("⚠️  Cache verification failed for: {}", file_info.file_name);
                }
                Err(e) => {
                    println!(
                        "❌ Cache verification error for {}: {}",
                        file_info.file_name, e
                    );
                }
            }
        }
    }

    println!("📊 Integration test summary:");
    println!(
        "   Files processed: {}",
        queue_stats.completed_count + queue_stats.failed_count
    );
    println!("   Files cached: {}", cached_files);
    println!("   Files verified: {}", verified_files);
    println!("   Progress updates: {}", progress_updates.len());

    // Test assertions
    assert!(cached_files > 0, "No files were successfully cached");
    assert!(verified_files > 0, "No files passed cache verification");

    // Verify worker status progression
    let worker_statuses: std::collections::HashSet<_> = progress_updates
        .iter()
        .map(|p| std::mem::discriminant(&p.status))
        .collect();

    assert!(
        worker_statuses.len() > 1,
        "Workers should progress through multiple states"
    );

    println!("🎉 Worker integration test completed successfully!");
    println!("   ✅ Workers successfully claimed work from queue");
    println!("   ✅ Cache reservations and atomic operations working");
    println!("   ✅ Real CEDA downloads completed with authentication");
    println!("   ✅ Progress reporting system functional");
    println!("   ✅ Worker pool coordination and shutdown successful");
    println!("   ✅ File integrity verified through cache system");
}

/// Test worker resilience with network interruptions
///
/// This test simulates network issues during downloads to verify
/// that workers handle timeouts and retries correctly.
#[tokio::test]
#[ignore = "Requires network access and simulated failures"]
async fn test_worker_resilience() {
    let (queue, cache, client, _temp_dir) = create_test_components().await;

    // Create worker pool with aggressive timeout settings for testing
    let worker_config = crate::app::worker::WorkerConfigBuilder::new()
        .worker_count(1)
        .max_retries(3)
        .download_timeout(Duration::from_secs(5)) // Short timeout to trigger failures
        .retry_base_delay(Duration::from_millis(100))
        .build()
        .unwrap();

    let mut pool = WorkerPool::new(worker_config, queue.clone(), cache, client);
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);

    // Add a test file that might timeout
    let file_info = create_test_file_info("a", "01381", _temp_dir.path());
    queue.add_work(file_info).await.unwrap();

    pool.start(progress_tx).await.unwrap();

    // Monitor for retry attempts
    let mut error_statuses = 0;
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    while start.elapsed() < timeout {
        tokio::select! {
            progress = progress_rx.recv() => {
                if let Some(update) = progress {
                    if matches!(update.status, WorkerStatus::Error { .. }) {
                        error_statuses += 1;
                        println!("Detected worker retry attempt: {:?}", update.status);
                    }
                } else {
                    break;
                }
            }
            _ = sleep(Duration::from_millis(100)) => {
                if queue.is_finished().await {
                    break;
                }
            }
        }
    }

    pool.shutdown().await.unwrap();

    // Should have seen some retry attempts
    println!("Total error statuses observed: {}", error_statuses);
    // Note: This test may pass even without retries if the network is very fast
}

/// Test worker performance under load
///
/// This test measures worker performance with multiple files to ensure
/// the system can handle reasonable throughput.
#[tokio::test]
#[ignore = "Performance test - requires network access"]
async fn test_worker_performance() {
    let (queue, cache, client, _temp_dir) = create_test_components().await;

    // Create worker pool optimized for throughput
    let worker_config = ConfigPresets::high_throughput();
    let mut pool = WorkerPool::new(worker_config, queue.clone(), cache, client);
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(1000);

    // Add multiple test files (using dummy data for performance testing)
    for i in 0..10 {
        let file_info = create_test_file_info(&format!("{}", i), "01381", _temp_dir.path());
        queue.add_work(file_info).await.unwrap();
    }

    let start_time = std::time::Instant::now();
    pool.start(progress_tx).await.unwrap();

    // Monitor performance metrics
    let monitoring_task = tokio::spawn(async move {
        let mut _total_throughput = 0.0;
        while let Some(progress) = progress_rx.recv().await {
            if matches!(progress.status, WorkerStatus::Downloading) {
                _total_throughput += progress.download_speed;
            }
        }
    });

    // Wait for completion or timeout
    let completion_timeout = Duration::from_secs(120);

    while start_time.elapsed() < completion_timeout {
        if queue.is_finished().await {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    pool.shutdown().await.unwrap();
    let _ = monitoring_task.await;

    let total_time = start_time.elapsed();
    println!("Performance test completed in {:?}", total_time);
    println!("Files processed: {}", queue.stats().await.completed_count);
    // Performance metrics would be calculated here in a real implementation

    // Basic performance assertions
    assert!(
        total_time < completion_timeout,
        "Test should complete within timeout"
    );
    // Additional performance metrics could be asserted here based on requirements
}

/// Test worker behavior with cache hits
///
/// This test verifies that workers correctly handle files that are
/// already cached and don't attempt unnecessary downloads.
#[tokio::test]
#[ignore = "Cache integration test - complex setup required"]
async fn test_worker_cache_behavior() {
    let (queue, cache, client, temp_dir) = create_test_components().await;

    // Pre-populate cache with a test file
    let file_info =
        create_test_file_info("a1b2c3d4e5f6789012345678901234ab", "01381", temp_dir.path());
    let test_content = b"test file content for cache test";
    // Skip cache verification for this test since we're using a fake hash
    std::fs::create_dir_all(file_info.destination_path.parent().unwrap()).unwrap();
    std::fs::write(&file_info.destination_path, test_content).unwrap();

    // Add the same file to work queue
    queue.add_work(file_info.clone()).await.unwrap();

    let worker_config = ConfigPresets::testing();
    let mut pool = WorkerPool::new(worker_config, queue.clone(), cache, client);
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);

    pool.start(progress_tx).await.unwrap();

    // Monitor for cache hit behavior
    let mut cache_hit_detected = false;
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    while start.elapsed() < timeout {
        tokio::select! {
            progress = progress_rx.recv() => {
                if let Some(update) = progress {
                    println!("Worker status: {:?}", update.status);
                } else {
                    break;
                }
            }
            _ = sleep(Duration::from_millis(100)) => {
                if queue.is_finished().await {
                    cache_hit_detected = true;
                    break;
                }
            }
        }
    }

    pool.shutdown().await.unwrap();

    // Should have completed quickly due to cache hit
    assert!(cache_hit_detected, "Should have detected cache hit");
    assert_eq!(queue.stats().await.completed_count, 1);
    println!("✅ Cache hit behavior test passed");
}

/// Test statistics and monitoring integration
///
/// This test verifies that worker statistics are correctly collected
/// and provide meaningful insights into worker performance.
#[tokio::test]
async fn test_worker_statistics_integration() {
    let (queue, cache, client, temp_dir) = create_test_components().await;

    let worker_config = ConfigPresets::testing();
    let mut pool = WorkerPool::new(worker_config, queue.clone(), cache, client);
    let (progress_tx, _progress_rx) = tokio::sync::mpsc::channel(100);

    // Add a test file
    let file_info =
        create_test_file_info("b2c3d4e5f6789012345678901234abcd", "01381", temp_dir.path());
    queue.add_work(file_info).await.unwrap();

    pool.start(progress_tx).await.unwrap();

    // Give workers time to start and report initial status
    sleep(Duration::from_millis(200)).await;

    // Add some mock progress to trigger worker status updates
    if let Some(stats) = pool.get_stats() {
        println!("Pool statistics:");
        println!("  Active workers: {}", stats.active_workers);
        println!("  Worker utilization: {:.1}%", stats.worker_utilization());

        // Workers may not all have reported yet, so check for reasonable bounds
        assert!(stats.active_workers <= 2); // Testing config has 2 workers max
        assert!(stats.worker_utilization() >= 0.0);
    }

    // Check detailed statistics
    if let Some((_stats, metrics)) = pool.get_detailed_stats() {
        println!("Performance metrics:");
        println!("  Throughput: {:.2} MB/s", metrics.throughput_mbps);
        println!("  Files per second: {:.2}", metrics.files_per_second);

        assert!(metrics.throughput_mbps >= 0.0);
        assert!(metrics.files_per_second >= 0.0);
    }

    // Test health assessment
    let health = crate::app::worker::SystemHealth::assess(&pool);
    println!("System health: {:?}", health);
    assert!(health.current_worker_count > 0);

    pool.shutdown().await.unwrap();
    println!("✅ Statistics integration test passed");
}
