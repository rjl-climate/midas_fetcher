//! Integration tests for the work queue system
//!
//! These tests verify the queue system works correctly in realistic scenarios
//! and integrates properly with other components.

use midas_fetcher::app::hash::Md5Hash;
use midas_fetcher::app::models::FileInfo;
use midas_fetcher::app::queue::*;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

fn create_test_file_info(hash: &str, station: &str) -> FileInfo {
    let temp_dir = TempDir::new().unwrap();
    let valid_hash = format!("{:0<32}", hash);
    let hash_obj = Md5Hash::from_hex(&valid_hash).unwrap();
    let path = format!(
        "./data/uk-daily-temperature-obs/dataset-version-202407/devon/{}_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_{}_twist_qcv-1_1980.csv",
        station, station
    );
    FileInfo::new(hash_obj, path, temp_dir.path(), None).unwrap()
}

#[tokio::test]
async fn test_real_world_download_simulation() {
    // Simulate a real download scenario with multiple workers
    let config = ConfigPresets::production();
    let queue = Arc::new(WorkQueue::with_config(config));
    
    // Add a realistic number of files
    let file_count = 100;
    let files: Vec<FileInfo> = (0..file_count)
        .map(|i| create_test_file_info(&format!("{}", i), &format!("./data/file_{}.csv", i)))
        .collect();
    
    let added = queue.add_work_bulk(files).await.unwrap();
    assert_eq!(added, file_count);
    
    // Spawn multiple workers
    let worker_count = 8;
    let mut handles = Vec::new();
    
    for worker_id in 0..worker_count {
        let queue_clone = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            let mut processed = 0;
            let mut failed = 0;
            
            while let Some(work) = queue_clone.get_next_work().await {
                // Simulate download time
                sleep(Duration::from_millis(10)).await;
                
                // Simulate 95% success rate
                if processed % 20 == 0 {
                    queue_clone.mark_failed(work.work_id(), "Network error").await.unwrap();
                    failed += 1;
                } else {
                    queue_clone.mark_completed(work.work_id()).await.unwrap();
                }
                
                processed += 1;
                
                // Exit if queue is finished
                if queue_clone.is_finished().await {
                    break;
                }
            }
            
            (processed, failed)
        });
        handles.push(handle);
    }
    
    // Wait for all workers to complete
    let mut total_processed = 0;
    let mut total_failed = 0;
    
    for handle in handles {
        let (processed, failed) = handle.await.unwrap();
        total_processed += processed;
        total_failed += failed;
    }
    
    // Verify results
    let stats = queue.stats().await;
    assert_eq!(stats.total_added, file_count as u64);
    assert!(stats.completed_count > 0);
    assert!(stats.success_rate() > 80.0); // Should have good success rate
    assert!(queue.is_finished().await);
    
    println!("Processed {} items, {} failed, success rate: {:.1}%", 
        total_processed, total_failed, stats.success_rate());
}

#[tokio::test]
async fn test_queue_recovery_after_failures() {
    // Test queue can recover from various failure scenarios
    let config = WorkQueueConfigBuilder::new()
        .max_retries(3)
        .retry_delay(Duration::from_millis(10))
        .build();
    let queue = WorkQueue::with_config(config);
    
    // Add work items
    let files = vec![
        create_test_file_info("1", "01381"),
        create_test_file_info("2", "01382"),
        create_test_file_info("3", "01383"),
    ];
    
    for file in files {
        queue.add_work(file).await.unwrap();
    }
    
    // Fail all items once
    for _ in 0..3 {
        let work = queue.get_next_work().await.unwrap();
        queue.mark_failed(work.work_id(), "Temporary failure").await.unwrap();
    }
    
    // Wait for retry delay
    sleep(Duration::from_millis(50)).await;
    
    // All items should be available for retry
    let stats = queue.stats().await;
    assert_eq!(stats.failed_count, 3);
    assert!(queue.has_work_available().await);
    
    // Process all items successfully on retry
    for _ in 0..3 {
        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();
    }
    
    let final_stats = queue.stats().await;
    assert_eq!(final_stats.completed_count, 3);
    assert_eq!(final_stats.failed_count, 0);
    assert!(queue.is_finished().await);
}

#[tokio::test]
async fn test_queue_performance_under_load() {
    // Test queue performance with high load
    let config = ConfigPresets::high_throughput();
    let queue = Arc::new(WorkQueue::with_config(config));
    
    let file_count = 1000;
    let worker_count = 16;
    
    // Measure bulk add performance
    let start = std::time::Instant::now();
    let files: Vec<FileInfo> = (0..file_count)
        .map(|i| create_test_file_info(&format!("{}", i), &format!("./data/file_{}.csv", i)))
        .collect();
    
    let added = queue.add_work_bulk(files).await.unwrap();
    let add_duration = start.elapsed();
    
    assert_eq!(added, file_count);
    assert!(add_duration < Duration::from_secs(1)); // Should be fast
    
    // Measure processing performance
    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    
    for _ in 0..worker_count {
        let queue_clone = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            let mut processed = 0;
            
            while let Some(work) = queue_clone.get_next_work().await {
                // Minimal processing time
                queue_clone.mark_completed(work.work_id()).await.unwrap();
                processed += 1;
                
                if queue_clone.is_finished().await {
                    break;
                }
            }
            
            processed
        });
        handles.push(handle);
    }
    
    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }
    
    let process_duration = start.elapsed();
    let throughput = file_count as f64 / process_duration.as_secs_f64();
    
    // Should achieve reasonable throughput
    assert!(throughput > 100.0); // At least 100 items/second
    assert!(queue.is_finished().await);
    
    println!("Processed {} items in {:?} ({:.1} items/sec)", 
        file_count, process_duration, throughput);
}

#[tokio::test]
async fn test_queue_memory_management() {
    // Test memory usage with cleanup
    let queue = WorkQueue::new();
    
    // Add and complete many items
    let item_count = 1000;
    for i in 0..item_count {
        let file = create_test_file_info(&format!("{}", i), &format!("./data/test{}.csv", i));
        queue.add_work(file).await.unwrap();
        
        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();
    }
    
    let stats_before = queue.stats().await;
    assert_eq!(stats_before.completed_count, item_count as u64);
    
    // Cleanup should free memory
    let removed = queue.cleanup().await;
    assert_eq!(removed, item_count);
    
    // Memory should be reclaimed
    let all_work = queue.get_all_work().await;
    assert!(all_work.is_empty());
    
    // But statistics should be preserved
    let stats_after = queue.stats().await;
    assert_eq!(stats_after.completed_count, item_count as u64);
}

#[tokio::test]
async fn test_queue_configuration_effects() {
    // Test how different configurations affect behavior
    let configs = vec![
        ("Development", ConfigPresets::development()),
        ("Production", ConfigPresets::production()),
        ("Testing", ConfigPresets::testing()),
        ("High Throughput", ConfigPresets::high_throughput()),
        ("Low Resource", ConfigPresets::low_resource()),
    ];
    
    for (name, config) in configs {
        let queue = WorkQueue::with_config(config);
        
        // Add work
        let file = create_test_file_info("1", "01381");
        let file_hash = file.hash;
        queue.add_work(file).await.unwrap();
        
        // Test retry behavior
        let work = queue.get_next_work().await.unwrap();
        queue.mark_failed(&file_hash, "Test error").await.unwrap();
        
        // Wait for retry delay
        sleep(queue.config().retry_delay + Duration::from_millis(10)).await;
        
        // Should be available for retry
        assert!(queue.has_work_available().await, "Config {} failed retry test", name);
        
        // Clean up
        queue.reset().await;
    }
}

#[tokio::test]
async fn test_queue_statistics_reporting() {
    // Test comprehensive statistics reporting
    let queue = WorkQueue::new();
    
    // Create diverse work scenarios
    let files = vec![
        create_test_file_info("1", "01381"),
        create_test_file_info("2", "01382"),
        create_test_file_info("3", "01383"),
        create_test_file_info("4", "01384"),
        create_test_file_info("5", "01385"),
    ];
    
    for file in files {
        queue.add_work(file).await.unwrap();
    }
    
    // Process with different outcomes
    // Success cases
    for _ in 0..2 {
        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();
    }
    
    // Retry cases
    for _ in 0..2 {
        let work = queue.get_next_work().await.unwrap();
        queue.mark_failed(work.work_id(), "Temporary error").await.unwrap();
    }
    
    // Abandon case
    let work = queue.get_next_work().await.unwrap();
    let abandon_id = *work.work_id();
    for _ in 0..3 {
        queue.mark_failed(&abandon_id, "Permanent error").await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }
    
    // Generate reports
    let stats = queue.stats().await;
    let summary = StatsReporter::generate_summary_report(&stats, 8);
    let compact = StatsReporter::generate_compact_status(&stats);
    
    // Verify report content
    assert!(summary.contains("Total Added: 5"));
    assert!(summary.contains("Completed: 2"));
    assert!(summary.contains("Success Rate:"));
    assert!(summary.contains("Health Score:"));
    
    assert!(compact.contains("5 total"));
    assert!(compact.contains("2 completed"));
    
    println!("Summary Report:\n{}", summary);
    println!("Compact Status: {}", compact);
}

#[tokio::test]
async fn test_queue_timeout_detection() {
    // Test timeout detection in realistic scenarios
    let config = WorkQueueConfigBuilder::new()
        .work_timeout(Duration::from_millis(100))
        .retry_delay(Duration::from_millis(10))
        .build();
    let queue = WorkQueue::with_config(config);
    
    // Add work and simulate hung workers
    let files = vec![
        create_test_file_info("1", "01381"),
        create_test_file_info("2", "01382"),
        create_test_file_info("3", "01383"),
    ];
    
    for file in files {
        queue.add_work(file).await.unwrap();
    }
    
    // Claim work but don't complete it (simulate hung workers)
    let work1 = queue.get_next_work().await.unwrap();
    let work2 = queue.get_next_work().await.unwrap();
    let work3 = queue.get_next_work().await.unwrap();
    
    // Complete one item normally
    queue.mark_completed(work3.work_id()).await.unwrap();
    
    // Wait for timeout
    sleep(Duration::from_millis(200)).await;
    
    // Handle timeouts
    let timed_out = queue.handle_timeouts().await;
    assert_eq!(timed_out, 2);
    
    // Timed out work should be available for retry
    assert!(queue.has_work_available().await);
    
    let stats = queue.stats().await;
    assert_eq!(stats.completed_count, 1);
    assert_eq!(stats.failed_count, 2);
}

#[tokio::test]
async fn test_queue_capacity_backpressure() {
    // Test capacity-based backpressure
    let config = WorkQueueConfigBuilder::new()
        .max_pending_items(10)
        .build();
    let queue = WorkQueue::with_config(config);
    
    // Fill queue to capacity
    for i in 0..15 {
        let file = create_test_file_info(&format!("{}", i), &format!("./data/test{}.csv", i));
        queue.add_work(file).await.unwrap();
    }
    
    let stats = queue.stats().await;
    assert_eq!(stats.total_added, 15);
    
    // Test capacity checking
    let has_capacity = queue.has_capacity().await;
    assert!(!has_capacity); // Should be at capacity
    
    // Process some items to free capacity
    for _ in 0..5 {
        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();
    }
    
    // Should have capacity now
    assert!(queue.has_capacity().await);
}

#[tokio::test]
async fn test_queue_health_monitoring() {
    // Test health monitoring utilities
    let queue = WorkQueue::new();
    
    // Add work and process with different outcomes
    for i in 0..20 {
        let file = create_test_file_info(&format!("{}", i), &format!("./data/test{}.csv", i));
        queue.add_work(file).await.unwrap();
    }
    
    // Process with 80% success rate
    for i in 0..20 {
        let work = queue.get_next_work().await.unwrap();
        
        if i % 5 == 0 {
            queue.mark_failed(work.work_id(), "Error").await.unwrap();
        } else {
            queue.mark_completed(work.work_id()).await.unwrap();
        }
    }
    
    // Check health
    let health = utils::get_queue_health(&queue).await;
    assert!(health.is_healthy); // 80% success should be healthy
    assert!(health.utilization >= 0.0);
    assert_eq!(health.backlog_size, 0); // No pending items
    assert!(health.error_rate > 0.0); // Some errors occurred
    
    println!("Queue Health: healthy={}, utilization={:.1}%, backlog={}, error_rate={:.1}%",
        health.is_healthy, health.utilization, health.backlog_size, health.error_rate);
}

#[tokio::test]
async fn test_queue_with_manifest_integration() {
    // Test integration with manifest filling (if available)
    let queue = WorkQueue::new();
    
    // This test simulates how the queue integrates with manifest processing
    // In a real scenario, this would use actual manifest data
    
    // Simulate manifest entries
    let manifest_entries = vec![
        ("hash1", "./data/uk-daily-rain-obs/file1.csv"),
        ("hash2", "./data/uk-daily-rain-obs/file2.csv"),
        ("hash3", "./data/uk-daily-rain-obs/file3.csv"),
    ];
    
    // Add work as if from manifest
    for (hash, path) in manifest_entries {
        let file = create_test_file_info(hash, path);
        queue.add_work(file).await.unwrap();
    }
    
    // Process as if downloading
    let mut downloaded = 0;
    while let Some(work) = queue.get_next_work().await {
        // Simulate download
        sleep(Duration::from_millis(1)).await;
        
        queue.mark_completed(work.work_id()).await.unwrap();
        downloaded += 1;
        
        if downloaded >= 3 {
            break;
        }
    }
    
    let stats = queue.stats().await;
    assert_eq!(stats.total_added, 3);
    assert_eq!(stats.completed_count, 3);
    assert!(queue.is_finished().await);
}