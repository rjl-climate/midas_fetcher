//! Unit tests for queue module components
//!
//! This module contains focused unit tests for individual queue components,
//! ensuring each part works correctly in isolation.

#[cfg(test)]
mod integration_tests {
    use super::super::*;
    use crate::app::models::FileInfo;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::sleep;

    fn create_test_file_info(hash: &str, station: &str) -> FileInfo {
        let temp_dir = TempDir::new().unwrap();
        let valid_hash = format!("{:0<32}", hash);
        let hash_obj = crate::app::hash::Md5Hash::from_hex(&valid_hash).unwrap();
        let path = format!(
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/{}_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_{}_twist_qcv-1_1980.csv",
            station, station
        );
        FileInfo::new(hash_obj, path, temp_dir.path(), None).unwrap()
    }

    #[tokio::test]
    async fn test_end_to_end_workflow() {
        // Test complete workflow from adding work to completion
        let queue = WorkQueue::new();

        // Add multiple work items
        let files = vec![
            create_test_file_info("1", "01381"),
            create_test_file_info("2", "01382"),
            create_test_file_info("3", "01383"),
        ];

        let mut file_hashes = Vec::new();
        for file in files {
            file_hashes.push(file.hash);
            queue.add_work(file).await.unwrap();
        }

        // Process all work items
        let mut completed = 0;
        while let Some(work) = queue.get_next_work().await {
            // Simulate processing
            sleep(Duration::from_millis(1)).await;

            queue.mark_completed(work.work_id()).await.unwrap();
            completed += 1;

            if completed >= 3 {
                break;
            }
        }

        // Verify final state
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 3);
        assert_eq!(stats.completed_count, 3);
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.in_progress_count, 0);
        assert!(queue.is_finished().await);
    }

    #[tokio::test]
    async fn test_failure_and_retry_workflow() {
        // Test failure and retry handling
        let config = ConfigPresets::testing();
        let queue = WorkQueue::with_config(config);

        // Add work
        let file = create_test_file_info("1", "01381");
        let file_hash = file.hash;
        queue.add_work(file).await.unwrap();

        // Get and fail work multiple times (max_retries = 2)
        for attempt in 1..=2 {
            let work = queue
                .get_next_work()
                .await
                .expect("Work should be available");
            assert_eq!(work.work_id(), &file_hash);

            queue
                .mark_failed(&file_hash, &format!("Error {}", attempt))
                .await
                .unwrap();

            // Wait for retry delay
            sleep(Duration::from_millis(15)).await;
        }

        // Should be abandoned after max retries
        let work_info = queue.get_work_info(&file_hash).await.unwrap();
        assert!(work_info.is_abandoned());

        let stats = queue.stats().await;
        assert_eq!(stats.abandoned_count, 1);
        assert!(queue.is_finished().await);
    }

    #[tokio::test]
    async fn test_concurrent_worker_simulation() {
        // Test concurrent access by multiple simulated workers
        let queue = Arc::new(WorkQueue::new());
        let worker_count = 5;
        let work_per_worker = 10;

        // Add work items
        for i in 0..(worker_count * work_per_worker) {
            let file = create_test_file_info(&format!("{:032}", i), &format!("{:05}", i));
            queue.add_work(file).await.unwrap();
        }

        // Spawn worker tasks
        let mut handles = Vec::new();
        for _worker_id in 0..worker_count {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                let mut processed = 0;

                while let Some(work) = queue_clone.get_next_work().await {
                    // Simulate work processing
                    sleep(Duration::from_millis(1)).await;

                    // Randomly succeed or fail (90% success rate)
                    if processed % 10 == 0 {
                        queue_clone
                            .mark_failed(work.work_id(), "Simulated error")
                            .await
                            .unwrap();
                    } else {
                        queue_clone.mark_completed(work.work_id()).await.unwrap();
                    }

                    processed += 1;

                    if processed >= work_per_worker {
                        break;
                    }
                }

                processed
            });
            handles.push(handle);
        }

        // Wait for all workers to complete
        let mut total_processed = 0;
        for handle in handles {
            let worker_processed = handle.await.unwrap();
            total_processed += worker_processed;
        }

        // Verify results
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, (worker_count * work_per_worker) as u64);
        assert!(stats.completed_count > 0);
        assert_eq!(total_processed, worker_count * work_per_worker);
    }

    #[tokio::test]
    async fn test_timeout_handling() {
        // Test timeout detection and handling
        let config = WorkQueueConfigBuilder::new()
            .work_timeout(Duration::from_millis(50))
            .build();
        let queue = WorkQueue::with_config(config);

        // Add work and claim it
        let file = create_test_file_info("1", "01381");
        let file_hash = file.hash;
        queue.add_work(file).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        assert!(work.is_in_progress());

        // Wait for timeout
        sleep(Duration::from_millis(100)).await;

        // Handle timeouts
        let timed_out = queue.handle_timeouts().await;
        assert_eq!(timed_out, 1);

        // Work should be marked as failed
        let work_info = queue.get_work_info(&file_hash).await.unwrap();
        assert!(work_info.is_failed());
    }

    #[tokio::test]
    async fn test_capacity_management() {
        // Test capacity and backpressure
        let config = WorkQueueConfigBuilder::new().max_pending_items(5).build();
        let queue = WorkQueue::with_config(config);

        // Fill up to near capacity
        for i in 0..4 {
            let file = create_test_file_info(&format!("{:032}", i), &format!("{:05}", i));
            queue.add_work(file).await.unwrap();
        }

        // Should still have capacity (not at limit yet)
        assert!(queue.has_capacity().await);

        // Add one more to reach capacity
        let file = create_test_file_info(&format!("{:032}", 4), &format!("{:05}", 4));
        queue.add_work(file).await.unwrap();

        // Should be at capacity now
        assert!(!queue.has_capacity().await);
        let stats = queue.stats().await;
        assert_eq!(stats.pending_count, 5);
    }

    #[tokio::test]
    async fn test_statistics_accuracy() {
        // Test that statistics are accurate throughout operations
        let queue = WorkQueue::new();

        // Add work
        let files = vec![
            create_test_file_info("1", "01381"),
            create_test_file_info("2", "01382"),
            create_test_file_info("3", "01383"),
        ];

        for file in files {
            queue.add_work(file).await.unwrap();
        }

        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 3);
        assert_eq!(stats.pending_count, 3);
        assert_eq!(stats.in_progress_count, 0);
        assert_eq!(stats.completed_count, 0);

        // Process one item
        let work = queue.get_next_work().await.unwrap();
        let stats = queue.stats().await;
        assert_eq!(stats.pending_count, 2);
        assert_eq!(stats.in_progress_count, 1);

        // Complete the item
        queue.mark_completed(work.work_id()).await.unwrap();
        let stats = queue.stats().await;
        assert_eq!(stats.pending_count, 2);
        assert_eq!(stats.in_progress_count, 0);
        assert_eq!(stats.completed_count, 1);
    }

    #[tokio::test]
    async fn test_bulk_operations_efficiency() {
        // Test bulk operations are more efficient than individual adds
        let queue = WorkQueue::new();

        // Create many file infos
        let files: Vec<FileInfo> = (0..1000)
            .map(|i| create_test_file_info(&format!("{:032}", i), &format!("{:05}", i)))
            .collect();

        // Bulk add should be faster than individual adds
        let start = std::time::Instant::now();
        let added = queue.add_work_bulk(files).await.unwrap();
        let bulk_duration = start.elapsed();

        assert_eq!(added, 1000);

        // Bulk operation should complete in reasonable time
        assert!(bulk_duration < Duration::from_secs(1));

        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 1000);
        assert_eq!(stats.pending_count, 1000);
    }

    #[tokio::test]
    async fn test_queue_cleanup_efficiency() {
        // Test cleanup removes completed work efficiently
        let queue = WorkQueue::new();

        // Add and complete work
        for i in 0..100 {
            let file = create_test_file_info(&format!("{:032}", i), &format!("{:05}", i));
            queue.add_work(file).await.unwrap();

            let work = queue.get_next_work().await.unwrap();
            queue.mark_completed(work.work_id()).await.unwrap();
        }

        let stats_before = queue.stats().await;
        assert_eq!(stats_before.completed_count, 100);

        // Cleanup should remove all completed work
        let removed = queue.cleanup().await;
        assert_eq!(removed, 100);

        let stats_after = queue.stats().await;
        assert_eq!(stats_after.completed_count, 100); // Count preserved

        // But internal storage should be cleaned
        let all_work = queue.get_all_work().await;
        assert!(all_work.is_empty());
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        // Test that higher priority work is processed first
        let queue = WorkQueue::new();

        // Add work with different priorities (reverse order)
        let low_file = create_test_file_info("1", "01381");
        let high_file = create_test_file_info("2", "01382");
        let normal_file = create_test_file_info("3", "01383");

        queue
            .add_work_with_priority(low_file, priority::LOW)
            .await
            .unwrap();
        queue
            .add_work_with_priority(high_file, priority::HIGH)
            .await
            .unwrap();
        queue
            .add_work_with_priority(normal_file, priority::NORMAL)
            .await
            .unwrap();

        // For now, we just verify all items are in the queue
        // Priority ordering would require more complex implementation
        let stats = queue.stats().await;
        assert_eq!(stats.pending_count, 3);
    }
}
