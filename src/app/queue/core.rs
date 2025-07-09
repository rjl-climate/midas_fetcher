//! Core work queue implementation
//!
//! This module contains the main WorkQueue implementation that coordinates
//! all queue operations using the extracted components.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::app::hash::Md5Hash;
use crate::app::models::FileInfo;
use crate::errors::{DownloadError, DownloadResult};

use super::config::ConfigPresets;
use super::state::QueueState;
use super::stats::StatsManager;
use super::types::{QueueStats, WorkInfo, WorkQueueConfig};

/// Main work queue implementation
///
/// This is the primary interface for the work queue system, providing
/// thread-safe operations for adding, claiming, and managing work items.
#[derive(Debug)]
pub struct WorkQueue {
    /// Configuration for queue behavior
    config: WorkQueueConfig,
    /// Shared state protected by async mutex
    state: Arc<Mutex<QueueState>>,
    /// Statistics manager with caching
    stats_manager: StatsManager,
}

impl WorkQueue {
    /// Create a new work queue with default configuration
    pub fn new() -> Self {
        Self::with_config(ConfigPresets::production())
    }

    /// Create a new work queue with custom configuration
    pub fn with_config(config: WorkQueueConfig) -> Self {
        // Validate configuration
        if let Err(e) = config.validate() {
            panic!("Invalid queue configuration: {}", e);
        }

        Self {
            config,
            state: Arc::new(Mutex::new(QueueState::new())),
            stats_manager: StatsManager::new(),
        }
    }

    /// Add a file to the work queue
    ///
    /// # Arguments
    ///
    /// * `file_info` - File information to queue for download
    ///
    /// # Returns
    ///
    /// `Ok(true)` if work was added, `Ok(false)` if duplicate, `Err` on error
    pub async fn add_work(&self, file_info: FileInfo) -> DownloadResult<bool> {
        self.add_work_with_priority(file_info, self.config.default_priority)
            .await
    }

    /// Add a file to the work queue with custom priority
    ///
    /// # Arguments
    ///
    /// * `file_info` - File information to queue for download
    /// * `priority` - Priority level (higher = more important)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if work was added, `Ok(false)` if duplicate, `Err` on error
    pub async fn add_work_with_priority(
        &self,
        file_info: FileInfo,
        priority: u32,
    ) -> DownloadResult<bool> {
        let work_info = WorkInfo::new(file_info, priority);
        let work_id = *work_info.work_id();

        let result = {
            let mut state = self.state.lock().await;
            
            // Check for duplicates
            if state.is_completed(&work_id) {
                state.get_stats_mut().duplicate_count += 1;
                debug!("Skipping duplicate completed work: {}", work_id);
                return Ok(false);
            }

            if state.contains_work(&work_id) {
                state.get_stats_mut().duplicate_count += 1;
                debug!("Skipping duplicate existing work: {}", work_id);
                return Ok(false);
            }

            // Add work
            state.add_work(work_info)
        };

        match result {
            Ok(()) => {
                self.update_stats_from_state().await;
                debug!("Added work to queue: {} (priority: {})", work_id, priority);
                Ok(true)
            }
            Err(e) => Err(DownloadError::QueueError { message: e }),
        }
    }

    /// Add multiple files to the work queue efficiently (bulk operation)
    pub async fn add_work_bulk(&self, file_infos: Vec<FileInfo>) -> DownloadResult<usize> {
        let total_files = file_infos.len();
        info!("Starting bulk add of {} files to queue", total_files);

        let work_items: Vec<WorkInfo> = file_infos
            .into_iter()
            .map(|file_info| WorkInfo::new(file_info, self.config.default_priority))
            .collect();

        let (added_count, skipped_duplicates) = {
            let mut state = self.state.lock().await;
            let (added, duplicates) = state.add_work_bulk(work_items);
            state.update_stats();
            (added, duplicates)
        };

        self.update_stats_from_state().await;

        info!(
            "Bulk add completed: {}/{} files added, {} duplicates skipped",
            added_count, total_files, skipped_duplicates
        );

        Ok(added_count)
    }

    /// Get next available work item for a worker
    ///
    /// This is the core work-stealing operation that prevents worker starvation.
    /// Workers call this method to atomically claim work without waiting for
    /// specific files.
    ///
    /// # Returns
    ///
    /// `Some(WorkInfo)` if work is available, `None` if queue is empty
    pub async fn get_next_work(&self) -> Option<WorkInfo> {
        let lock_start = std::time::Instant::now();
        let mut state = self.state.lock().await;

        // Log lock contention if high
        let lock_duration = lock_start.elapsed();
        if lock_duration > Duration::from_millis(10) {
            debug!("Queue lock contention detected: {:?} wait time", lock_duration);
        }

        // Try to get pending work first
        if let Some(work_id) = state.get_next_pending() {
            if let Some(work_info) = state.get_work(&work_id) {
                if work_info.is_available(self.config.retry_delay) {
                    return self.claim_work(&mut state, work_id).await;
                }
            }
        }

        // Try failed work ready for retry
        let retry_candidates = state.find_retry_candidates(self.config.retry_delay);
        if let Some(work_id) = retry_candidates.first() {
            return self.claim_work(&mut state, *work_id).await;
        }

        // No work available
        let total_lock_time = lock_start.elapsed();
        if total_lock_time > Duration::from_millis(5) {
            debug!("No work found after {:?} lock time", total_lock_time);
        }
        None
    }

    /// Claim work for a worker (internal helper)
    async fn claim_work(&self, state: &mut QueueState, work_id: Md5Hash) -> Option<WorkInfo> {
        let worker_id = state.next_worker_id();
        
        if let Some(work_info) = state.get_work_mut(&work_id) {
            let mut claimed_work = work_info.clone();
            claimed_work.mark_in_progress(worker_id);
            
            // Update the original work item
            work_info.mark_in_progress(worker_id);
            
            debug!("Claimed work {} for worker {}", work_id, worker_id);
            
            // Update statistics
            state.update_stats();
            let stats = state.get_stats().clone();
            drop(state); // Release lock before async operation
            
            self.stats_manager.update_stats(stats).await;
            
            Some(claimed_work)
        } else {
            None
        }
    }

    /// Mark work as completed successfully
    ///
    /// # Arguments
    ///
    /// * `work_id` - ID of the completed work (file hash)
    pub async fn mark_completed(&self, work_id: &Md5Hash) -> DownloadResult<()> {
        let result = {
            let mut state = self.state.lock().await;
            state.mark_completed(work_id)
        };

        match result {
            Ok(()) => {
                self.update_stats_from_state().await;
                info!("Marked work completed: {}", work_id);
                Ok(())
            }
            Err(e) => Err(DownloadError::QueueError { message: e }),
        }
    }

    /// Mark work as failed
    ///
    /// # Arguments
    ///
    /// * `work_id` - ID of the failed work (file hash)
    /// * `error` - Error that caused the failure
    pub async fn mark_failed(&self, work_id: &Md5Hash, error: &str) -> DownloadResult<()> {
        let result = {
            let mut state = self.state.lock().await;
            state.mark_failed(work_id, error, self.config.max_retries)
        };

        match result {
            Ok(()) => {
                self.update_stats_from_state().await;
                warn!("Marked work failed: {} - {}", work_id, error);
                Ok(())
            }
            Err(e) => Err(DownloadError::QueueError { message: e }),
        }
    }

    /// Get current queue statistics
    pub async fn stats(&self) -> QueueStats {
        self.stats_manager.get_stats().await
    }

    /// Check if the queue has any work available
    pub async fn has_work_available(&self) -> bool {
        let state = self.state.lock().await;
        state.has_work_available(self.config.retry_delay)
    }

    /// Check if all work is completed or abandoned
    pub async fn is_finished(&self) -> bool {
        let state = self.state.lock().await;
        state.is_finished(self.config.retry_delay)
    }

    /// Get work item by ID (for debugging/monitoring)
    pub async fn get_work_info(&self, work_id: &Md5Hash) -> Option<WorkInfo> {
        let state = self.state.lock().await;
        state.get_work(work_id).cloned()
    }

    /// Get all work items (for debugging/monitoring)
    pub async fn get_all_work(&self) -> Vec<WorkInfo> {
        let state = self.state.lock().await;
        state.get_all_work()
    }

    /// Clear all completed and abandoned work to free memory
    pub async fn cleanup(&self) -> usize {
        let removed = {
            let mut state = self.state.lock().await;
            let removed = state.cleanup();
            if removed > 0 {
                state.update_stats();
            }
            removed
        };

        if removed > 0 {
            self.update_stats_from_state().await;
            info!("Cleaned up {} completed/abandoned work items", removed);
        }

        removed
    }

    /// Reset the queue (clear all work)
    pub async fn reset(&self) {
        {
            let mut state = self.state.lock().await;
            state.reset();
        }
        
        self.stats_manager.reset().await;
        info!("Reset work queue");
    }

    /// Handle timed-out work items (workers that haven't reported progress)
    pub async fn handle_timeouts(&self) -> usize {
        let timed_out = {
            let mut state = self.state.lock().await;
            let timeout_candidates = state.find_timed_out_work(self.config.work_timeout);
            
            let mut count = 0;
            for work_id in timeout_candidates {
                if let Err(e) = state.mark_failed(&work_id, "Worker timeout", self.config.max_retries) {
                    warn!("Failed to mark work as timed out: {}", e);
                } else {
                    count += 1;
                    warn!("Work item timed out: {}", work_id);
                }
            }

            if count > 0 {
                state.update_stats();
                info!("Handled {} timed-out work items", count);
            }

            count
        };

        if timed_out > 0 {
            self.update_stats_from_state().await;
        }

        timed_out
    }

    /// Check if the queue has capacity for more work items
    ///
    /// This enables pull-based backpressure where the queue only requests
    /// more work when it has space to handle it.
    pub async fn has_capacity(&self) -> bool {
        let stats = self.stats().await;
        let total_active = stats.pending_count + stats.in_progress_count;
        total_active < self.config.max_pending_items as u64
    }

    /// Get queue configuration
    pub fn config(&self) -> &WorkQueueConfig {
        &self.config
    }

    /// Update statistics from current state (internal helper)
    async fn update_stats_from_state(&self) {
        let stats = {
            let mut state = self.state.lock().await;
            state.update_stats();
            state.get_stats().clone()
        };
        self.stats_manager.update_stats(stats).await;
    }
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::FileInfo;
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
    async fn test_basic_queue_operations() {
        // Test basic queue operations: add, get, complete
        let queue = WorkQueue::new();

        // Add work
        let file_info = create_test_file_info("1", "01381");
        let file_hash = file_info.hash;
        let added = queue.add_work(file_info).await.unwrap();
        assert!(added);

        // Check stats
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 1);
        assert_eq!(stats.pending_count, 1);

        // Get work
        let work = queue.get_next_work().await.unwrap();
        assert_eq!(work.work_id(), &file_hash);
        assert!(work.is_in_progress());

        // Complete work
        queue.mark_completed(&file_hash).await.unwrap();

        // Check final stats
        let stats = queue.stats().await;
        assert_eq!(stats.completed_count, 1);
        assert_eq!(stats.in_progress_count, 0);
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        // Test that duplicate work is properly detected and rejected
        let queue = WorkQueue::new();

        // Add same work twice
        let file_info = create_test_file_info("1", "01381");
        let added1 = queue.add_work(file_info.clone()).await.unwrap();
        let added2 = queue.add_work(file_info).await.unwrap();

        assert!(added1);
        assert!(!added2); // Duplicate should be rejected

        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 1);
        assert_eq!(stats.duplicate_count, 1);
    }

    #[tokio::test]
    async fn test_retry_mechanism() {
        // Test that failed work can be retried
        let config = super::super::config::ConfigPresets::testing();
        let queue = WorkQueue::with_config(config);

        // Add and get work
        let file_info = create_test_file_info("1", "01381");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        assert_eq!(work.work_id(), &file_hash);

        // Mark as failed
        queue.mark_failed(&file_hash, "Test error").await.unwrap();

        // Should not be available immediately
        assert!(queue.get_next_work().await.is_none());

        // Wait for retry delay
        sleep(Duration::from_millis(20)).await;

        // Should be available for retry
        let retry_work = queue.get_next_work().await.unwrap();
        assert_eq!(retry_work.work_id(), &file_hash);
        assert!(retry_work.is_in_progress());
    }

    #[tokio::test]
    async fn test_work_abandonment() {
        // Test that work is abandoned after max retries
        let config = super::super::config::ConfigPresets::testing();
        let queue = WorkQueue::with_config(config);

        // Add work
        let file_info = create_test_file_info("1", "01381");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        // Fail it multiple times
        for i in 0..3 {
            if let Some(work) = queue.get_next_work().await {
                queue
                    .mark_failed(work.work_id(), &format!("Error {}", i + 1))
                    .await
                    .unwrap();

                // Wait for retry delay
                sleep(Duration::from_millis(2)).await;
            } else {
                break;
            }
        }

        // Should be abandoned now
        let work_info = queue.get_work_info(&file_hash).await.unwrap();
        assert!(work_info.is_abandoned());

        let stats = queue.stats().await;
        assert_eq!(stats.abandoned_count, 1);
    }

    #[tokio::test]
    async fn test_bulk_operations() {
        // Test bulk add operations
        let queue = WorkQueue::new();

        // Add multiple work items
        let file_infos = vec![
            create_test_file_info("1", "01381"),
            create_test_file_info("2", "01382"),
            create_test_file_info("3", "01383"),
        ];

        let added_count = queue.add_work_bulk(file_infos).await.unwrap();
        assert_eq!(added_count, 3);

        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 3);
        assert_eq!(stats.pending_count, 3);
    }

    #[tokio::test]
    async fn test_queue_capacity() {
        // Test capacity management
        let queue = WorkQueue::new();

        // Empty queue should have capacity
        assert!(queue.has_capacity().await);

        // Add some work
        let file_info = create_test_file_info("1", "01381");
        queue.add_work(file_info).await.unwrap();

        // Should still have capacity
        assert!(queue.has_capacity().await);
    }

    #[tokio::test]
    async fn test_queue_cleanup() {
        // Test cleanup operations
        let queue = WorkQueue::new();

        // Add and complete work
        let file_info = create_test_file_info("1", "01381");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();

        // Cleanup should remove completed work
        let removed = queue.cleanup().await;
        assert_eq!(removed, 1);

        // Check that work items were removed
        let all_work = queue.get_all_work().await;
        assert!(all_work.is_empty());
    }

    #[tokio::test]
    async fn test_queue_reset() {
        // Test queue reset functionality
        let queue = WorkQueue::new();

        // Add work
        let file_info = create_test_file_info("1", "01381");
        queue.add_work(file_info).await.unwrap();

        // Reset queue
        queue.reset().await;

        // Queue should be empty
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 0);
        assert_eq!(stats.pending_count, 0);
        assert!(queue.is_finished().await);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        // Test basic thread safety
        let queue = Arc::new(WorkQueue::new());

        // Add work concurrently
        let mut handles = Vec::new();
        for i in 1..=5 {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                let file_info = create_test_file_info(
                    &format!("{}", i),
                    &format!("0138{}", i),
                );
                queue_clone.add_work(file_info).await
            });
            handles.push(handle);
        }

        // Wait for all additions to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Check that all work was added
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 5);
        assert_eq!(stats.pending_count, 5);
    }
}