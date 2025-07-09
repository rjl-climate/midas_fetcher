//! Work queue system for concurrent downloads
//!
//! This module provides a redesigned work queue system that supports efficient
//! concurrent downloads with work stealing, retry mechanisms, and comprehensive
//! statistics tracking.
//!
//! # Features
//!
//! - **Work Stealing**: Prevents worker starvation by allowing workers to claim
//!   any available work without waiting for specific items
//! - **Retry Logic**: Automatic retry of failed work with configurable delays
//! - **Statistics**: Comprehensive metrics and performance tracking
//! - **Configurable**: Fully dynamic configuration without hard-coded values
//! - **Thread Safe**: Designed for concurrent access by multiple workers
//! - **Backpressure**: Capacity management to prevent memory exhaustion
//!
//! # Basic Usage
//!
//! ```rust,no_run
//! use midas_fetcher::app::queue::WorkQueue;
//! use midas_fetcher::app::models::FileInfo;
//! use midas_fetcher::app::hash::Md5Hash;
//! use std::path::Path;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a queue with default configuration
//! let queue = WorkQueue::new();
//!
//! // Add work to the queue
//! let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e")?;
//! let path = "./data/uk-daily-temperature-obs/test.csv";
//! let cache_dir = Path::new("/tmp/cache");
//! let file_info = FileInfo::new(hash, path.to_string(), cache_dir, None)?;
//! queue.add_work(file_info).await?;
//!
//! // Workers can claim work
//! while let Some(work) = queue.get_next_work().await {
//!     // Simulate processing the work...
//!     let processing_result: Result<(), String> = Ok(());
//!     match processing_result {
//!         Ok(_) => {
//!             queue.mark_completed(work.work_id()).await?;
//!         }
//!         Err(e) => {
//!             queue.mark_failed(work.work_id(), &e.to_string()).await?;
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced Configuration
//!
//! ```rust,no_run
//! use midas_fetcher::app::queue::{WorkQueue, ConfigPresets, WorkQueueConfigBuilder};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Use a preset configuration
//! let queue = WorkQueue::with_config(ConfigPresets::high_throughput());
//!
//! // Or build custom configuration
//! let config = WorkQueueConfigBuilder::new()
//!     .max_retries(5)
//!     .retry_delay(Duration::from_secs(60))
//!     .max_workers(16)
//!     .build();
//!
//! let queue = WorkQueue::with_config(config);
//! # Ok(())
//! # }
//! ```
//!
//! # Statistics and Monitoring
//!
//! ```rust,no_run
//! use midas_fetcher::app::queue::{WorkQueue, StatsReporter};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let queue = WorkQueue::new();
//!
//! // Get current statistics
//! let stats = queue.stats().await;
//! println!("Queue has {} pending items", stats.pending_count);
//!
//! // Generate reports
//! let report = StatsReporter::generate_summary_report(&stats, 8);
//! println!("{}", report);
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod core;
pub mod state;
pub mod stats;
pub mod types;

#[cfg(test)]
mod tests;

// Re-export main types for public API
pub use config::{ConfigLoader, ConfigPresets, WorkQueueConfigBuilder};
pub use core::WorkQueue;
pub use stats::{PerformanceMetrics, StatsReporter};
pub use types::{QueueStats, WorkInfo, WorkQueueConfig, WorkStatus};

// Re-export commonly used items for convenience
pub use config::ConfigPresets as Presets;
pub use core::WorkQueue as Queue;
pub use stats::StatsReporter as Reporter;
pub use types::WorkInfo as Work;

/// Priority levels for work items
pub mod priority {
    pub use super::config::Priority;

    /// Convenience constants for common priority levels
    pub const URGENT: u32 = 300;
    pub const HIGH: u32 = Priority::HIGH;
    pub const NORMAL: u32 = Priority::DEFAULT;
    pub const LOW: u32 = Priority::LOW;
    pub const BACKGROUND: u32 = 25;
}

/// Result type for queue operations
pub type QueueResult<T> = Result<T, crate::errors::DownloadError>;

/// Builder pattern for creating work queues with fluent API
pub struct WorkQueueBuilder {
    config_builder: WorkQueueConfigBuilder,
}

impl WorkQueueBuilder {
    /// Create a new work queue builder
    pub fn new() -> Self {
        Self {
            config_builder: WorkQueueConfigBuilder::new(),
        }
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.config_builder = self.config_builder.max_retries(max_retries);
        self
    }

    /// Set retry delay
    pub fn with_retry_delay(mut self, delay: std::time::Duration) -> Self {
        self.config_builder = self.config_builder.retry_delay(delay);
        self
    }

    /// Set maximum workers
    pub fn with_max_workers(mut self, max_workers: u32) -> Self {
        self.config_builder = self.config_builder.max_workers(max_workers);
        self
    }

    /// Set work timeout
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config_builder = self.config_builder.work_timeout(timeout);
        self
    }

    /// Set maximum pending items
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.config_builder = self.config_builder.max_pending_items(capacity);
        self
    }

    /// Build the work queue
    pub fn build(self) -> WorkQueue {
        let config = self.config_builder.build();
        WorkQueue::with_config(config)
    }
}

impl Default for WorkQueueBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions for queue operations
pub mod utils {
    use super::*;
    use crate::app::models::FileInfo;

    /// Create a work queue optimized for testing
    pub fn create_test_queue() -> WorkQueue {
        WorkQueue::with_config(ConfigPresets::testing())
    }

    /// Add multiple files to queue with progress tracking
    pub async fn add_files_with_progress<F>(
        queue: &WorkQueue,
        files: Vec<FileInfo>,
        mut progress_callback: F,
    ) -> QueueResult<usize>
    where
        F: FnMut(usize, usize),
    {
        let total = files.len();
        let mut added = 0;

        for (i, file) in files.into_iter().enumerate() {
            if queue.add_work(file).await? {
                added += 1;
            }
            progress_callback(i + 1, total);
        }

        Ok(added)
    }

    /// Wait for queue to finish with timeout
    pub async fn wait_for_completion(
        queue: &WorkQueue,
        timeout: std::time::Duration,
    ) -> QueueResult<bool> {
        let start = std::time::Instant::now();

        while !queue.is_finished().await {
            if start.elapsed() > timeout {
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(true)
    }

    /// Get detailed queue health information
    pub async fn get_queue_health(queue: &WorkQueue) -> QueueHealth {
        let stats = queue.stats().await;
        let config = queue.config();

        QueueHealth {
            is_healthy: stats.success_rate() > 80.0,
            utilization: stats.worker_utilization(config.max_workers),
            backlog_size: stats.pending_count,
            error_rate: if stats.total_added > 0 {
                (stats.abandoned_count as f64 / stats.total_added as f64) * 100.0
            } else {
                0.0
            },
        }
    }
}

/// Queue health information
#[derive(Debug, Clone)]
pub struct QueueHealth {
    pub is_healthy: bool,
    pub utilization: f64,
    pub backlog_size: u64,
    pub error_rate: f64,
}

#[cfg(test)]
mod integration_tests {
    use super::*;
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
    async fn test_queue_builder_pattern() {
        // Test the fluent builder API
        let queue = WorkQueueBuilder::new()
            .with_max_retries(5)
            .with_retry_delay(Duration::from_secs(10))
            .with_max_workers(4)
            .with_capacity(1000)
            .build();

        let config = queue.config();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay, Duration::from_secs(10));
        assert_eq!(config.max_workers, 4);
        assert_eq!(config.max_pending_items, 1000);
    }

    #[tokio::test]
    async fn test_priority_levels() {
        // Test priority handling
        let queue = WorkQueue::new();

        // Add work with different priorities
        let high_priority_file = create_test_file_info("1", "01381");
        let normal_priority_file = create_test_file_info("2", "01382");
        let low_priority_file = create_test_file_info("3", "01383");

        queue
            .add_work_with_priority(high_priority_file, priority::HIGH)
            .await
            .unwrap();
        queue
            .add_work_with_priority(normal_priority_file, priority::NORMAL)
            .await
            .unwrap();
        queue
            .add_work_with_priority(low_priority_file, priority::LOW)
            .await
            .unwrap();

        let stats = queue.stats().await;
        assert_eq!(stats.pending_count, 3);
    }

    #[tokio::test]
    async fn test_configuration_presets() {
        // Test different configuration presets
        let prod_queue = WorkQueue::with_config(ConfigPresets::production());
        let dev_queue = WorkQueue::with_config(ConfigPresets::development());
        let test_queue = WorkQueue::with_config(ConfigPresets::testing());

        assert_eq!(prod_queue.config().max_retries, 3);
        assert_eq!(dev_queue.config().max_retries, 2);
        assert_eq!(test_queue.config().max_retries, 2);

        // Test queue works with different configs
        let file = create_test_file_info("1", "01381");
        assert!(prod_queue.add_work(file).await.unwrap());
    }

    #[tokio::test]
    async fn test_utility_functions() {
        // Test utility functions
        let queue = utils::create_test_queue();
        assert_eq!(queue.config().max_retries, 2); // Testing config

        // Test bulk add with progress
        let files = vec![
            create_test_file_info("1", "01381"),
            create_test_file_info("2", "01382"),
            create_test_file_info("3", "01383"),
        ];

        let mut progress_calls = 0;
        let added = utils::add_files_with_progress(&queue, files, |current, total| {
            progress_calls += 1;
            assert!(current <= total);
        })
        .await
        .unwrap();

        assert_eq!(added, 3);
        assert_eq!(progress_calls, 3);
    }

    #[tokio::test]
    async fn test_queue_health_monitoring() {
        // Test health monitoring
        let queue = WorkQueue::new();

        // Add some work
        let file = create_test_file_info("1", "01381");
        queue.add_work(file).await.unwrap();

        let health = utils::get_queue_health(&queue).await;
        assert!(health.backlog_size > 0);
        assert_eq!(health.error_rate, 0.0); // No errors yet
    }

    #[tokio::test]
    async fn test_concurrent_queue_operations() {
        // Test concurrent operations across the API
        let queue = Arc::new(WorkQueue::new());
        let mut add_handles = Vec::new();
        let mut process_handles = Vec::new();

        // Spawn multiple tasks that add work
        for i in 0..10 {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                let file = create_test_file_info(&format!("{}", i), &format!("0138{}", i));
                queue_clone.add_work(file).await
            });
            add_handles.push(handle);
        }

        // Spawn tasks that process work
        for _ in 0..3 {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                let mut processed = 0;
                while let Some(work) = queue_clone.get_next_work().await {
                    // Simulate processing
                    sleep(Duration::from_millis(1)).await;

                    // Mark as completed
                    queue_clone.mark_completed(work.work_id()).await.unwrap();
                    processed += 1;

                    if processed >= 3 {
                        break;
                    }
                }
            });
            process_handles.push(handle);
        }

        // Wait for all add tasks to complete
        for handle in add_handles {
            handle.await.unwrap().unwrap();
        }

        // Wait for all process tasks to complete
        for handle in process_handles {
            handle.await.unwrap();
        }

        // Check final state
        let stats = queue.stats().await;
        assert!(stats.total_added > 0);
        assert!(stats.completed_count > 0);
    }

    #[tokio::test]
    async fn test_stats_reporting() {
        // Test statistics reporting
        let queue = WorkQueue::new();

        // Add and process some work
        let file = create_test_file_info("1", "01381");
        queue.add_work(file).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        queue.mark_completed(work.work_id()).await.unwrap();

        let stats = queue.stats().await;
        let report = StatsReporter::generate_summary_report(&stats, 8);

        assert!(report.contains("Total Added: 1"));
        assert!(report.contains("Completed: 1"));
        assert!(report.contains("Success Rate:"));

        let compact = StatsReporter::generate_compact_status(&stats);
        assert!(compact.contains("1 total"));
        assert!(compact.contains("1 completed"));
    }
}
