//! Core data structures for the work queue system
//!
//! This module defines the fundamental types used throughout the queue system,
//! including work status, work information, configuration, and statistics.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::app::hash::Md5Hash;
use crate::app::models::FileInfo;

/// Status of a work item in the queue system
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkStatus {
    /// Work is waiting to be claimed by a worker
    Pending,
    /// Work is currently being processed by a worker
    InProgress {
        worker_id: u32,
        started_at: DateTime<Utc>,
        previous_failures: u32,
    },
    /// Work completed successfully
    Completed { completed_at: DateTime<Utc> },
    /// Work failed and is waiting for retry
    Failed {
        failure_count: u32,
        last_failure: DateTime<Utc>,
        error: String,
    },
    /// Work failed permanently (exceeded retry limit)
    Abandoned {
        failure_count: u32,
        abandoned_at: DateTime<Utc>,
        final_error: String,
    },
}

impl WorkStatus {
    /// Check if this work item is ready for retry
    pub fn is_ready_for_retry(&self, retry_delay: Duration) -> bool {
        match self {
            WorkStatus::Failed { last_failure, .. } => {
                let elapsed = Utc::now()
                    .signed_duration_since(*last_failure)
                    .to_std()
                    .unwrap_or(Duration::ZERO);
                elapsed >= retry_delay
            }
            _ => false,
        }
    }

    /// Get failure count if this is a failed work item
    pub fn failure_count(&self) -> u32 {
        match self {
            WorkStatus::Failed { failure_count, .. } => *failure_count,
            WorkStatus::Abandoned { failure_count, .. } => *failure_count,
            WorkStatus::InProgress {
                previous_failures, ..
            } => *previous_failures,
            _ => 0,
        }
    }

    /// Check if this status represents completed work
    pub fn is_completed(&self) -> bool {
        matches!(self, WorkStatus::Completed { .. })
    }

    /// Check if this status represents work in progress
    pub fn is_in_progress(&self) -> bool {
        matches!(self, WorkStatus::InProgress { .. })
    }

    /// Check if this status represents abandoned work
    pub fn is_abandoned(&self) -> bool {
        matches!(self, WorkStatus::Abandoned { .. })
    }

    /// Check if this status represents failed work
    pub fn is_failed(&self) -> bool {
        matches!(self, WorkStatus::Failed { .. })
    }

    /// Check if this status represents pending work
    pub fn is_pending(&self) -> bool {
        matches!(self, WorkStatus::Pending)
    }
}

/// Information about work being processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkInfo {
    /// The file to be downloaded
    pub file_info: FileInfo,
    /// Current status of this work
    pub status: WorkStatus,
    /// When this work was first added to the queue
    pub created_at: DateTime<Utc>,
    /// Priority of this work (higher = more important)
    pub priority: u32,
}

impl WorkInfo {
    /// Create new work info for a file
    pub fn new(file_info: FileInfo, priority: u32) -> Self {
        Self {
            file_info,
            status: WorkStatus::Pending,
            created_at: Utc::now(),
            priority,
        }
    }

    /// Get the unique identifier for this work (file hash)
    pub fn work_id(&self) -> &Md5Hash {
        &self.file_info.hash
    }

    /// Check if this work is ready to be processed
    pub fn is_available(&self, retry_delay: Duration) -> bool {
        self.status.is_pending() || self.status.is_ready_for_retry(retry_delay)
    }

    /// Check if this work is currently being processed
    pub fn is_in_progress(&self) -> bool {
        self.status.is_in_progress()
    }

    /// Check if this work is completed
    pub fn is_completed(&self) -> bool {
        self.status.is_completed()
    }

    /// Check if this work has failed permanently
    pub fn is_abandoned(&self) -> bool {
        self.status.is_abandoned()
    }

    /// Check if this work has failed but can be retried
    pub fn is_failed(&self) -> bool {
        self.status.is_failed()
    }

    /// Mark this work as in progress
    pub fn mark_in_progress(&mut self, worker_id: u32) {
        let previous_failures = self.status.failure_count();
        self.status = WorkStatus::InProgress {
            worker_id,
            started_at: Utc::now(),
            previous_failures,
        };
    }

    /// Mark this work as completed
    pub fn mark_completed(&mut self) {
        self.status = WorkStatus::Completed {
            completed_at: Utc::now(),
        };
    }

    /// Mark this work as failed
    pub fn mark_failed(&mut self, error: String, max_retries: u32) {
        let failure_count = self.status.failure_count() + 1;

        if failure_count >= max_retries {
            self.status = WorkStatus::Abandoned {
                failure_count,
                abandoned_at: Utc::now(),
                final_error: error,
            };
        } else {
            self.status = WorkStatus::Failed {
                failure_count,
                last_failure: Utc::now(),
                error,
            };
        }
    }
}

/// Configuration for the work queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueConfig {
    /// Maximum number of retry attempts per file
    pub max_retries: u32,
    /// Delay between retry attempts
    pub retry_delay: Duration,
    /// Maximum number of workers that can process work simultaneously
    pub max_workers: u32,
    /// Timeout for work items (if a worker doesn't report progress)
    pub work_timeout: Duration,
    /// Maximum number of pending items before applying backpressure
    pub max_pending_items: usize,
    /// Default priority for new work items
    pub default_priority: u32,
}

impl WorkQueueConfig {
    /// Create a new configuration with sensible defaults
    pub fn new() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_secs(30),
            max_workers: 8,
            work_timeout: Duration::from_secs(300),
            max_pending_items: 50_000,
            default_priority: 100,
        }
    }

    /// Create a configuration optimized for testing
    pub fn for_testing() -> Self {
        Self {
            max_retries: 2,
            retry_delay: Duration::from_millis(10),
            max_workers: 4,
            work_timeout: Duration::from_millis(100),
            max_pending_items: 100,
            default_priority: 100,
        }
    }

    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.max_retries == 0 {
            return Err("max_retries must be greater than 0".to_string());
        }
        if self.max_workers == 0 {
            return Err("max_workers must be greater than 0".to_string());
        }
        if self.max_pending_items == 0 {
            return Err("max_pending_items must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about queue operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    /// Total work items added to queue
    pub total_added: u64,
    /// Work items currently pending
    pub pending_count: u64,
    /// Work items currently in progress
    pub in_progress_count: u64,
    /// Work items completed successfully
    pub completed_count: u64,
    /// Work items failed and awaiting retry
    pub failed_count: u64,
    /// Work items permanently abandoned
    pub abandoned_count: u64,
    /// Total duplicate items encountered
    pub duplicate_count: u64,
    /// Average processing time for completed items
    pub avg_processing_time: Duration,
    /// Queue creation time
    pub created_at: DateTime<Utc>,
}

impl QueueStats {
    /// Create new statistics with current timestamp
    pub fn new() -> Self {
        Self {
            created_at: Utc::now(),
            ..Default::default()
        }
    }

    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total_processed = self.completed_count + self.abandoned_count;
        if total_processed == 0 {
            0.0
        } else {
            (self.completed_count as f64 / total_processed as f64) * 100.0
        }
    }

    /// Calculate total active items (pending + in_progress + failed)
    pub fn active_count(&self) -> u64 {
        self.pending_count + self.in_progress_count + self.failed_count
    }

    /// Calculate worker utilization (in_progress / max_workers)
    pub fn worker_utilization(&self, max_workers: u32) -> f64 {
        if max_workers == 0 {
            0.0
        } else {
            (self.in_progress_count as f64 / max_workers as f64).min(1.0) * 100.0
        }
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.active_count() == 0
    }

    /// Check if the queue is at capacity
    pub fn is_at_capacity(&self, max_pending: usize) -> bool {
        self.active_count() >= max_pending as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_file_info() -> FileInfo {
        let temp_dir = TempDir::new().unwrap();
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let path = "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1980.csv";
        FileInfo::new(hash, path.to_string(), temp_dir.path(), None).unwrap()
    }

    #[test]
    fn test_work_status_pending() {
        let status = WorkStatus::Pending;
        assert!(status.is_pending());
        assert!(!status.is_in_progress());
        assert!(!status.is_completed());
        assert!(!status.is_failed());
        assert!(!status.is_abandoned());
        assert_eq!(status.failure_count(), 0);
    }

    #[test]
    fn test_work_status_in_progress() {
        let status = WorkStatus::InProgress {
            worker_id: 1,
            started_at: Utc::now(),
            previous_failures: 2,
        };
        assert!(!status.is_pending());
        assert!(status.is_in_progress());
        assert!(!status.is_completed());
        assert!(!status.is_failed());
        assert!(!status.is_abandoned());
        assert_eq!(status.failure_count(), 2);
    }

    #[test]
    fn test_work_status_failed_retry_ready() {
        let past_time = Utc::now() - chrono::Duration::seconds(60);
        let status = WorkStatus::Failed {
            failure_count: 1,
            last_failure: past_time,
            error: "Test error".to_string(),
        };

        assert!(status.is_ready_for_retry(Duration::from_secs(30)));
        assert!(!status.is_ready_for_retry(Duration::from_secs(120)));
    }

    #[test]
    fn test_work_info_creation() {
        let file_info = create_test_file_info();
        let work = WorkInfo::new(file_info, 100);

        assert_eq!(work.priority, 100);
        assert!(work.status.is_pending());
        assert!(work.is_available(Duration::from_secs(1)));
    }

    #[test]
    fn test_work_info_state_transitions() {
        let file_info = create_test_file_info();
        let mut work = WorkInfo::new(file_info, 100);

        // Mark as in progress
        work.mark_in_progress(1);
        assert!(work.is_in_progress());

        // Mark as completed
        work.mark_completed();
        assert!(work.is_completed());
    }

    #[test]
    fn test_work_info_failure_handling() {
        let file_info = create_test_file_info();
        let mut work = WorkInfo::new(file_info, 100);

        // First failure
        work.mark_failed("Error 1".to_string(), 3);
        assert!(work.is_failed());
        assert_eq!(work.status.failure_count(), 1);

        // Second failure
        work.mark_failed("Error 2".to_string(), 3);
        assert!(work.is_failed());
        assert_eq!(work.status.failure_count(), 2);

        // Final failure (abandon)
        work.mark_failed("Error 3".to_string(), 3);
        assert!(work.is_abandoned());
        assert_eq!(work.status.failure_count(), 3);
    }

    #[test]
    fn test_config_validation() {
        let valid_config = WorkQueueConfig::new();
        assert!(valid_config.validate().is_ok());

        let invalid_config = WorkQueueConfig {
            max_retries: 0,
            ..WorkQueueConfig::new()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_queue_stats_calculations() {
        let mut stats = QueueStats::new();
        stats.completed_count = 80;
        stats.abandoned_count = 20;
        stats.pending_count = 10;
        stats.in_progress_count = 5;
        stats.failed_count = 3;

        assert_eq!(stats.success_rate(), 80.0);
        assert_eq!(stats.active_count(), 18);
        assert_eq!(stats.worker_utilization(10), 50.0);
        assert!(!stats.is_empty());
    }
}
