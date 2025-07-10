//! Worker type definitions and data structures
//!
//! This module contains all the data types used by the worker system including
//! progress reporting, status tracking, and statistics collection structures.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::app::models::FileInfo;

/// Progress information from a download worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerProgress {
    /// ID of the worker reporting progress
    pub worker_id: u32,
    /// File currently being processed
    pub file_info: Option<FileInfo>,
    /// Number of bytes downloaded for current file
    pub bytes_downloaded: u64,
    /// Total size of current file (if known)
    pub total_bytes: Option<u64>,
    /// Download speed in bytes per second
    pub download_speed: f64,
    /// Estimated time remaining for current file
    pub eta_seconds: Option<f64>,
    /// Current worker status
    pub status: WorkerStatus,
    /// Timestamp of this progress report
    pub timestamp: DateTime<Utc>,
    /// Total files completed by this worker
    pub files_completed: u64,
    /// Total bytes downloaded by this worker
    pub total_bytes_downloaded: u64,
    /// Current error message (if any)
    pub error_message: Option<String>,
}

impl WorkerProgress {
    /// Create a new progress report with minimal required information
    pub fn new(worker_id: u32, status: WorkerStatus) -> Self {
        Self {
            worker_id,
            file_info: None,
            bytes_downloaded: 0,
            total_bytes: None,
            download_speed: 0.0,
            eta_seconds: None,
            status,
            timestamp: Utc::now(),
            files_completed: 0,
            total_bytes_downloaded: 0,
            error_message: None,
        }
    }

    /// Calculate download progress percentage if total bytes is known
    pub fn progress_percentage(&self) -> Option<f64> {
        self.total_bytes.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.bytes_downloaded as f64 / total as f64) * 100.0
            }
        })
    }

    /// Check if this worker is currently active (downloading or processing)
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            WorkerStatus::RequestingWork
                | WorkerStatus::CheckingCache
                | WorkerStatus::Downloading
                | WorkerStatus::Saving
        )
    }

    /// Get a human-readable status description
    pub fn status_description(&self) -> String {
        match &self.status {
            WorkerStatus::Idle => "Waiting for work".to_string(),
            WorkerStatus::RequestingWork => "Looking for work".to_string(),
            WorkerStatus::CheckingCache => "Checking cache".to_string(),
            WorkerStatus::Downloading => {
                if let Some(file) = &self.file_info {
                    format!("Downloading {}", file.file_name)
                } else {
                    "Downloading file".to_string()
                }
            }
            WorkerStatus::Saving => "Saving to cache".to_string(),
            WorkerStatus::Error { retry_count } => {
                format!("Error (retry {})", retry_count)
            }
            WorkerStatus::Shutdown => "Shutting down".to_string(),
        }
    }
}

/// Current status of a download worker
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WorkerStatus {
    /// Worker is idle, waiting for work
    Idle,
    /// Worker is requesting work from queue
    RequestingWork,
    /// Worker is attempting cache reservation
    CheckingCache,
    /// Worker is downloading a file
    Downloading,
    /// Worker is saving file to cache
    Saving,
    /// Worker encountered an error
    Error { retry_count: u32 },
    /// Worker is shutting down
    Shutdown,
}

impl WorkerStatus {
    /// Check if this status indicates the worker is actively working
    pub fn is_working(&self) -> bool {
        matches!(
            self,
            WorkerStatus::RequestingWork
                | WorkerStatus::CheckingCache
                | WorkerStatus::Downloading
                | WorkerStatus::Saving
        )
    }

    /// Check if this status indicates an error state
    pub fn is_error(&self) -> bool {
        matches!(self, WorkerStatus::Error { .. })
    }

    /// Get the priority of this status for display purposes
    pub fn display_priority(&self) -> u8 {
        match self {
            WorkerStatus::Error { .. } => 5,
            WorkerStatus::Downloading => 4,
            WorkerStatus::Saving => 3,
            WorkerStatus::CheckingCache => 2,
            WorkerStatus::RequestingWork => 2,
            WorkerStatus::Idle => 1,
            WorkerStatus::Shutdown => 0,
        }
    }
}

/// Statistics for a worker pool
#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerPoolStats {
    /// Total number of active workers
    pub active_workers: usize,
    /// Total files downloaded successfully
    pub files_completed: u64,
    /// Total files that failed permanently
    pub files_failed: u64,
    /// Total bytes downloaded
    pub total_bytes_downloaded: u64,
    /// Average download speed across all workers
    pub average_download_speed: f64,
    /// Estimated time remaining for all work
    pub estimated_completion_time: Option<Duration>,
    /// Workers by current status
    pub workers_by_status: HashMap<WorkerStatus, usize>,
}

impl WorkerPoolStats {
    /// Calculate overall success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        let total_completed = self.files_completed + self.files_failed;
        if total_completed == 0 {
            0.0
        } else {
            (self.files_completed as f64 / total_completed as f64) * 100.0
        }
    }

    /// Calculate worker utilization as a percentage
    pub fn worker_utilization(&self) -> f64 {
        if self.active_workers == 0 {
            0.0
        } else {
            let working_workers = self
                .workers_by_status
                .iter()
                .filter(|(status, _)| status.is_working())
                .map(|(_, count)| count)
                .sum::<usize>();

            (working_workers as f64 / self.active_workers as f64) * 100.0
        }
    }

    /// Get the number of workers in error state
    pub fn error_count(&self) -> usize {
        self.workers_by_status
            .iter()
            .filter(|(status, _)| status.is_error())
            .map(|(_, count)| count)
            .sum()
    }

    /// Get the number of idle workers
    pub fn idle_count(&self) -> usize {
        self.workers_by_status
            .get(&WorkerStatus::Idle)
            .copied()
            .unwrap_or(0)
    }

    /// Get human-readable throughput description
    pub fn throughput_description(&self) -> String {
        if self.average_download_speed > 1_000_000.0 {
            format!("{:.1} MB/s", self.average_download_speed / 1_000_000.0)
        } else if self.average_download_speed > 1_000.0 {
            format!("{:.1} KB/s", self.average_download_speed / 1_000.0)
        } else {
            format!("{:.0} B/s", self.average_download_speed)
        }
    }
}

/// Internal statistics for a worker (not exposed in public API)
#[derive(Debug, Default)]
pub(crate) struct WorkerStats {
    pub files_completed: u64,
    pub total_bytes_downloaded: u64,
    pub current_download_start: Option<Instant>,
    pub current_bytes_downloaded: u64,
    pub speed_samples: Vec<(Instant, u64)>, // (timestamp, bytes) for speed calculation
    pub consecutive_empty_polls: u32,       // Track consecutive times we found no work
    pub last_empty_poll: Option<Instant>,
}

impl WorkerStats {
    /// Reset statistics for a new download
    pub fn reset_current_download(&mut self) {
        self.current_download_start = Some(Instant::now());
        self.current_bytes_downloaded = 0;
        self.speed_samples.clear();
    }

    /// Complete the current download
    pub fn complete_download(&mut self, total_bytes: u64) {
        self.files_completed += 1;
        self.total_bytes_downloaded += total_bytes;
        self.current_download_start = None;
        self.current_bytes_downloaded = 0;
        self.speed_samples.clear();
    }

    /// Track consecutive empty polls for backoff calculation
    pub fn record_empty_poll(&mut self) {
        self.consecutive_empty_polls += 1;
        self.last_empty_poll = Some(Instant::now());
    }

    /// Reset empty poll tracking after finding work
    pub fn reset_empty_polls(&mut self) {
        self.consecutive_empty_polls = 0;
        self.last_empty_poll = None;
    }

    /// Record progress for current download
    #[allow(dead_code)]
    pub fn record_progress(&mut self, bytes: u64) {
        self.current_bytes_downloaded += bytes;
        let now = Instant::now();
        self.speed_samples
            .push((now, self.current_bytes_downloaded));
    }
}

/// Result type for worker operations
pub type WorkerResult<T> = Result<T, crate::errors::DownloadError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test WorkerProgress creation and utility methods
    ///
    /// Verifies that progress reports can be created and provide
    /// correct calculations for percentages and status descriptions.
    #[test]
    fn test_worker_progress() {
        let mut progress = WorkerProgress::new(1, WorkerStatus::Idle);
        assert_eq!(progress.worker_id, 1);
        assert_eq!(progress.status, WorkerStatus::Idle);
        assert!(!progress.is_active());

        // Test progress percentage calculation
        progress.bytes_downloaded = 50;
        progress.total_bytes = Some(100);
        assert_eq!(progress.progress_percentage(), Some(50.0));

        // Test with zero total bytes
        progress.total_bytes = Some(0);
        assert_eq!(progress.progress_percentage(), Some(100.0));

        // Test with unknown total
        progress.total_bytes = None;
        assert_eq!(progress.progress_percentage(), None);

        // Test active status
        progress.status = WorkerStatus::Downloading;
        assert!(progress.is_active());
    }

    /// Test WorkerStatus utility methods
    ///
    /// Ensures status enumeration provides correct information about
    /// working state, error state, and display priorities.
    #[test]
    fn test_worker_status() {
        assert!(WorkerStatus::Downloading.is_working());
        assert!(!WorkerStatus::Idle.is_working());
        assert!(!WorkerStatus::Shutdown.is_working());

        assert!(WorkerStatus::Error { retry_count: 1 }.is_error());
        assert!(!WorkerStatus::Downloading.is_error());

        // Test display priorities
        assert!(
            WorkerStatus::Error { retry_count: 1 }.display_priority()
                > WorkerStatus::Idle.display_priority()
        );
        assert!(
            WorkerStatus::Downloading.display_priority()
                > WorkerStatus::RequestingWork.display_priority()
        );
    }

    /// Test WorkerPoolStats calculations
    ///
    /// Verifies that pool statistics correctly calculate success rates,
    /// utilization percentages, and other derived metrics.
    #[test]
    fn test_worker_pool_stats() {
        let mut stats = WorkerPoolStats::default();

        // Test success rate with no data
        assert_eq!(stats.success_rate(), 0.0);

        // Test success rate with data
        stats.files_completed = 80;
        stats.files_failed = 20;
        assert_eq!(stats.success_rate(), 80.0);

        // Test worker utilization
        stats.active_workers = 4;
        stats.workers_by_status.insert(WorkerStatus::Downloading, 2);
        stats.workers_by_status.insert(WorkerStatus::Idle, 2);
        assert_eq!(stats.worker_utilization(), 50.0);

        // Test error and idle counts
        stats
            .workers_by_status
            .insert(WorkerStatus::Error { retry_count: 1 }, 1);
        stats.workers_by_status.insert(WorkerStatus::Idle, 1);
        assert_eq!(stats.error_count(), 1);
        assert_eq!(stats.idle_count(), 1);

        // Test throughput description
        stats.average_download_speed = 1_500_000.0;
        assert!(stats.throughput_description().contains("MB/s"));

        stats.average_download_speed = 1_500.0;
        assert!(stats.throughput_description().contains("KB/s"));

        stats.average_download_speed = 500.0;
        assert!(stats.throughput_description().contains("B/s"));
    }

    /// Test WorkerStats internal tracking
    ///
    /// Ensures internal statistics correctly track download progress,
    /// file completion, and empty poll backoff state.
    #[test]
    fn test_worker_stats() {
        let mut stats = WorkerStats::default();

        // Test download tracking
        stats.reset_current_download();
        assert!(stats.current_download_start.is_some());
        assert_eq!(stats.current_bytes_downloaded, 0);
        assert!(stats.speed_samples.is_empty());

        // Test progress recording
        stats.record_progress(1024);
        assert_eq!(stats.current_bytes_downloaded, 1024);
        assert_eq!(stats.speed_samples.len(), 1);

        // Test download completion
        stats.complete_download(2048);
        assert_eq!(stats.files_completed, 1);
        assert_eq!(stats.total_bytes_downloaded, 2048);
        assert!(stats.current_download_start.is_none());

        // Test empty poll tracking
        stats.record_empty_poll();
        assert_eq!(stats.consecutive_empty_polls, 1);
        assert!(stats.last_empty_poll.is_some());

        stats.record_empty_poll();
        assert_eq!(stats.consecutive_empty_polls, 2);

        stats.reset_empty_polls();
        assert_eq!(stats.consecutive_empty_polls, 0);
        assert!(stats.last_empty_poll.is_none());
    }

    /// Test serialization of all WorkerStatus variants
    ///
    /// Ensures that all possible worker status states can be properly
    /// serialized and deserialized for progress monitoring systems.
    #[test]
    fn test_worker_status_serialization() {
        let statuses = vec![
            WorkerStatus::Idle,
            WorkerStatus::RequestingWork,
            WorkerStatus::CheckingCache,
            WorkerStatus::Downloading,
            WorkerStatus::Saving,
            WorkerStatus::Error { retry_count: 2 },
            WorkerStatus::Shutdown,
        ];

        for status in statuses {
            let serialized = serde_json::to_string(&status).unwrap();
            let deserialized: WorkerStatus = serde_json::from_str(&serialized).unwrap();
            assert_eq!(status, deserialized);
        }
    }

    /// Test WorkerProgress serialization with complex data
    ///
    /// Verifies that progress reports with all fields populated can be
    /// correctly serialized and deserialized for monitoring systems.
    #[test]
    fn test_worker_progress_serialization() {
        let progress = WorkerProgress {
            worker_id: 1,
            file_info: None, // FileInfo serialization tested elsewhere
            bytes_downloaded: 1024,
            total_bytes: Some(2048),
            download_speed: 1024.0,
            eta_seconds: Some(1.0),
            status: WorkerStatus::Downloading,
            timestamp: Utc::now(),
            files_completed: 5,
            total_bytes_downloaded: 10240,
            error_message: Some("Test error".to_string()),
        };

        let serialized = serde_json::to_string(&progress).unwrap();
        let deserialized: WorkerProgress = serde_json::from_str(&serialized).unwrap();

        assert_eq!(progress.worker_id, deserialized.worker_id);
        assert_eq!(progress.bytes_downloaded, deserialized.bytes_downloaded);
        assert_eq!(progress.status, deserialized.status);
        assert_eq!(progress.error_message, deserialized.error_message);
    }
}
