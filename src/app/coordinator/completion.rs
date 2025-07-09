//! Download completion detection and monitoring
//!
//! This module handles the complex logic for detecting when all downloads
//! are complete, including various completion criteria and timeout handling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use crate::app::queue::WorkQueue;
use crate::constants::coordinator;

/// Completion detector for monitoring download progress and detecting completion
pub struct CompletionDetector {
    queue: Arc<WorkQueue>,
    expected_total_files: u64,
}

impl CompletionDetector {
    /// Create a new completion detector
    pub fn new(queue: Arc<WorkQueue>, expected_total_files: u64) -> Self {
        Self {
            queue,
            expected_total_files,
        }
    }

    /// Wait for all downloads to complete naturally
    ///
    /// This method implements enhanced completion detection using multiple criteria:
    /// 1. Queue state (no pending or in-progress items)
    /// 2. File count completion (if expected total is known)
    /// 3. Progress-based timeout (detects stalled downloads)
    pub async fn wait_for_completion(&self) {
        debug!("Starting completion detection");
        let mut iteration_count = 0;

        // Progress-based timeout tracking
        let mut last_progress_time = Instant::now();
        let mut last_total_processed = 0u64;

        loop {
            // Force cleanup of any stale queue state before checking completion
            if iteration_count % coordinator::COMPLETION_CLEANUP_FREQUENCY == 0 {
                let _cleaned = self.queue.cleanup().await;
                let _timed_out = self.queue.handle_timeouts().await;
            }

            let queue_stats = self.queue.stats().await;
            iteration_count += 1;

            // Log state periodically
            if iteration_count % coordinator::COMPLETION_LOG_FREQUENCY == 0 {
                info!(
                    "Waiting for completion: pending={}, in_progress={}, completed={}, failed={} (iteration {})",
                    queue_stats.pending_count,
                    queue_stats.in_progress_count,
                    queue_stats.completed_count,
                    queue_stats.failed_count,
                    iteration_count
                );
            }

            // Enhanced completion detection: use multiple criteria
            let total_processed = queue_stats.completed_count + queue_stats.abandoned_count;
            let strict_queue_empty =
                queue_stats.pending_count == 0 && queue_stats.in_progress_count == 0;

            // Only use file count completion if we have a valid total (> 0)
            let all_files_processed =
                self.expected_total_files > 0 && total_processed >= self.expected_total_files;

            // Check for progress and update timeout tracking
            if total_processed > last_total_processed {
                last_progress_time = Instant::now();
                last_total_processed = total_processed;
            }

            // Log completion checks periodically
            if iteration_count % (coordinator::COMPLETION_LOG_FREQUENCY * 6) == 0 {
                debug!(
                    "Completion check {}: pending={}, in_progress={}, completed={}, total_processed={}/{}",
                    iteration_count,
                    queue_stats.pending_count,
                    queue_stats.in_progress_count,
                    queue_stats.completed_count,
                    total_processed,
                    self.expected_total_files
                );
            }

            // Only consider queue empty as completion if we have either:
            // 1. No expected total files (legacy behavior)
            // 2. Have processed some files already (not startup state)
            let can_complete_on_empty_queue = self.expected_total_files == 0 || total_processed > 0;
            let queue_empty_completion = strict_queue_empty && can_complete_on_empty_queue;

            // Check if all work is done using enhanced logic
            if queue_empty_completion || all_files_processed {
                // If we completed by file count but queue isn't empty, log diagnostics
                if all_files_processed && !queue_empty_completion {
                    warn!(
                        "All files processed ({}/{}) but queue not empty: pending={}, in_progress={}",
                        total_processed,
                        self.expected_total_files,
                        queue_stats.pending_count,
                        queue_stats.in_progress_count
                    );

                    // Force final cleanup to sync queue state
                    let cleaned = self.queue.cleanup().await;
                    let timed_out = self.queue.handle_timeouts().await;

                    if cleaned > 0 || timed_out > 0 {
                        info!(
                            "Final cleanup: removed {} completed items, {} timed out items",
                            cleaned, timed_out
                        );
                    }
                }

                info!(
                    "All downloads completed: {} successful, {} failed, {} abandoned",
                    queue_stats.completed_count,
                    queue_stats.failed_count,
                    queue_stats.abandoned_count
                );
                break;
            }

            // Progress-based timeout: only timeout if no progress for extended period
            if last_progress_time.elapsed() > coordinator::PROGRESS_TIMEOUT {
                error!(
                    "No progress for {} minutes (processed {}/{}) - forcing exit",
                    coordinator::PROGRESS_TIMEOUT.as_secs() / 60,
                    total_processed,
                    self.expected_total_files
                );
                break;
            }

            // Brief sleep to avoid busy waiting
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        debug!("Completion detection finished");
    }

    /// Check if completion criteria are met without waiting
    pub async fn is_complete(&self) -> bool {
        let queue_stats = self.queue.stats().await;
        let total_processed = queue_stats.completed_count + queue_stats.abandoned_count;
        let strict_queue_empty =
            queue_stats.pending_count == 0 && queue_stats.in_progress_count == 0;

        let all_files_processed =
            self.expected_total_files > 0 && total_processed >= self.expected_total_files;

        let can_complete_on_empty_queue = self.expected_total_files == 0 || total_processed > 0;
        let queue_empty_completion = strict_queue_empty && can_complete_on_empty_queue;

        queue_empty_completion || all_files_processed
    }

    /// Get completion status information
    pub async fn get_completion_status(&self) -> CompletionStatus {
        let queue_stats = self.queue.stats().await;
        let total_processed = queue_stats.completed_count + queue_stats.abandoned_count;

        CompletionStatus {
            total_expected: self.expected_total_files,
            total_processed,
            pending: queue_stats.pending_count,
            in_progress: queue_stats.in_progress_count,
            completed: queue_stats.completed_count,
            failed: queue_stats.failed_count,
            abandoned: queue_stats.abandoned_count,
        }
    }
}

/// Status information for completion monitoring
#[derive(Debug, Clone)]
pub struct CompletionStatus {
    /// Total expected files (0 if unknown)
    pub total_expected: u64,
    /// Total processed files (completed + failed + abandoned)
    pub total_processed: u64,
    /// Files pending processing
    pub pending: u64,
    /// Files currently being processed
    pub in_progress: u64,
    /// Successfully completed files
    pub completed: u64,
    /// Failed files
    pub failed: u64,
    /// Abandoned files
    pub abandoned: u64,
}

impl CompletionStatus {
    /// Calculate completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_expected == 0 {
            return 0.0;
        }
        (self.total_processed as f64 / self.total_expected as f64) * 100.0
    }

    /// Check if all work appears complete
    pub fn is_complete(&self) -> bool {
        let queue_empty = self.pending == 0 && self.in_progress == 0;
        let all_files_processed =
            self.total_expected > 0 && self.total_processed >= self.total_expected;
        let can_complete_on_empty_queue = self.total_expected == 0 || self.total_processed > 0;

        (queue_empty && can_complete_on_empty_queue) || all_files_processed
    }

    /// Get a human-readable status summary
    pub fn summary(&self) -> String {
        if self.total_expected > 0 {
            format!(
                "{}/{} files processed ({:.1}%): {} pending, {} in progress, {} completed, {} failed",
                self.total_processed,
                self.total_expected,
                self.completion_percentage(),
                self.pending,
                self.in_progress,
                self.completed,
                self.failed
            )
        } else {
            format!(
                "{} files processed: {} pending, {} in progress, {} completed, {} failed",
                self.total_processed, self.pending, self.in_progress, self.completed, self.failed
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::hash::Md5Hash;
    use crate::app::models::{DatasetFileInfo, FileInfo};
    use crate::app::WorkQueue;
    use std::path::PathBuf;

    async fn create_test_queue() -> Arc<WorkQueue> {
        Arc::new(WorkQueue::new())
    }

    fn create_test_file_info(name: &str) -> FileInfo {
        // Create a unique hash based on the file name to avoid deduplication
        let mut hash_string = format!(
            "{:x}",
            name.as_bytes()
                .iter()
                .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
        );
        // Pad to 32 characters
        while hash_string.len() < 32 {
            hash_string.push('0');
        }
        let hash = Md5Hash::from_hex(&hash_string[0..32]).unwrap();
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
            destination_path: PathBuf::from(format!("/tmp/{}", name)),
        }
    }

    /// Test completion detector creation
    ///
    /// Verifies that completion detectors can be created with
    /// various expected file counts.
    #[tokio::test]
    async fn test_completion_detector_creation() {
        let queue = create_test_queue().await;
        let detector = CompletionDetector::new(queue, 100);

        let status = detector.get_completion_status().await;
        assert_eq!(status.total_expected, 100);
        assert_eq!(status.total_processed, 0);
    }

    /// Test completion status calculation
    ///
    /// Ensures that completion status accurately reflects the
    /// current state of the work queue.
    #[tokio::test]
    async fn test_completion_status_calculation() {
        let queue = create_test_queue().await;
        let detector = CompletionDetector::new(queue.clone(), 10);

        // Add some work
        for i in 0..5 {
            let file_info = create_test_file_info(&format!("file{}.txt", i));
            queue.add_work(file_info).await.unwrap();
        }

        let status = detector.get_completion_status().await;
        assert_eq!(status.total_expected, 10);
        assert_eq!(status.pending, 5);
        assert!(!status.is_complete());
    }

    /// Test completion percentage calculation
    ///
    /// Verifies that completion percentages are calculated correctly
    /// for various scenarios.
    #[test]
    fn test_completion_percentage() {
        let status = CompletionStatus {
            total_expected: 100,
            total_processed: 50,
            pending: 30,
            in_progress: 20,
            completed: 40,
            failed: 10,
            abandoned: 0,
        };

        assert_eq!(status.completion_percentage(), 50.0);

        // Test with unknown total
        let unknown_status = CompletionStatus {
            total_expected: 0,
            total_processed: 50,
            pending: 0,
            in_progress: 0,
            completed: 50,
            failed: 0,
            abandoned: 0,
        };

        assert_eq!(unknown_status.completion_percentage(), 0.0);
    }

    /// Test completion detection logic
    ///
    /// Ensures that completion is detected correctly under
    /// various queue states and expected file counts.
    #[test]
    fn test_completion_detection_logic() {
        // Test queue empty with known total
        let complete_status = CompletionStatus {
            total_expected: 100,
            total_processed: 100,
            pending: 0,
            in_progress: 0,
            completed: 90,
            failed: 10,
            abandoned: 0,
        };
        assert!(complete_status.is_complete());

        // Test queue empty with unknown total (but some processed)
        let unknown_complete = CompletionStatus {
            total_expected: 0,
            total_processed: 50,
            pending: 0,
            in_progress: 0,
            completed: 50,
            failed: 0,
            abandoned: 0,
        };
        assert!(unknown_complete.is_complete());

        // Test incomplete
        let incomplete_status = CompletionStatus {
            total_expected: 100,
            total_processed: 50,
            pending: 30,
            in_progress: 20,
            completed: 40,
            failed: 10,
            abandoned: 0,
        };
        assert!(!incomplete_status.is_complete());
    }

    /// Test status summary formatting
    ///
    /// Verifies that status summaries provide clear, human-readable
    /// information about download progress.
    #[test]
    fn test_status_summary() {
        let status = CompletionStatus {
            total_expected: 100,
            total_processed: 60,
            pending: 20,
            in_progress: 20,
            completed: 50,
            failed: 10,
            abandoned: 0,
        };

        let summary = status.summary();
        assert!(summary.contains("60/100"));
        assert!(summary.contains("60.0%"));
        assert!(summary.contains("20 pending"));
        assert!(summary.contains("50 completed"));

        // Test unknown total summary
        let unknown_status = CompletionStatus {
            total_expected: 0,
            total_processed: 50,
            pending: 0,
            in_progress: 0,
            completed: 50,
            failed: 0,
            abandoned: 0,
        };

        let unknown_summary = unknown_status.summary();
        assert!(unknown_summary.contains("50 files processed"));
        assert!(!unknown_summary.contains("/"));
        assert!(!unknown_summary.contains("%"));
    }
}
