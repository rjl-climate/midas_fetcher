//! Download statistics tracking and aggregation
//!
//! This module handles the collection, calculation, and reporting of download
//! statistics including progress metrics, rates, and estimated completion times.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Aggregated download statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadStats {
    /// Total files to download
    pub total_files: usize,
    /// Files successfully completed
    pub files_completed: usize,
    /// Files that failed
    pub files_failed: usize,
    /// Files currently in progress
    pub files_in_progress: usize,
    /// Total bytes downloaded
    pub total_bytes_downloaded: u64,
    /// Current download rate (bytes per second)
    pub download_rate_bps: f64,
    /// Estimated time to completion
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Number of active workers
    pub active_workers: usize,
    /// Start time of download session
    pub session_start: DateTime<Utc>,
    /// Current session duration
    pub session_duration: Duration,
}

impl Default for DownloadStats {
    fn default() -> Self {
        Self {
            total_files: 0,
            files_completed: 0,
            files_failed: 0,
            files_in_progress: 0,
            total_bytes_downloaded: 0,
            download_rate_bps: 0.0,
            estimated_completion: None,
            active_workers: 0,
            session_start: Utc::now(),
            session_duration: Duration::ZERO,
        }
    }
}

/// Final result of a download session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResult {
    /// Final download statistics
    pub stats: DownloadStats,
    /// Whether the session completed successfully
    pub success: bool,
    /// Any errors that occurred during shutdown
    pub shutdown_errors: Vec<String>,
    /// Time taken for the entire session
    pub total_duration: Duration,
}

impl DownloadStats {
    /// Create new statistics with expected file count
    pub fn new_with_expected_files(expected_files: usize) -> Self {
        Self {
            total_files: expected_files,
            ..Default::default()
        }
    }

    /// Calculate completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_files == 0 {
            return 0.0;
        }
        (self.files_completed as f64 / self.total_files as f64) * 100.0
    }

    /// Calculate total processed files (completed + failed)
    pub fn total_processed(&self) -> usize {
        self.files_completed + self.files_failed
    }

    /// Check if all files are processed
    pub fn is_complete(&self) -> bool {
        self.total_files > 0 && self.total_processed() >= self.total_files
    }

    /// Calculate ETA based on current progress rate
    pub fn calculate_eta(&mut self) {
        if self.download_rate_bps > 0.0 {
            let remaining_files = self.total_files.saturating_sub(self.files_completed);
            if remaining_files > 0 {
                let avg_file_size = if self.files_completed > 0 {
                    self.total_bytes_downloaded as f64 / self.files_completed as f64
                } else {
                    1024.0 // Default estimate
                };

                let remaining_bytes = remaining_files as f64 * avg_file_size;
                let eta_seconds = remaining_bytes / self.download_rate_bps;

                self.estimated_completion = Some(
                    Utc::now() + chrono::Duration::seconds(eta_seconds as i64)
                );
            } else {
                self.estimated_completion = Some(Utc::now());
            }
        }
    }

    /// Update session duration from start time
    pub fn update_duration(&mut self) {
        self.session_duration = Utc::now()
            .signed_duration_since(self.session_start)
            .to_std()
            .unwrap_or(Duration::ZERO);
    }

    /// Format download rate as human-readable string
    pub fn format_download_rate(&self) -> String {
        if self.download_rate_bps < 1024.0 {
            format!("{:.1} B/s", self.download_rate_bps)
        } else if self.download_rate_bps < 1024.0 * 1024.0 {
            format!("{:.1} KB/s", self.download_rate_bps / 1024.0)
        } else {
            format!("{:.1} MB/s", self.download_rate_bps / (1024.0 * 1024.0))
        }
    }

    /// Format ETA as human-readable string
    pub fn format_eta(&self) -> String {
        match self.estimated_completion {
            Some(eta) => {
                let duration = eta.signed_duration_since(Utc::now());
                if let Ok(std_duration) = duration.to_std() {
                    format_duration(std_duration)
                } else {
                    "Complete".to_string()
                }
            }
            None => "Unknown".to_string(),
        }
    }
}

impl SessionResult {
    /// Create a successful session result
    pub fn success(stats: DownloadStats, total_duration: Duration) -> Self {
        Self {
            stats,
            success: true,
            shutdown_errors: Vec::new(),
            total_duration,
        }
    }

    /// Create a failed session result
    pub fn failed(stats: DownloadStats, total_duration: Duration, errors: Vec<String>) -> Self {
        Self {
            stats,
            success: false,
            shutdown_errors: errors,
            total_duration,
        }
    }

    /// Check if the session had any errors
    pub fn has_errors(&self) -> bool {
        !self.shutdown_errors.is_empty()
    }

    /// Get a summary of the session result
    pub fn summary(&self) -> String {
        if self.success && !self.has_errors() {
            format!(
                "Session completed successfully: {} files in {:?}",
                self.stats.files_completed, self.total_duration
            )
        } else if self.success {
            format!(
                "Session completed with warnings: {} files in {:?}, {} warnings",
                self.stats.files_completed,
                self.total_duration,
                self.shutdown_errors.len()
            )
        } else {
            format!(
                "Session failed: {} files in {:?}, {} errors",
                self.stats.files_completed,
                self.total_duration,
                self.shutdown_errors.len()
            )
        }
    }
}

/// Format a duration as human-readable string
fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        format!("{}m{}s", total_secs / 60, total_secs % 60)
    } else {
        format!("{}h{}m", total_secs / 3600, (total_secs % 3600) / 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test statistics calculation methods
    ///
    /// Verifies that completion percentage, total processed files,
    /// and completion status are calculated correctly.
    #[test]
    fn test_stats_calculations() {
        let mut stats = DownloadStats::new_with_expected_files(100);
        stats.files_completed = 60;
        stats.files_failed = 10;

        assert_eq!(stats.completion_percentage(), 60.0);
        assert_eq!(stats.total_processed(), 70);
        assert!(!stats.is_complete());

        stats.files_completed = 90;
        assert!(stats.is_complete());
    }

    /// Test ETA calculation
    ///
    /// Ensures that estimated time to completion is calculated
    /// correctly based on current download rate and remaining files.
    #[test]
    fn test_eta_calculation() {
        let mut stats = DownloadStats::new_with_expected_files(100);
        stats.files_completed = 50;
        stats.total_bytes_downloaded = 50 * 1024; // 50 files, 1KB each
        stats.download_rate_bps = 1024.0; // 1KB/s

        stats.calculate_eta();
        assert!(stats.estimated_completion.is_some());
    }

    /// Test download rate formatting
    ///
    /// Verifies that download rates are formatted in human-readable
    /// units (B/s, KB/s, MB/s) with appropriate precision.
    #[test]
    fn test_download_rate_formatting() {
        let mut stats = DownloadStats::default();
        
        stats.download_rate_bps = 512.0;
        assert_eq!(stats.format_download_rate(), "512.0 B/s");
        
        stats.download_rate_bps = 1536.0; // 1.5 KB/s
        assert_eq!(stats.format_download_rate(), "1.5 KB/s");
        
        stats.download_rate_bps = 2.5 * 1024.0 * 1024.0; // 2.5 MB/s
        assert_eq!(stats.format_download_rate(), "2.5 MB/s");
    }

    /// Test duration formatting
    ///
    /// Ensures that durations are formatted in a human-readable
    /// format with appropriate units.
    #[test]
    fn test_duration_formatting() {
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h1m");
    }

    /// Test session result creation
    ///
    /// Verifies that session results are created correctly with
    /// appropriate success/failure status and error handling.
    #[test]
    fn test_session_result_creation() {
        let stats = DownloadStats::default();
        let duration = Duration::from_secs(60);

        let success_result = SessionResult::success(stats.clone(), duration);
        assert!(success_result.success);
        assert!(!success_result.has_errors());

        let errors = vec!["Test error".to_string()];
        let failed_result = SessionResult::failed(stats, duration, errors);
        assert!(!failed_result.success);
        assert!(failed_result.has_errors());
    }

    /// Test session result summary
    ///
    /// Ensures that session summaries provide meaningful information
    /// about the download session outcome.
    #[test]
    fn test_session_summary() {
        let stats = DownloadStats::default();
        let duration = Duration::from_secs(60);

        let success_result = SessionResult::success(stats.clone(), duration);
        let summary = success_result.summary();
        assert!(summary.contains("completed successfully"));

        let errors = vec!["Warning".to_string()];
        let warning_result = SessionResult::success(stats.clone(), duration);
        let mut warning_result = warning_result;
        warning_result.shutdown_errors = errors;
        let summary = warning_result.summary();
        assert!(summary.contains("warnings") || summary.contains("completed successfully"));
    }
}