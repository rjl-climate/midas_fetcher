//! Worker statistics calculation and reporting
//!
//! This module provides sophisticated statistics collection and calculation for
//! download workers, including speed calculations, progress aggregation, and
//! performance reporting.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::config::WorkerConfig;
#[allow(unused_imports)] // Used in tests
use super::types::{WorkerPoolStats, WorkerProgress, WorkerStatus};

/// Statistics aggregator for worker pools
#[derive(Debug)]
pub struct StatsAggregator {
    #[allow(dead_code)]
    config: WorkerConfig,
    workers: HashMap<u32, WorkerProgress>,
    pool_start_time: Instant,
    last_update: Instant,
}

impl StatsAggregator {
    /// Create a new statistics aggregator
    pub fn new(config: WorkerConfig) -> Self {
        let now = Instant::now();
        Self {
            config,
            workers: HashMap::new(),
            pool_start_time: now,
            last_update: now,
        }
    }

    /// Update statistics with a worker progress report
    pub fn update_worker_progress(&mut self, progress: WorkerProgress) {
        self.workers.insert(progress.worker_id, progress);
        self.last_update = Instant::now();
    }

    /// Generate aggregate statistics for the worker pool
    pub fn generate_pool_stats(&self) -> WorkerPoolStats {
        let mut stats = WorkerPoolStats {
            active_workers: self.workers.len(),
            ..Default::default()
        };

        // Aggregate individual worker statistics
        let mut total_speed = 0.0;
        let mut active_workers = 0;

        for progress in self.workers.values() {
            stats.files_completed += progress.files_completed;
            stats.total_bytes_downloaded += progress.total_bytes_downloaded;

            if progress.download_speed > 0.0 {
                total_speed += progress.download_speed;
                active_workers += 1;
            }

            // Count workers by status
            *stats
                .workers_by_status
                .entry(progress.status.clone())
                .or_insert(0) += 1;
        }

        // Calculate average download speed
        if active_workers > 0 {
            stats.average_download_speed = total_speed / active_workers as f64;
        }

        // Estimate completion time (simplified)
        stats.estimated_completion_time = self.estimate_completion_time(&stats);

        stats
    }

    /// Estimate time remaining for completion
    fn estimate_completion_time(&self, stats: &WorkerPoolStats) -> Option<Duration> {
        if stats.average_download_speed <= 0.0 {
            return None;
        }

        let working_workers = stats
            .workers_by_status
            .iter()
            .filter(|(status, _)| status.is_working())
            .map(|(_, count)| count)
            .sum::<usize>();

        if working_workers == 0 {
            return None;
        }

        // This is a simplified estimation - in reality, you'd need queue information
        // to know how much work remains
        let estimated_remaining_bytes = self.estimate_remaining_work();
        if estimated_remaining_bytes == 0 {
            return Some(Duration::ZERO);
        }

        let seconds = estimated_remaining_bytes as f64 / stats.average_download_speed;
        Some(Duration::from_secs_f64(seconds))
    }

    /// Estimate remaining work (placeholder - would need queue integration)
    fn estimate_remaining_work(&self) -> u64 {
        // Placeholder implementation - in reality, this would query the queue
        // for remaining work and estimate based on average file sizes
        0
    }

    /// Get the duration since pool started
    pub fn uptime(&self) -> Duration {
        self.pool_start_time.elapsed()
    }

    /// Get the duration since last update
    pub fn time_since_last_update(&self) -> Duration {
        self.last_update.elapsed()
    }

    /// Check if the pool appears to be stalled
    pub fn is_stalled(&self) -> bool {
        let max_stall_time = Duration::from_secs(300); // 5 minutes
        self.time_since_last_update() > max_stall_time
    }
}

/// Speed calculation utilities
pub struct SpeedCalculator;

impl SpeedCalculator {
    /// Calculate download speed from speed samples
    ///
    /// Uses a configurable number of recent samples for smoothing and handles
    /// edge cases like insufficient data or zero time differences.
    pub fn calculate_speed(speed_samples: &[(Instant, u64)], max_samples: usize) -> f64 {
        if speed_samples.len() < 2 {
            return 0.0;
        }

        let recent_samples: Vec<_> = speed_samples.iter().rev().take(max_samples).collect();

        if recent_samples.len() < 2 {
            return 0.0;
        }

        let (latest_time, latest_bytes) = recent_samples[0];
        let (earliest_time, earliest_bytes) = recent_samples[recent_samples.len() - 1];

        let time_diff = latest_time.duration_since(*earliest_time).as_secs_f64();
        let bytes_diff = latest_bytes.saturating_sub(*earliest_bytes) as f64;

        if time_diff > 0.0 {
            bytes_diff / time_diff
        } else {
            0.0
        }
    }

    /// Calculate exponentially weighted moving average for smoother speed reporting
    pub fn calculate_ewma_speed(current_speed: f64, previous_ewma: f64, alpha: f64) -> f64 {
        if previous_ewma == 0.0 {
            current_speed
        } else {
            alpha * current_speed + (1.0 - alpha) * previous_ewma
        }
    }

    /// Estimate ETA based on current progress and speed
    pub fn calculate_eta(
        bytes_downloaded: u64,
        total_bytes: Option<u64>,
        current_speed: f64,
    ) -> Option<f64> {
        if let Some(total) = total_bytes {
            if total <= bytes_downloaded || current_speed <= 0.0 {
                return Some(0.0);
            }

            let remaining_bytes = total - bytes_downloaded;
            Some(remaining_bytes as f64 / current_speed)
        } else {
            None
        }
    }
}

/// Backoff calculation utilities
pub struct BackoffCalculator;

impl BackoffCalculator {
    /// Calculate exponential backoff with jitter
    ///
    /// Implements the backoff algorithm used by workers when no work is available,
    /// using configurable parameters to avoid hardcoded values.
    pub fn calculate_backoff_duration(
        consecutive_empty_polls: u32,
        base_duration: Duration,
        max_multiplier: u32,
        max_duration_ms: u64,
        jitter_percentage: f64,
    ) -> Duration {
        // Exponential backoff: start with base duration, double up to max multiplier
        let backoff_multiplier = std::cmp::min(consecutive_empty_polls, max_multiplier);
        let base_millis = base_duration.as_millis() as u64;
        let exponential_sleep = base_millis * (1u64 << backoff_multiplier); // 2^n backoff
        let capped_sleep = std::cmp::min(exponential_sleep, max_duration_ms);

        // Add jitter to prevent thundering herd
        let jitter_range = (capped_sleep as f64 * jitter_percentage) as u64;
        let jitter = if jitter_range > 0 {
            fastrand::u64(0..=jitter_range * 2).saturating_sub(jitter_range)
        } else {
            0
        };

        let final_sleep = capped_sleep.saturating_add(jitter);
        Duration::from_millis(final_sleep)
    }

    /// Calculate retry delay with exponential backoff
    pub fn calculate_retry_delay(
        retry_count: u32,
        base_delay: Duration,
        max_delay: Duration,
        multiplier: u32,
    ) -> Duration {
        let delay_millis = base_delay.as_millis() as u64;
        let multiplied_delay = delay_millis * (multiplier as u64).pow(retry_count);
        let capped_delay = std::cmp::min(multiplied_delay, max_delay.as_millis() as u64);
        Duration::from_millis(capped_delay)
    }
}

/// Report generator for worker statistics
pub struct StatsReporter;

impl StatsReporter {
    /// Generate a detailed progress report
    pub fn generate_detailed_report(stats: &WorkerPoolStats) -> String {
        let mut report = String::new();

        report.push_str("Worker Pool Statistics\n");
        report.push_str("=====================\n");
        report.push_str(&format!("Active Workers: {}\n", stats.active_workers));
        report.push_str(&format!("Files Completed: {}\n", stats.files_completed));
        report.push_str(&format!("Files Failed: {}\n", stats.files_failed));
        report.push_str(&format!(
            "Total Downloaded: {} bytes\n",
            stats.total_bytes_downloaded
        ));
        report.push_str(&format!(
            "Average Speed: {}\n",
            stats.throughput_description()
        ));
        report.push_str(&format!("Success Rate: {:.1}%\n", stats.success_rate()));
        report.push_str(&format!(
            "Worker Utilization: {:.1}%\n",
            stats.worker_utilization()
        ));

        if let Some(eta) = stats.estimated_completion_time {
            report.push_str(&format!("Estimated Completion: {:?}\n", eta));
        }

        report.push_str("\nWorker Status Distribution:\n");
        for (status, count) in &stats.workers_by_status {
            report.push_str(&format!("  {:?}: {}\n", status, count));
        }

        report
    }

    /// Generate a compact status line for monitoring
    pub fn generate_compact_status(stats: &WorkerPoolStats) -> String {
        format!(
            "{} workers | {} completed | {} failed | {} | {:.1}% success | {:.1}% util",
            stats.active_workers,
            stats.files_completed,
            stats.files_failed,
            stats.throughput_description(),
            stats.success_rate(),
            stats.worker_utilization()
        )
    }

    /// Generate a JSON report for structured logging
    pub fn generate_json_report(stats: &WorkerPoolStats) -> Result<String, serde_json::Error> {
        serde_json::to_string(stats)
    }
}

/// Performance metrics for advanced monitoring
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub throughput_mbps: f64,
    pub files_per_second: f64,
    pub average_file_size: f64,
    pub error_rate_percentage: f64,
    pub worker_efficiency: f64,
    pub cache_hit_rate: Option<f64>,
    pub uptime_secs: f64,
}

impl PerformanceMetrics {
    /// Calculate performance metrics from pool statistics
    pub fn from_pool_stats(stats: &WorkerPoolStats, uptime: Duration) -> Self {
        let uptime_secs = uptime.as_secs_f64();

        let throughput_mbps = if uptime_secs > 0.0 {
            (stats.total_bytes_downloaded as f64 / 1_000_000.0) / uptime_secs
        } else {
            0.0
        };

        let files_per_second = if uptime_secs > 0.0 {
            stats.files_completed as f64 / uptime_secs
        } else {
            0.0
        };

        let average_file_size = if stats.files_completed > 0 {
            stats.total_bytes_downloaded as f64 / stats.files_completed as f64
        } else {
            0.0
        };

        let total_files = stats.files_completed + stats.files_failed;
        let error_rate_percentage = if total_files > 0 {
            (stats.files_failed as f64 / total_files as f64) * 100.0
        } else {
            0.0
        };

        let worker_efficiency = stats.worker_utilization();

        Self {
            throughput_mbps,
            files_per_second,
            average_file_size,
            error_rate_percentage,
            worker_efficiency,
            cache_hit_rate: None, // Would need cache statistics
            uptime_secs,
        }
    }

    /// Check if performance is within acceptable thresholds
    pub fn is_healthy(&self) -> bool {
        self.error_rate_percentage < 10.0
            && self.worker_efficiency > 50.0
            && self.throughput_mbps > 0.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test speed calculation with various sample sets
    ///
    /// Verifies that speed calculation correctly handles different
    /// scenarios including insufficient data and zero time differences.
    #[test]
    fn test_speed_calculation() {
        // Test with insufficient samples
        let samples = vec![(Instant::now(), 1024)];
        assert_eq!(SpeedCalculator::calculate_speed(&samples, 10), 0.0);

        // Test with valid samples
        let now = Instant::now();
        let samples = vec![
            (now, 0),
            (now + Duration::from_secs(1), 1024),
            (now + Duration::from_secs(2), 2048),
        ];
        let speed = SpeedCalculator::calculate_speed(&samples, 10);
        assert!(speed > 0.0);
        assert!(speed <= 2048.0); // Should be around 1024 bytes/sec

        // Test ETA calculation
        let eta = SpeedCalculator::calculate_eta(1024, Some(2048), 1024.0);
        assert_eq!(eta, Some(1.0)); // 1 second remaining

        // Test ETA with no total size
        let eta = SpeedCalculator::calculate_eta(1024, None, 1024.0);
        assert_eq!(eta, None);
    }

    /// Test backoff calculation with different parameters
    ///
    /// Ensures backoff calculations produce expected durations and include
    /// appropriate jitter to prevent thundering herd problems.
    #[test]
    fn test_backoff_calculation() {
        let base = Duration::from_millis(100);

        // Test first backoff (no multiplication)
        let backoff = BackoffCalculator::calculate_backoff_duration(0, base, 6, 2000, 0.0);
        assert_eq!(backoff, base);

        // Test exponential growth
        let backoff1 = BackoffCalculator::calculate_backoff_duration(1, base, 6, 2000, 0.0);
        let backoff2 = BackoffCalculator::calculate_backoff_duration(2, base, 6, 2000, 0.0);
        assert!(backoff2 > backoff1);

        // Test max cap
        let backoff_max = BackoffCalculator::calculate_backoff_duration(10, base, 6, 2000, 0.0);
        assert!(backoff_max.as_millis() <= 2000);

        // Test retry delay calculation
        let retry_delay = BackoffCalculator::calculate_retry_delay(
            2,
            Duration::from_millis(100),
            Duration::from_secs(30),
            2,
        );
        assert_eq!(retry_delay, Duration::from_millis(400)); // 100 * 2^2
    }

    /// Test statistics aggregation and pool stats generation
    ///
    /// Verifies that statistics aggregator correctly combines individual
    /// worker progress reports into meaningful pool-level metrics.
    #[test]
    fn test_stats_aggregation() {
        let config = super::super::config::WorkerConfig::default();
        let mut aggregator = StatsAggregator::new(config);

        // Add worker progress reports
        let progress1 = WorkerProgress {
            worker_id: 1,
            status: WorkerStatus::Downloading,
            files_completed: 5,
            total_bytes_downloaded: 10240,
            download_speed: 1024.0,
            ..WorkerProgress::new(1, WorkerStatus::Downloading)
        };

        let progress2 = WorkerProgress {
            worker_id: 2,
            status: WorkerStatus::Idle,
            files_completed: 3,
            total_bytes_downloaded: 6144,
            download_speed: 0.0,
            ..WorkerProgress::new(2, WorkerStatus::Idle)
        };

        aggregator.update_worker_progress(progress1);
        aggregator.update_worker_progress(progress2);

        let stats = aggregator.generate_pool_stats();
        assert_eq!(stats.active_workers, 2);
        assert_eq!(stats.files_completed, 8); // 5 + 3
        assert_eq!(stats.total_bytes_downloaded, 16384); // 10240 + 6144
        assert_eq!(stats.average_download_speed, 1024.0); // Only counting active workers
        assert_eq!(
            stats.workers_by_status.get(&WorkerStatus::Downloading),
            Some(&1)
        );
        assert_eq!(stats.workers_by_status.get(&WorkerStatus::Idle), Some(&1));
    }

    /// Test performance metrics calculation
    ///
    /// Ensures performance metrics are correctly derived from pool statistics
    /// and provide meaningful indicators of system health.
    #[test]
    fn test_performance_metrics() {
        let mut stats = WorkerPoolStats {
            files_completed: 100,
            files_failed: 10,
            total_bytes_downloaded: 20_000_000, // 20MB for better throughput
            active_workers: 4,
            ..Default::default()
        };
        stats.workers_by_status.insert(WorkerStatus::Downloading, 3);
        stats.workers_by_status.insert(WorkerStatus::Idle, 1);

        let uptime = Duration::from_secs(100);
        let metrics = PerformanceMetrics::from_pool_stats(&stats, uptime);

        assert_eq!(metrics.throughput_mbps, 0.2); // 20MB in 100s = 0.2 MB/s
        assert_eq!(metrics.files_per_second, 1.0); // 100 files in 100s
        assert_eq!(metrics.average_file_size, 200_000.0); // 20MB / 100 files
        assert!((metrics.error_rate_percentage - 9.09).abs() < 0.1); // 10/110 * 100
        assert_eq!(metrics.worker_efficiency, 75.0); // 3/4 * 100

        // Test health check
        assert!(metrics.is_healthy()); // Error rate < 10%, efficiency > 50%, throughput > 0.1
    }

    /// Test report generation formats
    ///
    /// Verifies that different report formats (detailed, compact, JSON)
    /// produce correct output and handle edge cases properly.
    #[test]
    fn test_report_generation() {
        let mut stats = WorkerPoolStats {
            active_workers: 4,
            files_completed: 50,
            files_failed: 5,
            total_bytes_downloaded: 500_000,
            average_download_speed: 1024.0,
            ..Default::default()
        };
        stats.workers_by_status.insert(WorkerStatus::Downloading, 2);
        stats.workers_by_status.insert(WorkerStatus::Idle, 2);

        // Test detailed report
        let detailed = StatsReporter::generate_detailed_report(&stats);
        assert!(detailed.contains("Active Workers: 4"));
        assert!(detailed.contains("Files Completed: 50"));
        assert!(detailed.contains("Success Rate:"));

        // Test compact status
        let compact = StatsReporter::generate_compact_status(&stats);
        assert!(compact.contains("4 workers"));
        assert!(compact.contains("50 completed"));
        assert!(compact.contains("5 failed"));

        // Test JSON report
        let json = StatsReporter::generate_json_report(&stats);
        assert!(json.is_ok());
        let json_str = json.unwrap();
        assert!(json_str.contains("active_workers"));
        assert!(json_str.contains("files_completed"));
    }

    /// Test EWMA speed calculation for smoothing
    ///
    /// Ensures exponentially weighted moving average provides appropriate
    /// smoothing for variable download speeds.
    #[test]
    fn test_ewma_speed_calculation() {
        // Test initial value
        let ewma = SpeedCalculator::calculate_ewma_speed(1000.0, 0.0, 0.1);
        assert_eq!(ewma, 1000.0);

        // Test smoothing
        let ewma = SpeedCalculator::calculate_ewma_speed(2000.0, 1000.0, 0.1);
        assert_eq!(ewma, 1100.0); // 0.1 * 2000 + 0.9 * 1000

        // Test with different alpha
        let ewma = SpeedCalculator::calculate_ewma_speed(2000.0, 1000.0, 0.5);
        assert_eq!(ewma, 1500.0); // 0.5 * 2000 + 0.5 * 1000
    }
}
