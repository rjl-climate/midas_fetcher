//! Progress monitoring and statistics aggregation
//!
//! This module handles real-time progress tracking, download rate calculations,
//! and statistics updates for the download coordinator.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::app::queue::WorkQueue;
use crate::app::worker::WorkerProgress;
use crate::constants::coordinator;

use super::stats::DownloadStats;

/// Progress monitor for tracking download progress and calculating statistics
pub struct ProgressMonitor {
    stats: Arc<RwLock<DownloadStats>>,
    queue: Arc<WorkQueue>,
    update_interval: Duration,
    verbose: bool,
}

impl ProgressMonitor {
    /// Create a new progress monitor
    pub fn new(
        stats: Arc<RwLock<DownloadStats>>,
        queue: Arc<WorkQueue>,
        update_interval: Duration,
        verbose: bool,
    ) -> Self {
        Self {
            stats,
            queue,
            update_interval,
            verbose,
        }
    }

    /// Start progress monitoring task
    ///
    /// Returns a handle to the background task that monitors worker progress
    /// and updates statistics in real-time.
    pub fn start_monitoring(
        self,
        mut progress_rx: mpsc::Receiver<WorkerProgress>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut last_update = Instant::now();
            let mut bytes_window = Vec::new();

            loop {
                tokio::select! {
                    progress = progress_rx.recv() => {
                        match progress {
                            Some(update) => {
                                if self.verbose {
                                    debug!("Worker {} progress: {:?} status, {} files completed",
                                           update.worker_id, update.status, update.files_completed);
                                }

                                self.process_worker_update(update, &mut bytes_window).await;
                            }
                            None => {
                                debug!("Progress channel closed");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Progress monitor received shutdown signal");
                        break;
                    }
                    _ = tokio::time::sleep(self.update_interval) => {
                        self.update_periodic_stats(&mut last_update).await;
                    }
                }
            }
        })
    }

    /// Process a worker progress update
    async fn process_worker_update(
        &self,
        update: WorkerProgress,
        bytes_window: &mut Vec<(Instant, u64)>,
    ) {
        let mut stats_guard = self.stats.write().await;

        // Track bytes for rate calculation
        bytes_window.push((Instant::now(), update.bytes_downloaded));
        if bytes_window.len() > coordinator::RATE_CALCULATION_WINDOW {
            bytes_window.remove(0);
        }

        // Calculate download rate
        if bytes_window.len() >= 2 {
            let oldest = &bytes_window[0];
            let newest = &bytes_window[bytes_window.len() - 1];
            let time_diff = newest.0.duration_since(oldest.0).as_secs_f64();
            let bytes_diff = newest.1.saturating_sub(oldest.1);

            if time_diff > 0.0 {
                stats_guard.download_rate_bps = bytes_diff as f64 / time_diff;
            }
        }

        stats_guard.total_bytes_downloaded = update.total_bytes_downloaded;
    }

    /// Update periodic statistics from queue state
    async fn update_periodic_stats(&self, last_update: &mut Instant) {
        if last_update.elapsed() >= self.update_interval {
            // Use cached stats when available to reduce queue lock pressure
            let queue_stats = self.queue.stats().await;
            let mut stats_guard = self.stats.write().await;

            stats_guard.files_completed = queue_stats.completed_count as usize;
            stats_guard.files_failed = queue_stats.failed_count as usize;
            stats_guard.files_in_progress = queue_stats.in_progress_count as usize;
            stats_guard.update_duration();

            // Calculate ETA if we have a positive download rate
            stats_guard.calculate_eta();

            *last_update = Instant::now();
        }
    }
}

/// Rate calculator for download speed tracking
pub struct RateCalculator {
    window: Vec<(Instant, u64)>,
    window_size: usize,
}

impl RateCalculator {
    /// Create a new rate calculator with the specified window size
    pub fn new(window_size: usize) -> Self {
        Self {
            window: Vec::new(),
            window_size,
        }
    }

    /// Add a new data point and calculate the current rate
    pub fn add_sample(&mut self, bytes: u64) -> f64 {
        let now = Instant::now();
        self.window.push((now, bytes));

        // Keep only the most recent samples
        if self.window.len() > self.window_size {
            self.window.remove(0);
        }

        self.calculate_rate()
    }

    /// Calculate the current rate in bytes per second
    pub fn calculate_rate(&self) -> f64 {
        if self.window.len() < 2 {
            return 0.0;
        }

        let oldest = &self.window[0];
        let newest = &self.window[self.window.len() - 1];
        let time_diff = newest.0.duration_since(oldest.0).as_secs_f64();
        let bytes_diff = newest.1.saturating_sub(oldest.1);

        if time_diff > 0.0 {
            bytes_diff as f64 / time_diff
        } else {
            0.0
        }
    }

    /// Get the number of samples in the window
    pub fn sample_count(&self) -> usize {
        self.window.len()
    }

    /// Clear all samples
    pub fn clear(&mut self) {
        self.window.clear();
    }
}

/// Progress aggregator for combining multiple worker progress updates
pub struct ProgressAggregator {
    total_files_completed: u64,
    total_bytes_downloaded: u64,
    active_workers: usize,
    last_update: Instant,
}

impl ProgressAggregator {
    /// Create a new progress aggregator
    pub fn new() -> Self {
        Self {
            total_files_completed: 0,
            total_bytes_downloaded: 0,
            active_workers: 0,
            last_update: Instant::now(),
        }
    }

    /// Update with worker progress
    pub fn update_worker_progress(&mut self, _worker_id: usize, progress: &WorkerProgress) {
        // For simplicity, we assume worker progress represents cumulative totals
        // In a real implementation, this might need more sophisticated tracking
        self.total_files_completed = self.total_files_completed.max(progress.files_completed);
        self.total_bytes_downloaded = self.total_bytes_downloaded.max(progress.total_bytes_downloaded);
        self.last_update = Instant::now();
    }

    /// Set the number of active workers
    pub fn set_active_workers(&mut self, count: usize) {
        self.active_workers = count;
    }

    /// Get aggregated statistics
    pub fn get_aggregated_stats(&self) -> (u64, u64, usize, Instant) {
        (
            self.total_files_completed,
            self.total_bytes_downloaded,
            self.active_workers,
            self.last_update,
        )
    }

    /// Check if progress is stale
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.last_update.elapsed() > max_age
    }
}

impl Default for ProgressAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::worker::WorkerStatus;
    use chrono;
    use crate::app::WorkQueue;

    /// Test rate calculator functionality
    ///
    /// Verifies that download rates are calculated correctly
    /// over a rolling window of samples.
    #[test]
    fn test_rate_calculator() {
        let mut calculator = RateCalculator::new(3);
        
        // No samples should return 0 rate
        assert_eq!(calculator.calculate_rate(), 0.0);
        assert_eq!(calculator.sample_count(), 0);
        
        // Add samples (simulating 1KB/s over 3 seconds)
        let _rate1 = calculator.add_sample(1024);
        assert_eq!(calculator.sample_count(), 1);
        
        std::thread::sleep(std::time::Duration::from_millis(10));
        let rate2 = calculator.add_sample(2048);
        assert!(rate2 > 0.0);
        assert_eq!(calculator.sample_count(), 2);
        
        // Clear and verify
        calculator.clear();
        assert_eq!(calculator.sample_count(), 0);
    }

    /// Test progress aggregator
    ///
    /// Ensures that progress from multiple workers is
    /// correctly aggregated and tracked.
    #[test]
    fn test_progress_aggregator() {
        let mut aggregator = ProgressAggregator::new();
        
        let progress = WorkerProgress {
            worker_id: 1,
            file_info: None,
            bytes_downloaded: 1024,
            total_bytes: Some(2048),
            download_speed: 1024.0,
            eta_seconds: Some(1.0),
            status: WorkerStatus::Downloading,
            timestamp: chrono::Utc::now(),
            files_completed: 10,
            total_bytes_downloaded: 10240,
            error_message: None,
        };
        
        aggregator.update_worker_progress(1, &progress);
        aggregator.set_active_workers(4);
        
        let (files, bytes, workers, _) = aggregator.get_aggregated_stats();
        assert_eq!(files, 10);
        assert_eq!(bytes, 10240);
        assert_eq!(workers, 4);
        
        // Test staleness
        assert!(!aggregator.is_stale(Duration::from_secs(1)));
    }

    /// Test progress monitor creation
    ///
    /// Verifies that progress monitors can be created with
    /// valid configuration and dependencies.
    #[tokio::test]
    async fn test_progress_monitor_creation() {
        let stats = Arc::new(RwLock::new(DownloadStats::default()));
        let queue = Arc::new(WorkQueue::new());
        let update_interval = Duration::from_millis(100);
        
        let monitor = ProgressMonitor::new(
            stats.clone(),
            queue,
            update_interval,
            false
        );
        
        // Create channels for testing
        let (_progress_tx, progress_rx) = mpsc::channel(10);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        // Start monitoring (should not panic)
        let handle = monitor.start_monitoring(progress_rx, shutdown_rx);
        
        // Send shutdown signal
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
        
        // Wait for shutdown
        let _ = handle.await;
    }

    /// Test worker progress processing
    ///
    /// Ensures that worker progress updates are correctly
    /// processed and reflected in download statistics.
    #[tokio::test]
    async fn test_worker_progress_processing() {
        let stats = Arc::new(RwLock::new(DownloadStats::default()));
        let queue = Arc::new(WorkQueue::new());
        let update_interval = Duration::from_millis(100);
        
        let monitor = ProgressMonitor::new(
            stats.clone(),
            queue,
            update_interval,
            true // verbose mode
        );
        
        let progress = WorkerProgress {
            worker_id: 1,
            file_info: None,
            bytes_downloaded: 512,
            total_bytes: Some(1024),
            download_speed: 512.0,
            eta_seconds: Some(1.0),
            status: WorkerStatus::Downloading,
            timestamp: chrono::Utc::now(),
            files_completed: 5,
            total_bytes_downloaded: 5120,
            error_message: None,
        };
        
        let mut bytes_window = Vec::new();
        monitor.process_worker_update(progress, &mut bytes_window).await;
        
        // Verify stats were updated
        let stats_guard = stats.read().await;
        assert_eq!(stats_guard.total_bytes_downloaded, 5120);
        assert_eq!(bytes_window.len(), 1);
    }

    /// Test rate calculation window management
    ///
    /// Verifies that the rate calculation window is properly
    /// managed and doesn't grow beyond the specified size.
    #[test]
    fn test_rate_window_management() {
        let mut calculator = RateCalculator::new(3);
        
        // Add more samples than window size
        for i in 0..5 {
            calculator.add_sample((i + 1) * 1024);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        
        // Window should be limited to specified size
        assert_eq!(calculator.sample_count(), 3);
        
        // Rate should be calculated from the most recent samples
        let rate = calculator.calculate_rate();
        assert!(rate > 0.0);
    }
}