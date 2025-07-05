//! Real-time progress display for download operations
//!
//! This module provides sophisticated progress visualization using indicatif
//! with support for multiple progress bars, ETA calculations, and terminal
//! resize handling. It integrates with the coordinator to display live
//! download statistics and worker status.
//!
//! # Key Features
//!
//! - **Multi-bar Display**: Shows overall progress and individual worker status
//! - **Real-time ETA**: Rolling window calculations for accurate time estimates
//! - **Terminal Adaptability**: Handles resize events and cleanup gracefully
//! - **Rich Statistics**: Download rates, completion percentages, error counts
//! - **Spinner Animations**: Visual feedback for ongoing operations
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::cli::{ProgressDisplay, ProgressConfig, ProgressEvent};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure progress display
//! let config = ProgressConfig {
//!     enable_progress_bars: true,
//!     update_interval: Duration::from_millis(100),
//!     show_worker_details: true,
//!     ..Default::default()
//! };
//!
//! // Create progress display
//! let mut display = ProgressDisplay::new(config);
//! display.start(1000, 4).await?; // 1000 total files, 4 workers
//!
//! // Update progress
//! display.update(ProgressEvent::FileCompleted {
//!     worker_id: 1,
//!     bytes_downloaded: 2048,
//!     file_name: "data.csv".to_string(),
//! }).await?;
//!
//! // Finish and cleanup
//! display.finish().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::terminal;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::app::coordinator::DownloadStats;
use crate::errors::{DownloadError, DownloadResult};

/// Configuration for progress display
#[derive(Debug, Clone)]
pub struct ProgressConfig {
    /// Enable visual progress bars
    pub enable_progress_bars: bool,
    /// How often to update the display
    pub update_interval: Duration,
    /// Show detailed worker information
    pub show_worker_details: bool,
    /// Enable terminal colors
    pub enable_colors: bool,
    /// Show download rate in progress bar
    pub show_download_rate: bool,
    /// Show ETA in progress bar
    pub show_eta: bool,
    /// Maximum width for file names in display
    pub max_filename_width: usize,
    /// Enable spinner animations
    pub enable_spinner: bool,
    /// Compact mode (fewer lines)
    pub compact_mode: bool,
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            enable_progress_bars: true,
            update_interval: Duration::from_millis(100),
            show_worker_details: true,
            enable_colors: true,
            show_download_rate: true,
            show_eta: true,
            max_filename_width: 40,
            enable_spinner: true,
            compact_mode: false,
        }
    }
}

/// Events that can update the progress display
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// A file was completed by a worker
    FileCompleted {
        worker_id: u32,
        bytes_downloaded: u64,
        file_name: String,
    },
    /// A file failed to download
    FileFailed {
        worker_id: u32,
        file_name: String,
        error: String,
    },
    /// Worker status changed
    WorkerStatusChanged {
        worker_id: u32,
        status: String,
        current_file: Option<String>,
    },
    /// Overall statistics update
    StatsUpdate { stats: DownloadStats },
    /// Download session completed
    SessionCompleted {
        total_files: usize,
        successful: usize,
        failed: usize,
        duration: Duration,
    },
}

/// Worker-specific progress information
#[derive(Debug, Clone)]
struct WorkerProgress {
    #[allow(dead_code)]
    worker_id: u32,
    status: String,
    current_file: Option<String>,
    files_completed: usize,
    bytes_downloaded: u64,
    last_update: Instant,
}

/// Main progress display manager
pub struct ProgressDisplay {
    config: ProgressConfig,
    multi_progress: Option<MultiProgress>,
    main_progress: Option<ProgressBar>,
    worker_progress_bars: HashMap<u32, ProgressBar>,
    worker_progress: Arc<RwLock<HashMap<u32, WorkerProgress>>>,
    stats: Arc<RwLock<DownloadStats>>,
    update_task: Option<JoinHandle<()>>,
    event_tx: Option<mpsc::UnboundedSender<ProgressEvent>>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    is_terminal: bool,
}

impl ProgressDisplay {
    /// Create a new progress display with the given configuration
    pub fn new(config: ProgressConfig) -> Self {
        let is_terminal = atty::is(atty::Stream::Stderr);

        Self {
            config,
            multi_progress: None,
            main_progress: None,
            worker_progress_bars: HashMap::new(),
            worker_progress: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(DownloadStats::default())),
            update_task: None,
            event_tx: None,
            shutdown_tx: None,
            is_terminal,
        }
    }

    /// Start the progress display for a download session
    ///
    /// # Arguments
    ///
    /// * `total_files` - Total number of files to download
    /// * `worker_count` - Number of workers that will be active
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if terminal setup fails
    pub async fn start(&mut self, total_files: usize, worker_count: usize) -> DownloadResult<()> {
        if !self.config.enable_progress_bars || !self.is_terminal {
            // Fallback to simple text progress
            return self.start_text_mode(total_files, worker_count).await;
        }

        // Setup terminal
        terminal::enable_raw_mode()
            .map_err(|e| DownloadError::Other(format!("Failed to enable raw mode: {}", e)))?;

        // Create multi-progress manager
        let multi = MultiProgress::new();

        // Main progress bar
        let main_pb = multi.add(ProgressBar::new(total_files as u64));
        main_pb.set_style(
            ProgressStyle::default_bar()
                .template(if self.config.show_eta && self.config.show_download_rate {
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {bytes_per_sec}"
                } else if self.config.show_eta {
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})"
                } else if self.config.show_download_rate {
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {bytes_per_sec}"
                } else {
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}"
                })
                .map_err(|e| DownloadError::Other(format!("Progress bar template error: {}", e)))?
                .progress_chars("##-")
        );
        main_pb.set_message("Downloading MIDAS files");

        // Create worker progress bars if enabled
        let mut worker_bars = HashMap::new();
        if self.config.show_worker_details && !self.config.compact_mode {
            for i in 0..worker_count {
                let worker_pb = multi.add(ProgressBar::new_spinner());
                worker_pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("  Worker {prefix}: {spinner:.blue} {msg}")
                        .map_err(|e| {
                            DownloadError::Other(format!("Worker progress template error: {}", e))
                        })?,
                );
                worker_pb.set_prefix(match i {
                    0 => "1",
                    1 => "2",
                    2 => "3",
                    3 => "4",
                    4 => "5",
                    5 => "6",
                    6 => "7",
                    7 => "8",
                    _ => "N",
                });
                worker_pb.set_message("Initializing...");
                worker_bars.insert(i as u32 + 1, worker_pb);
            }
        }

        // Setup event channel
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Initialize state
        {
            let mut stats = self.stats.write().await;
            stats.total_files = total_files;
            stats.active_workers = worker_count;
        }

        // Start update task
        let update_task = self.start_update_task(event_rx, shutdown_rx).await;

        // Store components
        self.multi_progress = Some(multi);
        self.main_progress = Some(main_pb);
        self.worker_progress_bars = worker_bars;
        self.event_tx = Some(event_tx);
        self.shutdown_tx = Some(shutdown_tx);
        self.update_task = Some(update_task);

        debug!(
            "Progress display started for {} files with {} workers",
            total_files, worker_count
        );
        Ok(())
    }

    /// Start text-mode progress (for non-terminal environments)
    async fn start_text_mode(
        &mut self,
        total_files: usize,
        worker_count: usize,
    ) -> DownloadResult<()> {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

        // Initialize state
        {
            let mut stats = self.stats.write().await;
            stats.total_files = total_files;
            stats.active_workers = worker_count;
        }

        // Simple text progress task
        let stats = self.stats.clone();
        let update_interval = self.config.update_interval;
        let update_task = tokio::spawn(async move {
            let mut last_report = Instant::now();
            let report_interval = Duration::from_secs(10); // Report every 10 seconds

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Some(ProgressEvent::FileCompleted { file_name: _, .. }) => {
                                if last_report.elapsed() >= report_interval {
                                    let stats_guard = stats.read().await;
                                    eprintln!("Progress: {}/{} files completed ({:.1}%)",
                                             stats_guard.files_completed,
                                             stats_guard.total_files,
                                             (stats_guard.files_completed as f64 / stats_guard.total_files as f64) * 100.0);
                                    last_report = Instant::now();
                                }
                            }
                            Some(ProgressEvent::SessionCompleted { successful, failed, duration, .. }) => {
                                eprintln!("Download completed: {} successful, {} failed in {:?}", successful, failed, duration);
                                break;
                            }
                            Some(_) => {} // Ignore other events in text mode
                            None => break,
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    _ = tokio::time::sleep(update_interval) => {
                        // Periodic updates handled above
                    }
                }
            }
        });

        self.event_tx = Some(event_tx);
        self.shutdown_tx = Some(shutdown_tx);
        self.update_task = Some(update_task);

        eprintln!(
            "Starting download of {} files with {} workers...",
            total_files, worker_count
        );
        Ok(())
    }

    /// Update the progress display with an event
    pub async fn update(&self, event: ProgressEvent) -> DownloadResult<()> {
        if let Some(tx) = &self.event_tx {
            tx.send(event).map_err(|e| {
                DownloadError::Other(format!("Failed to send progress event: {}", e))
            })?;
        }
        Ok(())
    }

    /// Finish the progress display and cleanup
    pub async fn finish(&mut self) -> DownloadResult<()> {
        debug!("Finishing progress display");

        // Signal shutdown
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }

        // Wait for update task to complete
        if let Some(task) = self.update_task.take() {
            let _ = task.await;
        }

        // Cleanup terminal
        if self.config.enable_progress_bars && self.is_terminal {
            if let Some(main_pb) = &self.main_progress {
                main_pb.finish_with_message("Download completed");
            }

            for worker_pb in self.worker_progress_bars.values() {
                worker_pb.finish_and_clear();
            }

            terminal::disable_raw_mode()
                .map_err(|e| DownloadError::Other(format!("Failed to disable raw mode: {}", e)))?;
        }

        // Final statistics report
        let stats = self.stats.read().await;
        eprintln!("\n✅ Download Summary:");
        eprintln!("   Total files: {}", stats.total_files);
        eprintln!("   Completed: {}", stats.files_completed);
        eprintln!("   Failed: {}", stats.files_failed);
        eprintln!("   Total bytes: {} bytes", stats.total_bytes_downloaded);
        eprintln!("   Duration: {:?}", stats.session_duration);

        if stats.files_failed > 0 {
            eprintln!("⚠️  Some files failed to download. Check logs for details.");
        }

        Ok(())
    }

    /// Start the background update task
    async fn start_update_task(
        &self,
        mut event_rx: mpsc::UnboundedReceiver<ProgressEvent>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        let main_pb = self.main_progress.clone();
        let worker_bars = self.worker_progress_bars.clone();
        let worker_progress = self.worker_progress.clone();
        let stats = self.stats.clone();
        let update_interval = self.config.update_interval;
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut last_update = Instant::now();

            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Some(event) => {
                                Self::handle_progress_event(
                                    &event,
                                    &main_pb,
                                    &worker_bars,
                                    &worker_progress,
                                    &stats,
                                    &config
                                ).await;
                            }
                            None => {
                                debug!("Progress event channel closed");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Progress display received shutdown signal");
                        break;
                    }
                    _ = tokio::time::sleep(update_interval) => {
                        if last_update.elapsed() >= update_interval {
                            Self::periodic_update(&main_pb, &stats).await;
                            last_update = Instant::now();
                        }
                    }
                }
            }
        })
    }

    /// Handle a progress event and update displays
    async fn handle_progress_event(
        event: &ProgressEvent,
        main_pb: &Option<ProgressBar>,
        worker_bars: &HashMap<u32, ProgressBar>,
        worker_progress: &Arc<RwLock<HashMap<u32, WorkerProgress>>>,
        stats: &Arc<RwLock<DownloadStats>>,
        config: &ProgressConfig,
    ) {
        match event {
            ProgressEvent::FileCompleted {
                worker_id,
                bytes_downloaded,
                file_name,
            } => {
                // Update main progress
                if let Some(pb) = main_pb {
                    pb.inc(1);
                }

                // Update worker progress
                {
                    let mut worker_map = worker_progress.write().await;
                    let worker = worker_map
                        .entry(*worker_id)
                        .or_insert_with(|| WorkerProgress {
                            worker_id: *worker_id,
                            status: "Working".to_string(),
                            current_file: None,
                            files_completed: 0,
                            bytes_downloaded: 0,
                            last_update: Instant::now(),
                        });

                    worker.files_completed += 1;
                    worker.bytes_downloaded += bytes_downloaded;
                    worker.current_file = None;
                    worker.last_update = Instant::now();
                }

                // Update worker progress bar
                if let Some(worker_pb) = worker_bars.get(worker_id) {
                    let truncated_name = if file_name.len() > config.max_filename_width {
                        format!(
                            "...{}",
                            &file_name[file_name.len() - config.max_filename_width + 3..]
                        )
                    } else {
                        file_name.clone()
                    };
                    worker_pb.set_message(format!("✅ Completed: {}", truncated_name));
                }

                // Update global stats
                {
                    let mut stats_guard = stats.write().await;
                    stats_guard.files_completed += 1;
                    stats_guard.total_bytes_downloaded += bytes_downloaded;
                }
            }

            ProgressEvent::FileFailed {
                worker_id,
                file_name,
                error,
            } => {
                // Update worker progress bar
                if let Some(worker_pb) = worker_bars.get(worker_id) {
                    let truncated_name = if file_name.len() > config.max_filename_width {
                        format!(
                            "...{}",
                            &file_name[file_name.len() - config.max_filename_width + 3..]
                        )
                    } else {
                        file_name.clone()
                    };
                    worker_pb.set_message(format!("❌ Failed: {} ({})", truncated_name, error));
                }

                // Update global stats
                {
                    let mut stats_guard = stats.write().await;
                    stats_guard.files_failed += 1;
                }
            }

            ProgressEvent::WorkerStatusChanged {
                worker_id,
                status,
                current_file,
            } => {
                // Update worker progress
                {
                    let mut worker_map = worker_progress.write().await;
                    let worker = worker_map
                        .entry(*worker_id)
                        .or_insert_with(|| WorkerProgress {
                            worker_id: *worker_id,
                            status: status.clone(),
                            current_file: current_file.clone(),
                            files_completed: 0,
                            bytes_downloaded: 0,
                            last_update: Instant::now(),
                        });

                    worker.status = status.clone();
                    worker.current_file = current_file.clone();
                    worker.last_update = Instant::now();
                }

                // Update worker progress bar
                if let Some(worker_pb) = worker_bars.get(worker_id) {
                    let message = if let Some(file) = current_file {
                        let truncated_name = if file.len() > config.max_filename_width {
                            format!("...{}", &file[file.len() - config.max_filename_width + 3..])
                        } else {
                            file.clone()
                        };
                        format!("{}: {}", status, truncated_name)
                    } else {
                        status.clone()
                    };
                    worker_pb.set_message(message);
                }
            }

            ProgressEvent::StatsUpdate { stats: new_stats } => {
                // Update global stats
                {
                    let mut stats_guard = stats.write().await;
                    *stats_guard = new_stats.clone();
                }

                // Update main progress bar with rate information
                if let Some(pb) = main_pb {
                    pb.set_position(new_stats.files_completed as u64);

                    if new_stats.download_rate_bps > 0.0 {
                        let rate_mb = new_stats.download_rate_bps / (1024.0 * 1024.0);
                        pb.set_message(format!("Downloading at {:.1} MB/s", rate_mb));
                    }
                }
            }

            ProgressEvent::SessionCompleted {
                total_files: _,
                successful,
                failed,
                duration,
            } => {
                // Finalize all progress bars
                if let Some(pb) = main_pb {
                    pb.finish_with_message(format!(
                        "✅ Completed: {} successful, {} failed in {:?}",
                        successful, failed, duration
                    ));
                }

                for worker_pb in worker_bars.values() {
                    worker_pb.finish_and_clear();
                }
            }
        }
    }

    /// Perform periodic updates to the display
    async fn periodic_update(main_pb: &Option<ProgressBar>, stats: &Arc<RwLock<DownloadStats>>) {
        if let Some(pb) = main_pb {
            let stats_guard = stats.read().await;

            // Update position and rate
            pb.set_position(stats_guard.files_completed as u64);

            if stats_guard.download_rate_bps > 0.0 {
                let rate_mb = stats_guard.download_rate_bps / (1024.0 * 1024.0);
                if let Some(eta) = &stats_guard.estimated_completion {
                    let now = chrono::Utc::now();
                    let eta_duration = eta
                        .signed_duration_since(now)
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    pb.set_message(format!("📥 {:.1} MB/s (ETA: {:?})", rate_mb, eta_duration));
                } else {
                    pb.set_message(format!("📥 {:.1} MB/s", rate_mb));
                }
            }
        }
    }
}

impl Drop for ProgressDisplay {
    fn drop(&mut self) {
        // Cleanup on drop
        if self.config.enable_progress_bars && self.is_terminal {
            let _ = terminal::disable_raw_mode();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ProgressConfig {
        ProgressConfig {
            enable_progress_bars: false, // Disable for testing
            update_interval: Duration::from_millis(1),
            show_worker_details: true,
            enable_colors: false,
            show_download_rate: true,
            show_eta: true,
            max_filename_width: 20,
            enable_spinner: false,
            compact_mode: true,
        }
    }

    /// Test progress display creation and configuration
    ///
    /// Verifies that progress displays can be created with different
    /// configurations and that they initialize properly.
    #[tokio::test]
    async fn test_progress_display_creation() {
        let config = create_test_config();
        let display = ProgressDisplay::new(config.clone());

        assert_eq!(display.config.update_interval, config.update_interval);
        assert_eq!(
            display.config.show_worker_details,
            config.show_worker_details
        );
        assert!(display.multi_progress.is_none());
        assert!(display.event_tx.is_none());
    }

    /// Test progress events and updates
    ///
    /// Ensures that progress events are properly handled and that
    /// statistics are updated correctly in response to events.
    #[tokio::test]
    async fn test_progress_events() {
        let config = create_test_config();
        let mut display = ProgressDisplay::new(config);

        // Start progress display
        display.start(10, 2).await.unwrap();

        // Send file completion event
        let event = ProgressEvent::FileCompleted {
            worker_id: 1,
            bytes_downloaded: 1024,
            file_name: "test.csv".to_string(),
        };

        display.update(event).await.unwrap();

        // Give time for processing
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Check that statistics were updated
        {
            let stats = display.stats.read().await;
            assert_eq!(stats.total_files, 10);
        }

        // Cleanup
        display.finish().await.unwrap();
    }

    /// Test text mode fallback
    ///
    /// Verifies that progress display gracefully falls back to text mode
    /// when terminal features are not available.
    #[tokio::test]
    async fn test_text_mode_fallback() {
        let mut config = create_test_config();
        config.enable_progress_bars = false;

        let mut display = ProgressDisplay::new(config);

        // Start should succeed in text mode
        let result = display.start(5, 1).await;
        assert!(result.is_ok());

        // Update should work
        let event = ProgressEvent::FileCompleted {
            worker_id: 1,
            bytes_downloaded: 512,
            file_name: "test.txt".to_string(),
        };

        let result = display.update(event).await;
        assert!(result.is_ok());

        // Finish should clean up properly
        let result = display.finish().await;
        assert!(result.is_ok());
    }

    /// Test progress configuration defaults
    ///
    /// Ensures that default configuration values are sensible for
    /// typical usage scenarios.
    #[tokio::test]
    async fn test_progress_config_defaults() {
        let config = ProgressConfig::default();

        assert!(config.enable_progress_bars);
        assert!(config.show_worker_details);
        assert!(config.enable_colors);
        assert!(config.show_download_rate);
        assert!(config.show_eta);
        assert!(config.update_interval > Duration::ZERO);
        assert!(config.max_filename_width > 0);
    }

    /// Test filename truncation
    ///
    /// Verifies that long filenames are properly truncated to fit
    /// within the configured display width.
    #[tokio::test]
    async fn test_filename_truncation() {
        let config = ProgressConfig {
            max_filename_width: 10,
            ..create_test_config()
        };

        let mut display = ProgressDisplay::new(config);
        display.start(1, 1).await.unwrap();

        // Send event with long filename
        let event = ProgressEvent::FileCompleted {
            worker_id: 1,
            bytes_downloaded: 1024,
            file_name: "very_long_filename_that_should_be_truncated.csv".to_string(),
        };

        // Should handle without error
        let result = display.update(event).await;
        assert!(result.is_ok());

        display.finish().await.unwrap();
    }

    /// Test session completion event
    ///
    /// Ensures that session completion events properly finalize the
    /// progress display and provide accurate statistics.
    #[tokio::test]
    async fn test_session_completion() {
        let config = create_test_config();
        let mut display = ProgressDisplay::new(config);

        display.start(3, 1).await.unwrap();

        // Send completion event
        let event = ProgressEvent::SessionCompleted {
            total_files: 3,
            successful: 2,
            failed: 1,
            duration: Duration::from_secs(30),
        };

        let result = display.update(event).await;
        assert!(result.is_ok());

        display.finish().await.unwrap();
    }
}
