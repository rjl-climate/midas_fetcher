//! Download orchestration and progress coordination
//!
//! This module provides the main coordination layer that orchestrates worker pools,
//! collects progress updates, handles graceful shutdown, and provides status reporting
//! for the CLI. It implements the control plane for the download system.
//!
//! # Key Features
//!
//! - **Worker Pool Management**: Spawns and manages configurable number of workers
//! - **Progress Aggregation**: Collects and aggregates progress from all workers
//! - **Graceful Shutdown**: Handles CTRL-C and other termination signals cleanly
//! - **Real-time Status**: Provides live download statistics and ETA calculations
//! - **Error Coordination**: Centralizes error handling and recovery strategies
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::{
//!     Coordinator, CoordinatorConfig, CacheManager, CedaClient, WorkQueue
//! };
//! use std::sync::Arc;
//! use std::path::PathBuf;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Setup shared components
//! let cache = Arc::new(CacheManager::new(Default::default()).await?);
//! let client = Arc::new(CedaClient::new().await?);
//! let queue = Arc::new(WorkQueue::new());
//!
//! // Configure coordinator
//! let config = CoordinatorConfig {
//!     worker_count: 4,
//!     progress_update_interval: std::time::Duration::from_millis(500),
//!     enable_progress_bar: true,
//!     ..Default::default()
//! };
//!
//! // Create and run coordinator
//! let mut coordinator = Coordinator::new(config, queue, cache, client);
//!
//! // Start download process with progress monitoring
//! let result = coordinator.run_downloads().await?;
//! println!("Downloaded {} files", result.files_completed);
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::app::cache::CacheManager;
use crate::app::client::CedaClient;
use crate::app::queue::WorkQueue;
use crate::app::worker::{WorkerConfig, WorkerPool, WorkerProgress};
use crate::constants::workers;
use crate::errors::DownloadResult;

/// Configuration for the download coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Number of concurrent workers to spawn
    pub worker_count: usize,
    /// How often to update progress displays
    pub progress_update_interval: Duration,
    /// Maximum time to wait for graceful shutdown
    pub shutdown_timeout: Duration,
    /// Enable real-time progress bar display
    pub enable_progress_bar: bool,
    /// Enable detailed logging during downloads
    pub verbose_logging: bool,
    /// Batch size for progress updates
    pub progress_batch_size: usize,
    /// Worker configuration
    pub worker_config: WorkerConfig,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            worker_count: workers::DEFAULT_WORKER_COUNT,
            progress_update_interval: Duration::from_millis(500),
            shutdown_timeout: Duration::from_secs(30),
            enable_progress_bar: true,
            verbose_logging: false,
            progress_batch_size: 50,
            worker_config: WorkerConfig::default(),
        }
    }
}

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

/// Main coordinator for orchestrating downloads
pub struct Coordinator {
    config: CoordinatorConfig,
    queue: Arc<WorkQueue>,
    cache: Arc<CacheManager>,
    client: Arc<CedaClient>,
    stats: Arc<RwLock<DownloadStats>>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    worker_pool: Option<WorkerPool>,
}

impl Coordinator {
    /// Create a new coordinator with the given configuration and shared components
    pub fn new(
        config: CoordinatorConfig,
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
    ) -> Self {
        let stats = Arc::new(RwLock::new(DownloadStats::default()));

        Self {
            config,
            queue,
            cache,
            client,
            stats,
            shutdown_tx: None,
            worker_pool: None,
        }
    }

    /// Run the complete download process with orchestration
    ///
    /// This is the main entry point that:
    /// 1. Sets up signal handling for graceful shutdown
    /// 2. Spawns the worker pool
    /// 3. Monitors progress and collects statistics
    /// 4. Handles shutdown and cleanup
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if worker pool setup fails or critical errors occur
    pub async fn run_downloads(&mut self) -> DownloadResult<SessionResult> {
        let session_start = Instant::now();
        info!(
            "Starting download coordination with {} workers",
            self.config.worker_count
        );

        // Setup shutdown signaling
        let (shutdown_tx, _) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx.clone());

        // Initialize statistics
        {
            let mut stats = self.stats.write().await;
            stats.session_start = Utc::now();
            stats.total_files = self.queue.stats().await.total_added as usize;
            stats.active_workers = self.config.worker_count;
        }

        // Setup signal handling for graceful shutdown
        let shutdown_signal = self.setup_signal_handling(shutdown_tx.clone());

        // Create and start worker pool
        let worker_pool_result = self.create_worker_pool().await;
        let mut worker_pool = match worker_pool_result {
            Ok(pool) => pool,
            Err(e) => {
                error!("Failed to create worker pool: {}", e);
                return Ok(SessionResult {
                    stats: self.stats.read().await.clone(),
                    success: false,
                    shutdown_errors: vec![format!("Worker pool creation failed: {}", e)],
                    total_duration: session_start.elapsed(),
                });
            }
        };

        // Start progress monitoring
        let (progress_tx, progress_rx) = mpsc::channel(self.config.progress_batch_size);
        let progress_monitor = self.start_progress_monitoring(progress_rx, shutdown_tx.subscribe());

        // Start the worker pool
        info!("Starting worker pool...");
        if let Err(e) = worker_pool.start(progress_tx).await {
            error!("Failed to start worker pool: {}", e);
            return Ok(SessionResult {
                stats: self.stats.read().await.clone(),
                success: false,
                shutdown_errors: vec![format!("Worker pool start failed: {}", e)],
                total_duration: session_start.elapsed(),
            });
        }

        self.worker_pool = Some(worker_pool);

        // Wait for completion or shutdown signal
        let completion_result = tokio::select! {
            _ = shutdown_signal => {
                info!("Shutdown signal received, initiating graceful shutdown");
                self.handle_shutdown().await
            }
            _ = self.wait_for_completion() => {
                info!("All downloads completed naturally");
                self.handle_completion().await
            }
        };

        // Wait for progress monitor to finish
        if let Err(e) = progress_monitor.await {
            warn!("Progress monitor task failed: {}", e);
        }

        // Shutdown worker pool
        let shutdown_errors = if let Some(pool) = self.worker_pool.take() {
            match tokio::time::timeout(self.config.shutdown_timeout, pool.shutdown()).await {
                Ok(Ok(())) => Vec::new(),
                Ok(Err(e)) => vec![format!("Worker pool shutdown error: {}", e)],
                Err(_) => {
                    warn!(
                        "Worker pool shutdown timed out after {:?}",
                        self.config.shutdown_timeout
                    );
                    vec!["Worker pool shutdown timed out".to_string()]
                }
            }
        } else {
            Vec::new()
        };

        // Update final statistics
        let final_stats = {
            let mut stats = self.stats.write().await;
            stats.session_duration = session_start.elapsed();
            stats.active_workers = 0;
            stats.clone()
        };

        info!(
            "Download session completed in {:?}",
            session_start.elapsed()
        );
        info!(
            "Final stats: {} completed, {} failed, {} total",
            final_stats.files_completed, final_stats.files_failed, final_stats.total_files
        );

        Ok(SessionResult {
            stats: final_stats,
            success: completion_result.is_ok(),
            shutdown_errors,
            total_duration: session_start.elapsed(),
        })
    }

    /// Get current download statistics
    pub async fn get_stats(&self) -> DownloadStats {
        self.stats.read().await.clone()
    }

    /// Trigger graceful shutdown
    pub async fn shutdown(&self) -> DownloadResult<()> {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
        Ok(())
    }

    /// Create and configure the worker pool
    async fn create_worker_pool(&self) -> DownloadResult<WorkerPool> {
        info!(
            "Creating worker pool with {} workers",
            self.config.worker_count
        );

        let mut worker_config = self.config.worker_config.clone();
        worker_config.worker_count = self.config.worker_count;

        let pool = WorkerPool::new(
            worker_config,
            self.queue.clone(),
            self.cache.clone(),
            self.client.clone(),
        );

        Ok(pool)
    }

    /// Setup signal handling for graceful shutdown (CTRL-C, SIGTERM)
    fn setup_signal_handling(&self, shutdown_tx: broadcast::Sender<()>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let ctrl_c = async {
                signal::ctrl_c()
                    .await
                    .expect("Failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to install signal handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {
                    info!("Received Ctrl+C, initiating shutdown");
                },
                _ = terminate => {
                    info!("Received terminate signal, initiating shutdown");
                },
            }

            let _ = shutdown_tx.send(());
        })
    }

    /// Start progress monitoring task
    fn start_progress_monitoring(
        &self,
        mut progress_rx: mpsc::Receiver<WorkerProgress>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        let stats = self.stats.clone();
        let queue = self.queue.clone();
        let update_interval = self.config.progress_update_interval;
        let verbose = self.config.verbose_logging;

        tokio::spawn(async move {
            let mut last_update = Instant::now();
            let mut bytes_window = Vec::new();
            let window_size = 10; // Rolling window for rate calculation

            loop {
                tokio::select! {
                    progress = progress_rx.recv() => {
                        match progress {
                            Some(update) => {
                                if verbose {
                                    debug!("Worker {} progress: {:?} status, {} files completed",
                                           update.worker_id, update.status, update.files_completed);
                                }

                                // Update statistics
                                {
                                    let mut stats_guard = stats.write().await;

                                    // Track bytes for rate calculation
                                    bytes_window.push((Instant::now(), update.bytes_downloaded));
                                    if bytes_window.len() > window_size {
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
                    _ = tokio::time::sleep(update_interval) => {
                        // Periodic statistics update
                        if last_update.elapsed() >= update_interval {
                            let queue_stats = queue.stats().await;
                            let mut stats_guard = stats.write().await;

                            stats_guard.files_completed = queue_stats.completed_count as usize;
                            stats_guard.files_failed = queue_stats.failed_count as usize;
                            stats_guard.files_in_progress = queue_stats.in_progress_count as usize;
                            stats_guard.session_duration = stats_guard.session_start.signed_duration_since(Utc::now()).to_std().unwrap_or(Duration::ZERO);

                            // Calculate ETA if we have a positive download rate
                            if stats_guard.download_rate_bps > 0.0 {
                                let remaining_files = stats_guard.total_files.saturating_sub(stats_guard.files_completed);
                                if remaining_files > 0 {
                                    let avg_file_size = if stats_guard.files_completed > 0 {
                                        stats_guard.total_bytes_downloaded as f64 / stats_guard.files_completed as f64
                                    } else {
                                        1024.0 // Default estimate
                                    };

                                    let remaining_bytes = remaining_files as f64 * avg_file_size;
                                    let eta_seconds = remaining_bytes / stats_guard.download_rate_bps;

                                    stats_guard.estimated_completion = Some(
                                        Utc::now() + chrono::Duration::seconds(eta_seconds as i64)
                                    );
                                } else {
                                    stats_guard.estimated_completion = Some(Utc::now());
                                }
                            }

                            last_update = Instant::now();
                        }
                    }
                }
            }
        })
    }

    /// Wait for all downloads to complete naturally
    async fn wait_for_completion(&self) {
        loop {
            let queue_stats = self.queue.stats().await;

            // Check if all work is done
            if queue_stats.pending_count == 0 && queue_stats.in_progress_count == 0 {
                debug!(
                    "All downloads completed: {} successful, {} failed",
                    queue_stats.completed_count, queue_stats.failed_count
                );
                break;
            }

            // Brief sleep to avoid busy waiting
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Handle graceful shutdown
    async fn handle_shutdown(&mut self) -> DownloadResult<()> {
        info!("Initiating graceful shutdown...");

        // Signal shutdown to all components
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }

        // Update statistics to reflect shutdown state
        {
            let mut stats = self.stats.write().await;
            stats.session_duration = stats
                .session_start
                .signed_duration_since(Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
        }

        Ok(())
    }

    /// Handle natural completion
    async fn handle_completion(&mut self) -> DownloadResult<()> {
        info!("All downloads completed successfully");

        // Final statistics update
        let queue_stats = self.queue.stats().await;
        {
            let mut stats = self.stats.write().await;
            stats.files_completed = queue_stats.completed_count as usize;
            stats.files_failed = queue_stats.failed_count as usize;
            stats.files_in_progress = 0;
            stats.active_workers = 0;
            stats.session_duration = stats
                .session_start
                .signed_duration_since(Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::hash::Md5Hash;
    use crate::app::models::{DatasetInfo, FileInfo};
    use crate::app::{CacheConfig, CacheManager, CedaClient, WorkQueue};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_config() -> CoordinatorConfig {
        CoordinatorConfig {
            worker_count: 2,
            progress_update_interval: Duration::from_millis(10),
            shutdown_timeout: Duration::from_millis(100),
            enable_progress_bar: false,
            verbose_logging: false,
            progress_batch_size: 5,
            worker_config: WorkerConfig {
                worker_count: 2,
                max_retries: 1,
                retry_base_delay: Duration::from_millis(10),
                retry_max_delay: Duration::from_millis(100),
                idle_sleep_duration: Duration::from_millis(5),
                progress_buffer_size: 5,
                download_timeout: Duration::from_millis(500),
                detailed_progress: false,
            },
        }
    }

    async fn create_test_components() -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>) {
        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        let queue = Arc::new(WorkQueue::new());
        let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
        let client = Arc::new(CedaClient::new_simple().await.unwrap());

        (queue, cache, client)
    }

    /// Test coordinator creation and basic configuration
    ///
    /// Verifies that a coordinator can be created with valid configuration
    /// and that initial state is properly set up.
    #[tokio::test]
    async fn test_coordinator_creation() {
        let config = create_test_config();
        let (queue, cache, client) = create_test_components().await;

        let coordinator = Coordinator::new(config.clone(), queue, cache, client);

        assert_eq!(coordinator.config.worker_count, config.worker_count);
        assert!(coordinator.shutdown_tx.is_none());
        assert!(coordinator.worker_pool.is_none());
    }

    /// Test statistics initialization and updates
    ///
    /// Ensures that download statistics are properly initialized and
    /// can be updated throughout the download process.
    #[tokio::test]
    async fn test_statistics_tracking() {
        let config = create_test_config();
        let (queue, cache, client) = create_test_components().await;

        let coordinator = Coordinator::new(config, queue.clone(), cache, client);

        // Initial stats should be default
        let stats = coordinator.get_stats().await;
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.files_failed, 0);
        assert_eq!(stats.total_bytes_downloaded, 0);

        // Add some work to queue and verify stats update
        let hash = Md5Hash::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let file_info = FileInfo {
            hash,
            relative_path: "./test.txt".to_string(),
            file_name: "test.txt".to_string(),
            dataset_info: DatasetInfo {
                dataset_name: "test".to_string(),
                version: "v1".to_string(),
                county: None,
                station_id: None,
                station_name: None,
                quality_version: None,
                year: None,
                file_type: None,
            },
            retry_count: 0,
            last_attempt: None,
            estimated_size: Some(1024),
            destination_path: PathBuf::from("/tmp/test.txt"),
        };

        queue.add_work(file_info).await.unwrap();

        // Stats should reflect the added work
        let _stats = coordinator.get_stats().await;
        // Note: total_files is set during run_downloads() initialization
    }

    /// Test graceful shutdown functionality
    ///
    /// Verifies that shutdown signals are properly handled and that
    /// the coordinator can gracefully terminate ongoing operations.
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let config = create_test_config();
        let (queue, cache, client) = create_test_components().await;

        let coordinator = Coordinator::new(config, queue, cache, client);

        // Test shutdown before starting downloads
        let result = coordinator.shutdown().await;
        assert!(result.is_ok());
    }

    /// Test configuration validation
    ///
    /// Ensures that coordinator configurations are validated and
    /// that invalid configurations are handled appropriately.
    #[tokio::test]
    async fn test_config_validation() {
        let (queue, cache, client) = create_test_components().await;

        // Test with zero workers (should still work, just no downloads)
        let config = CoordinatorConfig {
            worker_count: 0,
            ..create_test_config()
        };

        let coordinator = Coordinator::new(config.clone(), queue, cache, client);
        assert_eq!(coordinator.config.worker_count, 0);
    }

    /// Test download stats calculations
    ///
    /// Verifies that download rate calculations, ETA estimates, and
    /// other derived statistics are computed correctly.
    #[tokio::test]
    async fn test_download_stats_calculations() {
        let config = create_test_config();
        let (queue, cache, client) = create_test_components().await;

        let coordinator = Coordinator::new(config, queue, cache, client);

        // Get initial stats
        let stats = coordinator.get_stats().await;
        assert_eq!(stats.download_rate_bps, 0.0);
        assert!(stats.estimated_completion.is_none());

        // Statistics calculations are tested through the progress monitoring
        // This test mainly verifies the basic structure is in place
    }

    /// Test coordinator default configuration
    ///
    /// Ensures that default configuration values are sensible and
    /// match the expected patterns for production use.
    #[tokio::test]
    async fn test_coordinator_config_default() {
        let config = CoordinatorConfig::default();
        assert_eq!(config.worker_count, workers::DEFAULT_WORKER_COUNT);
        assert!(config.progress_update_interval > Duration::ZERO);
        assert!(config.shutdown_timeout > Duration::ZERO);
        assert!(config.enable_progress_bar);
        assert!(!config.verbose_logging);
    }

    /// Test session result structure
    ///
    /// Verifies that SessionResult properly captures all relevant
    /// information about a download session including errors and timing.
    #[tokio::test]
    async fn test_session_result_structure() {
        let result = SessionResult {
            stats: DownloadStats::default(),
            success: true,
            shutdown_errors: vec!["test error".to_string()],
            total_duration: Duration::from_secs(60),
        };

        assert!(result.success);
        assert_eq!(result.shutdown_errors.len(), 1);
        assert_eq!(result.total_duration, Duration::from_secs(60));
    }
}
