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
//! # Architecture
//!
//! The coordinator module is organized into specialized components:
//!
//! - [`config`] - Configuration structures and validation
//! - [`stats`] - Download statistics tracking and aggregation
//! - [`progress`] - Real-time progress monitoring and rate calculations
//! - [`signals`] - Signal handling for graceful shutdown
//! - [`background_tasks`] - Background task management and coordination
//! - [`completion`] - Download completion detection and monitoring
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
//! let config = CoordinatorConfig::default()
//!     .with_worker_count(4)
//!     .with_verbose_logging(true);
//!
//! // Create and run coordinator
//! let mut coordinator = Coordinator::new(config, queue, cache, client);
//!
//! // Start download process with progress monitoring
//! let result = coordinator.run_downloads().await?;
//! println!("Downloaded {} files", result.stats.files_completed);
//! # Ok(())
//! # }
//! ```

pub mod background_tasks;
pub mod completion;
pub mod config;
pub mod progress;
pub mod signals;
pub mod stats;

#[cfg(test)]
pub mod tests;

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, RwLock};
use tracing::{error, info};

use crate::app::cache::CacheManager;
use crate::app::client::CedaClient;
use crate::app::queue::WorkQueue;
use crate::app::worker::WorkerPool;
use crate::errors::DownloadResult;

pub use background_tasks::BackgroundTaskManager;
pub use completion::{CompletionDetector, CompletionStatus};
pub use config::CoordinatorConfig;
pub use progress::{ProgressAggregator, ProgressMonitor, RateCalculator};
pub use signals::{create_shutdown_channel, wait_for_shutdown_signal, SignalHandler};
pub use stats::{DownloadStats, SessionResult};

/// Main coordinator for orchestrating downloads
///
/// The coordinator serves as the central orchestration point for download operations,
/// managing worker pools, progress monitoring, signal handling, and graceful shutdown.
/// It has been refactored to use specialized components for different responsibilities.
pub struct Coordinator {
    config: CoordinatorConfig,
    queue: Arc<WorkQueue>,
    cache: Arc<CacheManager>,
    client: Arc<CedaClient>,
    stats: Arc<RwLock<DownloadStats>>,
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
        }
    }

    /// Create a new coordinator with expected file count for accurate progress tracking
    pub fn new_with_expected_files(
        config: CoordinatorConfig,
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
        expected_files: usize,
    ) -> Self {
        let stats = DownloadStats::new_with_expected_files(expected_files);
        let stats = Arc::new(RwLock::new(stats));

        Self {
            config,
            queue,
            cache,
            client,
            stats,
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

        // Validate configuration
        if let Err(e) = self.config.validate() {
            error!("Invalid coordinator configuration: {}", e);
            return Ok(SessionResult::failed(
                self.stats.read().await.clone(),
                session_start.elapsed(),
                vec![format!("Configuration validation failed: {}", e)],
            ));
        }

        // Setup shutdown signaling
        let (shutdown_tx, _) = create_shutdown_channel();
        let signal_handler = SignalHandler::new(shutdown_tx.clone());
        let mut shutdown_signal = signal_handler.setup();

        // Initialize statistics
        {
            let mut stats = self.stats.write().await;
            stats.session_start = chrono::Utc::now();

            // Only update total_files if it wasn't set during construction
            if stats.total_files == 0 {
                stats.total_files = self.queue.stats().await.total_added as usize;
            }

            stats.active_workers = self.config.worker_count;
        }

        // Create and start worker pool
        let mut worker_pool = match self.create_worker_pool().await {
            Ok(pool) => pool,
            Err(e) => {
                error!("Failed to create worker pool: {}", e);
                return Ok(SessionResult::failed(
                    self.stats.read().await.clone(),
                    session_start.elapsed(),
                    vec![format!("Worker pool creation failed: {}", e)],
                ));
            }
        };

        // Start progress monitoring
        let (progress_tx, progress_rx) = mpsc::channel(self.config.progress_batch_size);
        let progress_monitor = ProgressMonitor::new(
            self.stats.clone(),
            self.queue.clone(),
            self.config.progress_update_interval,
            self.config.verbose_logging,
        );
        let progress_handle = progress_monitor.start_monitoring(progress_rx, shutdown_tx.subscribe());

        // Start the worker pool
        if let Err(e) = worker_pool.start(progress_tx).await {
            error!("Failed to start worker pool: {}", e);
            return Ok(SessionResult::failed(
                self.stats.read().await.clone(),
                session_start.elapsed(),
                vec![format!("Worker pool start failed: {}", e)],
            ));
        }

        // Start background tasks
        let mut background_tasks = BackgroundTaskManager::new();
        background_tasks.start_cleanup_task(
            self.cache.clone(),
            self.queue.clone(),
            shutdown_tx.subscribe(),
        );
        background_tasks.start_periodic_logging_task(
            self.queue.clone(),
            shutdown_tx.subscribe(),
        );
        background_tasks.start_timeout_monitoring_task(
            self.queue.clone(),
            shutdown_tx.subscribe(),
        );

        // Create completion detector
        let expected_files = {
            let stats = self.stats.read().await;
            stats.total_files as u64
        };
        let completion_detector = CompletionDetector::new(self.queue.clone(), expected_files);

        // Wait for completion or shutdown signal
        let completion_result = tokio::select! {
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received, initiating graceful shutdown");
                self.handle_shutdown().await
            }
            _ = completion_detector.wait_for_completion() => {
                info!("All downloads completed naturally");
                self.handle_completion().await
            }
        };

        // Send shutdown signals to all background components
        let _ = shutdown_tx.send(());

        // Wait for background tasks to finish
        background_tasks.shutdown_all().await;

        // Wait for progress monitor to finish
        let _ = progress_handle.await;

        // Shutdown worker pool
        let shutdown_errors = match tokio::time::timeout(self.config.shutdown_timeout, worker_pool.shutdown()).await {
            Ok(Ok(())) => Vec::new(),
            Ok(Err(e)) => {
                error!("Worker pool shutdown error: {}", e);
                vec![format!("Worker pool shutdown error: {}", e)]
            }
            Err(_) => {
                error!("Worker pool shutdown timed out after {:?}", self.config.shutdown_timeout);
                vec!["Worker pool shutdown timed out".to_string()]
            }
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

        Ok(if completion_result.is_ok() && shutdown_errors.is_empty() {
            SessionResult::success(final_stats, session_start.elapsed())
        } else {
            SessionResult::failed(final_stats, session_start.elapsed(), shutdown_errors)
        })
    }

    /// Get current download statistics
    pub async fn get_stats(&self) -> DownloadStats {
        self.stats.read().await.clone()
    }

    /// Trigger graceful shutdown
    pub async fn shutdown(&self) -> DownloadResult<()> {
        // This method would need access to the shutdown sender, which we'd need to store
        // For now, we'll implement a basic version
        info!("Shutdown requested via API");
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

    /// Handle graceful shutdown
    async fn handle_shutdown(&mut self) -> DownloadResult<()> {
        info!("Initiating graceful shutdown...");

        // Update statistics to reflect shutdown state
        {
            let mut stats = self.stats.write().await;
            stats.update_duration();
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
            stats.update_duration();
        }

        Ok(())
    }
}