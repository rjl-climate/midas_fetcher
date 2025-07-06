//! Download worker implementation for concurrent file processing
//!
//! This module implements the core worker system that integrates the queue, cache,
//! and client components into a cohesive work-stealing download system. Workers
//! continuously seek work from the queue, attempt cache reservations, and download
//! files without ever waiting for specific files (preventing worker starvation).
//!
//! # Key Features
//!
//! - **Work-stealing pattern**: Workers never wait for specific files
//! - **Cache coordination**: Uses reservation system to prevent duplicate downloads
//! - **Rate-limited downloads**: Integrates with CEDA client rate limiting
//! - **Progress reporting**: Real-time download statistics and ETA calculations
//! - **Error recovery**: Automatic retry with exponential backoff
//! - **Graceful shutdown**: Clean termination with work completion
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::{DownloadWorker, WorkerConfig, WorkerPool};
//! use midas_fetcher::app::{CacheManager, CacheConfig, CedaClient, WorkQueue};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create shared components
//! let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//! let client = Arc::new(CedaClient::new().await?);
//! let queue = Arc::new(WorkQueue::new());
//!
//! // Create worker pool
//! let config = WorkerConfig::default();
//! let mut pool = WorkerPool::new(config, queue, cache, client);
//!
//! // Start workers
//! let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);
//! pool.start(progress_tx).await?;
//!
//! // Monitor progress
//! while let Some(progress) = progress_rx.recv().await {
//!     println!("Downloaded: {} bytes", progress.bytes_downloaded);
//! }
//!
//! // Shutdown gracefully
//! pool.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::app::cache::{CacheManager, ReservationStatus};
use crate::app::client::CedaClient;
use crate::app::models::FileInfo;
use crate::app::queue::WorkQueue;
use crate::constants::workers;
use crate::errors::{DownloadError, DownloadResult};

/// Configuration for download workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Number of concurrent workers to spawn
    pub worker_count: usize,
    /// Maximum retry attempts per file
    pub max_retries: u32,
    /// Base delay between retries (exponential backoff)
    pub retry_base_delay: Duration,
    /// Maximum retry delay (backoff cap)
    pub retry_max_delay: Duration,
    /// Sleep duration when no work is available
    pub idle_sleep_duration: Duration,
    /// Channel buffer size for progress reporting
    pub progress_buffer_size: usize,
    /// Timeout for individual downloads
    pub download_timeout: Duration,
    /// Enable detailed progress reporting
    pub detailed_progress: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_count: workers::DEFAULT_WORKER_COUNT,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(30),
            idle_sleep_duration: Duration::from_millis(100),
            progress_buffer_size: workers::CHANNEL_BUFFER_SIZE,
            download_timeout: Duration::from_secs(600), // 10 minutes (increased from 5)
            detailed_progress: true,
        }
    }
}

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

/// Current status of a download worker
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Statistics for a worker pool
#[derive(Debug, Clone, Default)]
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
    pub workers_by_status: std::collections::HashMap<WorkerStatus, usize>,
}

/// Individual download worker
#[derive(Debug)]
pub struct DownloadWorker {
    /// Unique worker identifier
    id: u32,
    /// Worker configuration
    config: WorkerConfig,
    /// Shared work queue
    queue: Arc<WorkQueue>,
    /// Shared cache manager
    cache: Arc<CacheManager>,
    /// Shared CEDA client
    client: Arc<CedaClient>,
    /// Progress reporting channel
    progress_tx: mpsc::Sender<WorkerProgress>,
    /// Worker statistics
    stats: WorkerStats,
    /// Shutdown signal receiver
    shutdown_rx: Arc<RwLock<Option<mpsc::Receiver<()>>>>,
}

/// Internal statistics for a worker
#[derive(Debug, Default)]
struct WorkerStats {
    files_completed: u64,
    total_bytes_downloaded: u64,
    current_download_start: Option<Instant>,
    current_bytes_downloaded: u64,
    speed_samples: Vec<(Instant, u64)>, // (timestamp, bytes) for speed calculation
    consecutive_empty_polls: u32,       // Track consecutive times we found no work
    last_empty_poll: Option<Instant>,
}

impl DownloadWorker {
    /// Create a new download worker
    pub fn new(
        id: u32,
        config: WorkerConfig,
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
        progress_tx: mpsc::Sender<WorkerProgress>,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            id,
            config,
            queue,
            cache,
            client,
            progress_tx,
            stats: WorkerStats::default(),
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_rx))),
        }
    }

    /// Start the worker loop
    pub async fn run(mut self) -> DownloadResult<()> {
        info!("Worker {} starting", self.id);

        // Report initial status
        self.report_progress(WorkerStatus::Idle, None).await;

        // Log that worker is ready to work
        debug!("Worker {} ready for work", self.id);

        loop {
            // Check for shutdown signal
            if self.check_shutdown().await {
                info!("Worker {} received shutdown signal", self.id);
                break;
            }

            // Main worker loop: get work and process it
            match self.worker_iteration().await {
                Ok(work_found) => {
                    if !work_found {
                        // No work available, use exponential backoff to reduce contention
                        self.report_progress(WorkerStatus::Idle, None).await;

                        // Track consecutive empty polls for backoff calculation
                        self.stats.consecutive_empty_polls += 1;
                        self.stats.last_empty_poll = Some(Instant::now());

                        // Exponential backoff: start with base duration, double up to max
                        let backoff_multiplier =
                            std::cmp::min(self.stats.consecutive_empty_polls, 6); // Cap at 6 (2^6 = 64x)
                        let base_sleep = self.config.idle_sleep_duration.as_millis() as u64;
                        let exponential_sleep = base_sleep * (1u64 << backoff_multiplier); // 2^n backoff
                        let capped_sleep = std::cmp::min(exponential_sleep, 2000); // Cap at 2 seconds

                        // Add jitter to prevent thundering herd (±25%)
                        let jitter_range = capped_sleep / 4;
                        let jitter =
                            fastrand::u64(0..=jitter_range * 2).saturating_sub(jitter_range);
                        let final_sleep = capped_sleep.saturating_add(jitter);

                        let sleep_duration = Duration::from_millis(final_sleep);
                        debug!(
                            "Worker {} idle (attempt {}), sleeping for {:?}",
                            self.id, self.stats.consecutive_empty_polls, sleep_duration
                        );
                        tokio::time::sleep(sleep_duration).await;
                    } else {
                        // Found work, reset backoff counter
                        if self.stats.consecutive_empty_polls > 0 {
                            debug!(
                                "Worker {} found work after {} empty polls",
                                self.id, self.stats.consecutive_empty_polls
                            );
                            self.stats.consecutive_empty_polls = 0;
                        }
                    }
                }
                Err(e) => {
                    debug!("Worker {} encountered error: {}", self.id, e);
                    self.report_progress(
                        WorkerStatus::Error { retry_count: 0 },
                        Some(format!("Worker error: {}", e)),
                    )
                    .await;

                    // Sleep longer on errors to avoid tight error loops
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        self.report_progress(WorkerStatus::Shutdown, None).await;
        info!("Worker {} shutting down", self.id);
        Ok(())
    }

    /// Perform one iteration of the worker loop
    async fn worker_iteration(&mut self) -> DownloadResult<bool> {
        // 1. Request work from queue
        self.report_progress(WorkerStatus::RequestingWork, None)
            .await;

        let work_request_start = std::time::Instant::now();
        let work_info = match self.queue.get_next_work().await {
            Some(work) => {
                let queue_wait_time = work_request_start.elapsed();
                if queue_wait_time > std::time::Duration::from_millis(50) {
                    debug!(
                        "Worker {} waited {:?} for work from queue",
                        self.id, queue_wait_time
                    );
                }
                work
            }
            None => {
                let queue_wait_time = work_request_start.elapsed();
                if queue_wait_time > std::time::Duration::from_millis(10) {
                    debug!(
                        "Worker {} found no work after {:?} queue wait",
                        self.id, queue_wait_time
                    );
                }
                return Ok(false); // No work available
            }
        };

        let file_info = work_info.file_info;
        debug!("Worker {} got work: {}", self.id, file_info.file_name);

        // 2. Attempt cache reservation
        self.report_progress(WorkerStatus::CheckingCache, None)
            .await;

        match self.cache.check_and_reserve(&file_info).await {
            Ok(ReservationStatus::AlreadyExists) => {
                debug!(
                    "Worker {} found file already cached: {}",
                    self.id, file_info.file_name
                );
                self.queue.mark_completed(&file_info.hash).await?;
                return Ok(true);
            }
            Ok(ReservationStatus::ReservedByOther { worker_id }) => {
                debug!(
                    "Worker {} found file reserved by worker {}: {} - immediately seeking new work",
                    self.id, worker_id, file_info.file_name
                );
                // Immediately seek new work - don't wait for this file
                return Ok(true);
            }
            Ok(ReservationStatus::Reserved) => {
                // We got the reservation, proceed with download
                debug!("Worker {} reserved file: {}", self.id, file_info.file_name);
            }
            Err(e) => {
                warn!("Worker {} cache reservation failed: {}", self.id, e);
                self.queue
                    .mark_failed(&file_info.hash, &format!("Cache reservation failed: {}", e))
                    .await?;
                return Err(e.into());
            }
        }

        // 3. Download and save the file
        let result = self.download_and_save_file(&file_info).await;

        // 4. Report completion to queue
        match &result {
            Ok(_) => {
                self.queue.mark_completed(&file_info.hash).await?;
            }
            Err(e) => {
                self.queue
                    .mark_failed(&file_info.hash, &e.to_string())
                    .await?;
            }
        }

        // 5. Update statistics
        match &result {
            Ok(_) => {
                self.stats.files_completed += 1;
                info!(
                    "Worker {} completed download: {}",
                    self.id, file_info.file_name
                );
            }
            Err(e) => {
                debug!(
                    "Worker {} failed to download: {} - {}",
                    self.id, file_info.file_name, e
                );
            }
        }

        result.map(|_| true)
    }

    /// Download and save a file with retry logic
    async fn download_and_save_file(&mut self, file_info: &FileInfo) -> DownloadResult<()> {
        let mut retry_count = 0;
        let mut retry_delay = self.config.retry_base_delay;

        loop {
            // Start download
            self.stats.current_download_start = Some(Instant::now());
            self.stats.current_bytes_downloaded = 0;
            self.stats.speed_samples.clear();

            self.report_progress(WorkerStatus::Downloading, None).await;

            match self.attempt_download(file_info).await {
                Ok(content) => {
                    // Download successful, now save atomically
                    self.report_progress(WorkerStatus::Saving, None).await;

                    match self.cache.save_file_atomic(&content, file_info).await {
                        Ok(()) => {
                            // Success! Update statistics
                            self.stats.total_bytes_downloaded += content.len() as u64;
                            return Ok(());
                        }
                        Err(e) => {
                            debug!("Worker {} failed to save file: {}", self.id, e);
                            retry_count += 1;

                            if retry_count >= self.config.max_retries {
                                return Err(DownloadError::MaxRetriesExceeded {
                                    max_retries: retry_count,
                                });
                            }

                            self.report_progress(
                                WorkerStatus::Error { retry_count },
                                Some(format!("Save failed, retrying: {}", e)),
                            )
                            .await;
                        }
                    }
                }
                Err(e) => {
                    debug!("Worker {} download failed: {}", self.id, e);
                    retry_count += 1;

                    if retry_count >= self.config.max_retries {
                        // Release the reservation on permanent failure
                        let _ = self.cache.release_reservation(&file_info.hash).await;
                        return Err(DownloadError::MaxRetriesExceeded {
                            max_retries: retry_count,
                        });
                    }

                    self.report_progress(
                        WorkerStatus::Error { retry_count },
                        Some(format!("Download failed, retrying: {}", e)),
                    )
                    .await;
                }
            }

            // Wait before retry with exponential backoff
            debug!(
                "Worker {} retrying in {:?} (attempt {})",
                self.id, retry_delay, retry_count
            );
            tokio::time::sleep(retry_delay).await;

            // Exponential backoff with cap
            retry_delay = std::cmp::min(retry_delay * 2, self.config.retry_max_delay);
        }
    }

    /// Attempt to download a file
    async fn attempt_download(&mut self, file_info: &FileInfo) -> DownloadResult<Vec<u8>> {
        let url = file_info.download_url("https://data.ceda.ac.uk");
        debug!("Worker {} downloading: {}", self.id, url);

        // Use client's download functionality with timeout
        let download_future = self.client.download_file_content(&url);

        match tokio::time::timeout(self.config.download_timeout, download_future).await {
            Ok(Ok(content)) => {
                debug!("Worker {} downloaded {} bytes", self.id, content.len());
                Ok(content)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(DownloadError::Timeout {
                seconds: self.config.download_timeout.as_secs(),
            }),
        }
    }

    /// Check if shutdown signal was received
    async fn check_shutdown(&self) -> bool {
        let mut shutdown_rx_guard = self.shutdown_rx.write().await;
        if let Some(rx) = shutdown_rx_guard.as_mut() {
            match rx.try_recv() {
                Ok(()) => {
                    *shutdown_rx_guard = None; // Consume the receiver
                    true
                }
                Err(mpsc::error::TryRecvError::Empty) => false,
                Err(mpsc::error::TryRecvError::Disconnected) => true,
            }
        } else {
            true // Already shut down
        }
    }

    /// Report progress to the monitoring system
    async fn report_progress(&self, status: WorkerStatus, error_message: Option<String>) {
        let progress = WorkerProgress {
            worker_id: self.id,
            file_info: None, // Could be enhanced to track current file
            bytes_downloaded: self.stats.current_bytes_downloaded,
            total_bytes: None, // Could be enhanced with file size estimates
            download_speed: self.calculate_download_speed(),
            eta_seconds: None, // Could be enhanced with ETA calculation
            status,
            timestamp: Utc::now(),
            files_completed: self.stats.files_completed,
            total_bytes_downloaded: self.stats.total_bytes_downloaded,
            error_message,
        };

        // Send progress update (non-blocking)
        if let Err(e) = self.progress_tx.try_send(progress) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    // Progress channel is full, skip this update
                    debug!("Worker {} progress channel full, skipping update", self.id);
                }
                mpsc::error::TrySendError::Closed(_) => {
                    // Progress monitoring has shut down
                    debug!("Worker {} progress channel closed", self.id);
                }
            }
        }
    }

    /// Calculate current download speed
    fn calculate_download_speed(&self) -> f64 {
        if self.stats.speed_samples.len() < 2 {
            return 0.0;
        }

        let recent_samples: Vec<_> = self
            .stats
            .speed_samples
            .iter()
            .rev()
            .take(10) // Use last 10 samples for smoothing
            .collect();

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
}

/// Pool for managing multiple download workers
#[derive(Debug)]
pub struct WorkerPool {
    /// Worker configuration
    config: WorkerConfig,
    /// Shared work queue
    queue: Arc<WorkQueue>,
    /// Shared cache manager  
    cache: Arc<CacheManager>,
    /// Shared CEDA client
    client: Arc<CedaClient>,
    /// Worker task handles
    worker_handles: Vec<JoinHandle<DownloadResult<()>>>,
    /// Shutdown signal senders
    shutdown_senders: Vec<mpsc::Sender<()>>,
    /// Pool statistics
    stats: Arc<RwLock<WorkerPoolStats>>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(
        config: WorkerConfig,
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
    ) -> Self {
        Self {
            config,
            queue,
            cache,
            client,
            worker_handles: Vec::new(),
            shutdown_senders: Vec::new(),
            stats: Arc::new(RwLock::new(WorkerPoolStats::default())),
        }
    }

    /// Start all workers
    pub async fn start(&mut self, progress_tx: mpsc::Sender<WorkerProgress>) -> DownloadResult<()> {
        info!("Starting {} workers", self.config.worker_count);

        for worker_id in 0..self.config.worker_count {
            let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

            let worker = DownloadWorker::new(
                worker_id as u32,
                self.config.clone(),
                self.queue.clone(),
                self.cache.clone(),
                self.client.clone(),
                progress_tx.clone(),
                shutdown_rx,
            );

            let handle = tokio::spawn(async move { worker.run().await });

            self.worker_handles.push(handle);
            self.shutdown_senders.push(shutdown_tx);
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.active_workers = self.config.worker_count;

        info!(
            "Worker pool started with {} workers",
            self.config.worker_count
        );
        Ok(())
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(self) -> DownloadResult<()> {
        info!("Shutting down worker pool");

        // Send shutdown signals to all workers
        for shutdown_tx in self.shutdown_senders {
            let _ = shutdown_tx.send(()).await; // Ignore errors, worker might already be done
        }

        // Wait for all workers to complete
        let mut results = Vec::new();
        for handle in self.worker_handles {
            results.push(handle.await);
        }

        // Check for worker errors
        let mut error_count = 0;
        for result in results {
            match result {
                Ok(Ok(())) => {
                    // Worker completed successfully
                }
                Ok(Err(e)) => {
                    debug!("Worker failed: {}", e);
                    error_count += 1;
                }
                Err(e) => {
                    debug!("Worker panicked: {}", e);
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            warn!("{} workers encountered errors during shutdown", error_count);
        }

        info!("Worker pool shutdown complete");
        Ok(())
    }

    /// Get current pool statistics
    pub async fn get_stats(&self) -> WorkerPoolStats {
        self.stats.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::cache::CacheConfig;
    use crate::app::client::ClientConfig;
    use tempfile::TempDir;

    fn create_test_worker_config() -> WorkerConfig {
        WorkerConfig {
            worker_count: 2,
            max_retries: 2,
            retry_base_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_millis(100),
            idle_sleep_duration: Duration::from_millis(10),
            progress_buffer_size: 10,
            download_timeout: Duration::from_secs(5),
            detailed_progress: true,
        }
    }

    /// Test that WorkerConfig provides sensible defaults
    ///
    /// Verifies that default configuration values match constants and
    /// provide reasonable retry behavior for production use.
    #[tokio::test]
    async fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.worker_count, workers::DEFAULT_WORKER_COUNT);
        assert_eq!(config.max_retries, 3);
        assert!(config.retry_base_delay > Duration::ZERO);
        assert!(config.retry_max_delay > config.retry_base_delay);
    }

    /// Test WorkerProgress serialization for JSON-based progress reporting
    ///
    /// Ensures that progress reports can be serialized/deserialized correctly
    /// for sending over channels, logging, or API responses. This is critical
    /// for real-time monitoring and debugging of download operations.
    #[tokio::test]
    async fn test_worker_progress_serialization() {
        let progress = WorkerProgress {
            worker_id: 1,
            file_info: None,
            bytes_downloaded: 1024,
            total_bytes: Some(2048),
            download_speed: 1024.0,
            eta_seconds: Some(1.0),
            status: WorkerStatus::Downloading,
            timestamp: Utc::now(),
            files_completed: 5,
            total_bytes_downloaded: 10240,
            error_message: None,
        };

        let serialized = serde_json::to_string(&progress).unwrap();
        let deserialized: WorkerProgress = serde_json::from_str(&serialized).unwrap();

        assert_eq!(progress.worker_id, deserialized.worker_id);
        assert_eq!(progress.bytes_downloaded, deserialized.bytes_downloaded);
        assert_eq!(progress.status, deserialized.status);
    }

    /// Test WorkerPool creation with proper component integration
    ///
    /// Verifies that a WorkerPool can be created with all required shared
    /// components (queue, cache, client) and initializes with correct state.
    /// This test ensures the worker pool architecture is sound before
    /// attempting to start workers.
    #[tokio::test]
    async fn test_worker_pool_creation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        let queue = Arc::new(WorkQueue::new());
        let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
        let client = Arc::new(
            CedaClient::new_simple_with_config(ClientConfig::default())
                .await
                .unwrap(),
        );

        let config = create_test_worker_config();
        let pool = WorkerPool::new(config.clone(), queue, cache, client);

        assert_eq!(pool.config.worker_count, config.worker_count);
        assert_eq!(pool.worker_handles.len(), 0); // Not started yet
        assert_eq!(pool.shutdown_senders.len(), 0);
    }

    /// Test serialization of all WorkerStatus variants
    ///
    /// Ensures that all possible worker status states can be properly
    /// serialized and deserialized. This is essential for progress
    /// monitoring systems that track worker states across different
    /// phases of the download process.
    #[tokio::test]
    async fn test_worker_status_variants() {
        // Test all WorkerStatus variants for serialization
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

    /// Test WorkerPoolStats default initialization
    ///
    /// Verifies that pool statistics start with correct initial values
    /// (all zeros, empty collections). This ensures metrics collection
    /// starts from a clean state and can accurately track progress.
    #[tokio::test]
    async fn test_worker_pool_stats_default() {
        let stats = WorkerPoolStats::default();
        assert_eq!(stats.active_workers, 0);
        assert_eq!(stats.files_completed, 0);
        assert_eq!(stats.files_failed, 0);
        assert_eq!(stats.total_bytes_downloaded, 0);
        assert_eq!(stats.average_download_speed, 0.0);
        assert!(stats.estimated_completion_time.is_none());
        assert!(stats.workers_by_status.is_empty());
    }

    /// End-to-end integration test for worker functionality
    ///
    /// This test verifies complete worker operation including:
    /// - Queue integration for work claiming
    /// - Cache reservation and atomic file operations  
    /// - Real CEDA downloads with authentication
    /// - Progress reporting and error handling
    /// - Worker pool coordination and shutdown
    ///
    /// Requires CEDA credentials and network access, so it's ignored
    /// by default and only run during explicit integration testing.
    ///
    /// Run with: cargo test test_worker_integration -- --ignored --nocapture
    ///
    /// Setup instructions:
    /// 1. Create a .env file in the project root with:
    ///    CEDA_USERNAME=your_username
    ///    CEDA_PASSWORD=your_password
    /// 2. Or set environment variables directly
    #[tokio::test]
    #[ignore = "Requires CEDA authentication and network access"]
    async fn test_worker_integration() {
        use crate::app::{ManifestConfig, ManifestStreamer};
        use crate::constants::env as env_constants;
        use futures::StreamExt;
        use std::{env, path::Path, time::Duration};
        use tempfile::TempDir;

        // Initialize tracing for detailed debugging
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(false)
            .with_line_number(false)
            .try_init()
            .ok();

        println!("🚀 Starting worker integration test");

        // Load credentials from .env file if it exists
        if Path::new(".env").exists() {
            println!("📁 Loading credentials from .env file...");
            let env_content = std::fs::read_to_string(".env").expect("Failed to read .env file");
            for line in env_content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    unsafe {
                        if key.trim() == env_constants::USERNAME && !value.trim().is_empty() {
                            env::set_var(env_constants::USERNAME, value.trim());
                        }
                        if key.trim() == env_constants::PASSWORD && !value.trim().is_empty() {
                            env::set_var(env_constants::PASSWORD, value.trim());
                        }
                    }
                }
            }
        }

        // Check credentials
        let username = env::var(env_constants::USERNAME)
            .expect("CEDA_USERNAME environment variable not set. Please set credentials.");
        let _password = env::var(env_constants::PASSWORD)
            .expect("CEDA_PASSWORD environment variable not set. Please set credentials.");

        println!("🔐 Testing worker integration with user: {}", username);

        // Create temporary directory for cache
        let temp_dir = TempDir::new().unwrap();
        println!(
            "📁 Using temporary cache directory: {}",
            temp_dir.path().display()
        );

        // Setup shared components
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        println!("🗄️  Initializing cache manager...");
        let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());

        println!("🌐 Creating authenticated CEDA client...");
        let client = Arc::new(
            CedaClient::new()
                .await
                .expect("Failed to create CEDA client"),
        );

        println!("📋 Initializing work queue...");
        let queue = Arc::new(WorkQueue::new());

        // Load a few files from the example manifest
        let manifest_path = Path::new("examples/midas-open-v202407-md5s.txt");
        if !manifest_path.exists() {
            panic!(
                "Example manifest file not found at: {}",
                manifest_path.display()
            );
        }

        println!("📄 Reading manifest file: {}", manifest_path.display());
        let manifest_config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(manifest_config);
        let mut stream = streamer.stream(manifest_path).await.unwrap();

        // Add first 3 files to queue for testing
        let mut test_files = Vec::new();
        let mut file_count = 0;
        while let Some(result) = stream.next().await {
            if file_count >= 3 {
                break;
            } // Limit to 3 files for integration test

            match result {
                Ok(file_info) => {
                    println!(
                        "📋 Adding to queue: {} ({})",
                        file_info.file_name, file_info.hash
                    );
                    queue.add_work(file_info.clone()).await.unwrap();
                    test_files.push(file_info);
                    file_count += 1;
                }
                Err(e) => {
                    eprintln!("❌ Error parsing manifest line: {}", e);
                    continue;
                }
            }
        }

        println!("📊 Added {} files to work queue", test_files.len());
        assert!(!test_files.is_empty(), "No files loaded from manifest");

        // Create worker pool with 2 workers for testing
        let worker_config = WorkerConfig {
            worker_count: 2,
            max_retries: 2,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(5),
            idle_sleep_duration: Duration::from_millis(50),
            progress_buffer_size: 10,
            download_timeout: Duration::from_secs(60), // Generous timeout for real downloads
            detailed_progress: true,
        };

        println!(
            "👷 Creating worker pool with {} workers",
            worker_config.worker_count
        );
        let mut pool = WorkerPool::new(worker_config, queue.clone(), cache.clone(), client);

        // Setup progress monitoring
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);

        // Start workers
        println!("🚀 Starting worker pool...");
        pool.start(progress_tx).await.unwrap();

        // Monitor progress with timeout
        let queue_clone = queue.clone();
        let progress_task = tokio::spawn(async move {
            let mut progress_updates = Vec::new();
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(180); // 3 minute timeout for all downloads

            while start.elapsed() < timeout {
                tokio::select! {
                    progress = progress_rx.recv() => {
                        match progress {
                            Some(update) => {
                                println!(
                                    "📈 Worker {}: {:?} status, {} files completed, {} total bytes",
                                    update.worker_id,
                                    update.status,
                                    update.files_completed,
                                    update.total_bytes_downloaded
                                );
                                progress_updates.push(update);
                            }
                            None => {
                                println!("📈 Progress channel closed");
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Check if all work is done
                        if queue_clone.is_finished().await {
                            println!("✅ All work completed!");
                            break;
                        }
                    }
                }
            }

            if start.elapsed() >= timeout {
                println!("⚠️  Test timeout reached after 3 minutes");
            }

            progress_updates
        });

        // Wait for progress monitoring to complete
        let progress_updates = progress_task.await.unwrap();

        // Shutdown worker pool
        println!("🛑 Shutting down worker pool...");
        pool.shutdown().await.unwrap();

        // Verify results
        println!("🔍 Verifying integration test results...");

        // Check progress updates
        assert!(!progress_updates.is_empty(), "No progress updates received");
        println!("📊 Received {} progress updates", progress_updates.len());

        // Check queue statistics
        let queue_stats = queue.stats().await;
        println!(
            "📋 Queue stats: {} completed, {} failed, {} pending",
            queue_stats.completed_count, queue_stats.failed_count, queue_stats.pending_count
        );

        // Verify at least some files were processed
        assert!(
            queue_stats.completed_count > 0 || queue_stats.failed_count > 0,
            "No files were processed by workers"
        );

        // Check cache for completed files
        let mut cached_files = 0;
        let mut verified_files = 0;

        for file_info in &test_files {
            let cache_path = cache.get_file_path(file_info);
            if cache_path.exists() {
                cached_files += 1;
                println!("📁 Found cached file: {}", cache_path.display());

                // Verify file integrity
                match cache.verify_cache_integrity(vec![file_info.clone()]).await {
                    Ok(report) if report.files_verified > 0 => {
                        verified_files += 1;
                        println!("✅ Cache verification passed for: {}", file_info.file_name);
                    }
                    Ok(_) => {
                        println!("⚠️  Cache verification failed for: {}", file_info.file_name);
                    }
                    Err(e) => {
                        println!(
                            "❌ Cache verification error for {}: {}",
                            file_info.file_name, e
                        );
                    }
                }
            }
        }

        println!("📊 Integration test summary:");
        println!(
            "   Files processed: {}",
            queue_stats.completed_count + queue_stats.failed_count
        );
        println!("   Files cached: {}", cached_files);
        println!("   Files verified: {}", verified_files);
        println!("   Progress updates: {}", progress_updates.len());

        // Test assertions
        assert!(cached_files > 0, "No files were successfully cached");
        assert!(verified_files > 0, "No files passed cache verification");

        // Verify worker status progression
        let worker_statuses: std::collections::HashSet<_> = progress_updates
            .iter()
            .map(|p| std::mem::discriminant(&p.status))
            .collect();

        assert!(
            worker_statuses.len() > 1,
            "Workers should progress through multiple states"
        );

        println!("🎉 Worker integration test completed successfully!");
        println!("   ✅ Workers successfully claimed work from queue");
        println!("   ✅ Cache reservations and atomic operations working");
        println!("   ✅ Real CEDA downloads completed with authentication");
        println!("   ✅ Progress reporting system functional");
        println!("   ✅ Worker pool coordination and shutdown successful");
        println!("   ✅ File integrity verified through cache system");
    }
}
