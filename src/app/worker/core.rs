//! Core download worker implementation
//!
//! This module contains the main DownloadWorker that integrates the queue, cache,
//! and client components into a cohesive work-stealing download system. Workers
//! continuously seek work from the queue, attempt cache reservations, and download
//! files without ever waiting for specific files (preventing worker starvation).

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use super::config::WorkerConfig;
use super::stats::{BackoffCalculator, SpeedCalculator};
use super::types::{WorkerProgress, WorkerResult, WorkerStats, WorkerStatus};
use crate::app::cache::{CacheManager, ReservationStatus};
use crate::app::client::CedaClient;
use crate::app::models::FileInfo;
use crate::app::queue::WorkQueue;
use crate::constants::ceda;
use crate::errors::DownloadError;

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
    pub async fn run(mut self) -> WorkerResult<()> {
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
                        self.handle_no_work_available().await;
                    } else {
                        // Found work, reset backoff counter
                        if self.stats.consecutive_empty_polls > 0 {
                            debug!(
                                "Worker {} found work after {} empty polls",
                                self.id, self.stats.consecutive_empty_polls
                            );
                            self.stats.reset_empty_polls();
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

                    // Sleep on errors to avoid tight error loops
                    tokio::time::sleep(self.config.error_sleep_duration).await;
                }
            }
        }

        self.report_progress(WorkerStatus::Shutdown, None).await;
        info!("Worker {} shutting down", self.id);
        Ok(())
    }

    /// Handle the case when no work is available
    async fn handle_no_work_available(&mut self) {
        // Track consecutive empty polls for backoff calculation
        self.stats.record_empty_poll();

        // Calculate exponential backoff using configurable parameters
        let sleep_duration = BackoffCalculator::calculate_backoff_duration(
            self.stats.consecutive_empty_polls,
            self.config.idle_sleep_duration,
            self.config.max_backoff_multiplier,
            self.config.max_idle_sleep_ms,
            self.config.backoff_jitter_percentage,
        );

        debug!(
            "Worker {} idle (attempt {}), sleeping for {:?}",
            self.id, self.stats.consecutive_empty_polls, sleep_duration
        );
        tokio::time::sleep(sleep_duration).await;
    }

    /// Perform one iteration of the worker loop
    async fn worker_iteration(&mut self) -> WorkerResult<bool> {
        // 1. Request work from queue
        self.report_progress(WorkerStatus::RequestingWork, None)
            .await;

        let work_request_start = Instant::now();
        let work_info = match self.queue.get_next_work().await {
            Some(work) => {
                let queue_wait_time = work_request_start.elapsed();
                if queue_wait_time.as_millis() > self.config.queue_wait_log_threshold_ms as u128 {
                    debug!(
                        "Worker {} waited {:?} for work from queue",
                        self.id, queue_wait_time
                    );
                }
                work
            }
            None => {
                let queue_wait_time = work_request_start.elapsed();
                if queue_wait_time.as_millis()
                    > self.config.short_queue_wait_log_threshold_ms as u128
                {
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
    async fn download_and_save_file(&mut self, file_info: &FileInfo) -> WorkerResult<()> {
        let mut retry_count = 0;
        let mut retry_delay = self.config.retry_base_delay;

        loop {
            // Start download
            self.stats.reset_current_download();

            self.report_progress(WorkerStatus::Downloading, None).await;

            match self.attempt_download(file_info).await {
                Ok(content) => {
                    // Download successful, now save atomically
                    self.report_progress(WorkerStatus::Saving, None).await;

                    match self.cache.save_file_atomic(&content, file_info).await {
                        Ok(()) => {
                            // Success! Update statistics
                            self.stats.complete_download(content.len() as u64);
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

                    // Check if this is a permanent error (404, 403, etc.) that shouldn't be retried
                    match &e {
                        DownloadError::NotFound { .. } | DownloadError::Forbidden { .. } => {
                            // Release the reservation on permanent failure
                            let _ = self.cache.release_reservation(&file_info.hash).await;
                            return Err(e);
                        }
                        _ => {
                            // For retryable errors, continue with retry logic
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
                }
            }

            // Wait before retry with exponential backoff
            debug!(
                "Worker {} retrying in {:?} (attempt {})",
                self.id, retry_delay, retry_count
            );
            tokio::time::sleep(retry_delay).await;

            // Exponential backoff using configurable multiplier
            retry_delay = BackoffCalculator::calculate_retry_delay(
                retry_count,
                self.config.retry_base_delay,
                self.config.retry_max_delay,
                self.config.retry_backoff_multiplier,
            );
        }
    }

    /// Attempt to download a file
    async fn attempt_download(&mut self, file_info: &FileInfo) -> WorkerResult<Vec<u8>> {
        let url = file_info.download_url(ceda::BASE_URL);
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
            timestamp: chrono::Utc::now(),
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

    /// Calculate current download speed using configurable parameters
    fn calculate_download_speed(&self) -> f64 {
        SpeedCalculator::calculate_speed(
            &self.stats.speed_samples,
            self.config.speed_calculation_samples,
        )
    }

    /// Get worker ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get worker configuration
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Get current worker statistics (for testing)
    #[cfg(test)]
    pub(crate) fn stats(&self) -> &WorkerStats {
        &self.stats
    }
}

/// Builder for creating DownloadWorker instances with validation
#[derive(Debug)]
pub struct DownloadWorkerBuilder {
    id: Option<u32>,
    config: Option<WorkerConfig>,
    queue: Option<Arc<WorkQueue>>,
    cache: Option<Arc<CacheManager>>,
    client: Option<Arc<CedaClient>>,
    progress_tx: Option<mpsc::Sender<WorkerProgress>>,
    shutdown_rx: Option<mpsc::Receiver<()>>,
}

impl DownloadWorkerBuilder {
    /// Create a new worker builder
    pub fn new() -> Self {
        Self {
            id: None,
            config: None,
            queue: None,
            cache: None,
            client: None,
            progress_tx: None,
            shutdown_rx: None,
        }
    }

    /// Set worker ID
    pub fn id(mut self, id: u32) -> Self {
        self.id = Some(id);
        self
    }

    /// Set worker configuration
    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set work queue
    pub fn queue(mut self, queue: Arc<WorkQueue>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set cache manager
    pub fn cache(mut self, cache: Arc<CacheManager>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Set CEDA client
    pub fn client(mut self, client: Arc<CedaClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Set progress channel
    pub fn progress_channel(mut self, progress_tx: mpsc::Sender<WorkerProgress>) -> Self {
        self.progress_tx = Some(progress_tx);
        self
    }

    /// Set shutdown receiver
    pub fn shutdown_receiver(mut self, shutdown_rx: mpsc::Receiver<()>) -> Self {
        self.shutdown_rx = Some(shutdown_rx);
        self
    }

    /// Build the worker (validates all required fields are set)
    pub fn build(self) -> WorkerResult<DownloadWorker> {
        let id = self.id.ok_or_else(|| {
            DownloadError::ConfigurationError("Worker ID is required".to_string())
        })?;

        let config = self.config.ok_or_else(|| {
            DownloadError::ConfigurationError("Worker configuration is required".to_string())
        })?;

        let queue = self.queue.ok_or_else(|| {
            DownloadError::ConfigurationError("Work queue is required".to_string())
        })?;

        let cache = self.cache.ok_or_else(|| {
            DownloadError::ConfigurationError("Cache manager is required".to_string())
        })?;

        let client = self.client.ok_or_else(|| {
            DownloadError::ConfigurationError("CEDA client is required".to_string())
        })?;

        let progress_tx = self.progress_tx.ok_or_else(|| {
            DownloadError::ConfigurationError("Progress channel is required".to_string())
        })?;

        let shutdown_rx = self.shutdown_rx.ok_or_else(|| {
            DownloadError::ConfigurationError("Shutdown receiver is required".to_string())
        })?;

        // Validate configuration
        config.validate()?;

        Ok(DownloadWorker::new(
            id,
            config,
            queue,
            cache,
            client,
            progress_tx,
            shutdown_rx,
        ))
    }
}

impl Default for DownloadWorkerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::cache::CacheConfig;
    use crate::app::client::ClientConfig;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    async fn create_test_components(
    ) -> (Arc<WorkQueue>, Arc<CacheManager>, Arc<CedaClient>, TempDir) {
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

        (queue, cache, client, temp_dir)
    }

    fn create_test_config() -> WorkerConfig {
        super::super::config::ConfigPresets::testing()
    }

    /// Test DownloadWorker creation and basic properties
    ///
    /// Verifies that workers can be created with valid configuration
    /// and provide access to their properties for monitoring.
    #[tokio::test]
    async fn test_worker_creation() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = create_test_config();
        let (progress_tx, _progress_rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let worker = DownloadWorker::new(
            1,
            config.clone(),
            queue,
            cache,
            client,
            progress_tx,
            shutdown_rx,
        );

        assert_eq!(worker.id(), 1);
        assert_eq!(worker.config().worker_count, config.worker_count);
    }

    /// Test DownloadWorkerBuilder validation and construction
    ///
    /// Ensures that the builder pattern correctly validates required
    /// fields and produces properly configured workers.
    #[tokio::test]
    async fn test_worker_builder() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = create_test_config();
        let (progress_tx, _progress_rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Test successful build
        let worker = DownloadWorkerBuilder::new()
            .id(2)
            .config(config.clone())
            .queue(queue.clone())
            .cache(cache.clone())
            .client(client.clone())
            .progress_channel(progress_tx.clone())
            .shutdown_receiver(shutdown_rx)
            .build()
            .unwrap();

        assert_eq!(worker.id(), 2);

        // Test missing required field
        let (_shutdown_tx2, shutdown_rx2) = mpsc::channel(1);
        let result = DownloadWorkerBuilder::new()
            .config(config)
            .queue(queue)
            .cache(cache)
            .client(client)
            .progress_channel(progress_tx)
            .shutdown_receiver(shutdown_rx2)
            .build(); // Missing ID

        assert!(result.is_err());
    }

    /// Test shutdown signal handling
    ///
    /// Verifies that workers properly detect and respond to shutdown
    /// signals without blocking the shutdown process.
    #[tokio::test]
    async fn test_shutdown_signal() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = create_test_config();
        let (progress_tx, _progress_rx) = mpsc::channel(10);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let worker = DownloadWorker::new(3, config, queue, cache, client, progress_tx, shutdown_rx);

        // Initially should not be shutdown
        assert!(!worker.check_shutdown().await);

        // Send shutdown signal
        shutdown_tx.send(()).await.unwrap();

        // Should now detect shutdown
        assert!(worker.check_shutdown().await);

        // Subsequent checks should also return true
        assert!(worker.check_shutdown().await);
    }

    /// Test backoff calculation with worker configuration
    ///
    /// Ensures that workers use the configured backoff parameters
    /// correctly and produce expected delay patterns.
    #[test]
    fn test_worker_backoff_calculation() {
        let config = create_test_config();

        // Test backoff with config parameters
        let backoff = BackoffCalculator::calculate_backoff_duration(
            1,
            config.idle_sleep_duration,
            config.max_backoff_multiplier,
            config.max_idle_sleep_ms,
            config.backoff_jitter_percentage,
        );

        // Should be at least the base duration
        assert!(backoff >= config.idle_sleep_duration);

        // Test maximum backoff
        let max_backoff = BackoffCalculator::calculate_backoff_duration(
            10,
            config.idle_sleep_duration,
            config.max_backoff_multiplier,
            config.max_idle_sleep_ms,
            0.0, // No jitter for predictable testing
        );

        assert!(max_backoff.as_millis() <= config.max_idle_sleep_ms as u128);
    }

    /// Test worker statistics tracking
    ///
    /// Verifies that workers correctly maintain internal statistics
    /// for files completed, bytes downloaded, and polling behavior.
    #[tokio::test]
    async fn test_worker_statistics() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = create_test_config();
        let (progress_tx, _progress_rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let mut worker =
            DownloadWorker::new(4, config, queue, cache, client, progress_tx, shutdown_rx);

        // Test initial statistics
        assert_eq!(worker.stats().files_completed, 0);
        assert_eq!(worker.stats().total_bytes_downloaded, 0);
        assert_eq!(worker.stats().consecutive_empty_polls, 0);

        // Test empty poll tracking
        worker.stats.record_empty_poll();
        assert_eq!(worker.stats().consecutive_empty_polls, 1);

        worker.stats.record_empty_poll();
        assert_eq!(worker.stats().consecutive_empty_polls, 2);

        worker.stats.reset_empty_polls();
        assert_eq!(worker.stats().consecutive_empty_polls, 0);

        // Test download completion
        worker.stats.complete_download(1024);
        assert_eq!(worker.stats().files_completed, 1);
        assert_eq!(worker.stats().total_bytes_downloaded, 1024);
    }

    /// Test speed calculation integration
    ///
    /// Ensures that workers correctly integrate with the speed calculation
    /// system and use configured parameters for sample management.
    #[test]
    fn test_worker_speed_calculation() {
        let config = create_test_config();

        // Test with configured sample size
        let speed = SpeedCalculator::calculate_speed(
            &[(std::time::Instant::now(), 0)],
            config.speed_calculation_samples,
        );
        assert_eq!(speed, 0.0); // Insufficient samples

        // Test with multiple samples
        let now = std::time::Instant::now();
        let samples = vec![(now, 0), (now + std::time::Duration::from_secs(1), 1024)];
        let speed = SpeedCalculator::calculate_speed(&samples, config.speed_calculation_samples);
        assert!(speed > 0.0);
    }

    /// Test progress reporting functionality
    ///
    /// Verifies that workers correctly format and send progress reports
    /// through the configured channels without blocking operations.
    #[tokio::test]
    async fn test_progress_reporting() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = create_test_config();
        let (progress_tx, mut progress_rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let worker = DownloadWorker::new(5, config, queue, cache, client, progress_tx, shutdown_rx);

        // Test progress reporting
        worker
            .report_progress(WorkerStatus::Downloading, None)
            .await;

        // Should receive progress report
        let progress = progress_rx.recv().await.unwrap();
        assert_eq!(progress.worker_id, 5);
        assert_eq!(progress.status, WorkerStatus::Downloading);
    }
}
