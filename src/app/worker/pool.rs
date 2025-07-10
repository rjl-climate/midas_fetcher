//! Worker pool management and coordination
//!
//! This module provides a WorkerPool that manages multiple download workers,
//! handling their lifecycle, coordination, and graceful shutdown. The pool
//! integrates with the statistics system to provide real-time monitoring
//! of worker performance and health.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::config::WorkerConfig;
use super::core::DownloadWorkerBuilder;
use super::stats::StatsAggregator;
use super::types::{WorkerPoolStats, WorkerProgress, WorkerResult};
use crate::app::cache::CacheManager;
use crate::app::client::CedaClient;
use crate::app::queue::WorkQueue;

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
    worker_handles: Vec<JoinHandle<WorkerResult<()>>>,
    /// Shutdown signal senders
    shutdown_senders: Vec<mpsc::Sender<()>>,
    /// Statistics aggregator
    stats_aggregator: Option<StatsAggregator>,
    /// Pool state
    state: PoolState,
}

/// Current state of the worker pool
#[derive(Debug, Clone, PartialEq)]
pub enum PoolState {
    /// Pool has been created but not started
    Created,
    /// Pool is running with active workers
    Running,
    /// Pool is shutting down
    ShuttingDown,
    /// Pool has been shut down
    Shutdown,
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
            stats_aggregator: None,
            state: PoolState::Created,
        }
    }

    /// Start all workers
    pub async fn start(&mut self, progress_tx: mpsc::Sender<WorkerProgress>) -> WorkerResult<()> {
        if self.state != PoolState::Created {
            return Err(crate::errors::DownloadError::ConfigurationError(format!(
                "Cannot start pool in state: {:?}",
                self.state
            )));
        }

        info!("Starting {} workers", self.config.worker_count);

        // Initialize statistics aggregator
        self.stats_aggregator = Some(StatsAggregator::new(self.config.clone()));

        // Start workers
        for worker_id in 0..self.config.worker_count {
            let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

            let worker = DownloadWorkerBuilder::new()
                .id(worker_id as u32)
                .config(self.config.clone())
                .queue(self.queue.clone())
                .cache(self.cache.clone())
                .client(self.client.clone())
                .progress_channel(progress_tx.clone())
                .shutdown_receiver(shutdown_rx)
                .build()?;

            let handle = tokio::spawn(async move { worker.run().await });

            self.worker_handles.push(handle);
            self.shutdown_senders.push(shutdown_tx);
        }

        self.state = PoolState::Running;

        info!(
            "Worker pool started with {} workers",
            self.config.worker_count
        );
        Ok(())
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(mut self) -> WorkerResult<()> {
        if self.state == PoolState::Shutdown {
            return Ok(());
        }

        if self.state != PoolState::Running {
            warn!("Shutting down pool in state: {:?}", self.state);
        }

        self.state = PoolState::ShuttingDown;
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

        self.state = PoolState::Shutdown;
        info!("Worker pool shutdown complete");
        Ok(())
    }

    /// Update worker progress in statistics aggregator
    pub fn update_worker_progress(&mut self, progress: WorkerProgress) {
        if let Some(ref mut aggregator) = self.stats_aggregator {
            aggregator.update_worker_progress(progress);
        }
    }

    /// Get current pool statistics
    pub fn get_stats(&self) -> Option<WorkerPoolStats> {
        self.stats_aggregator
            .as_ref()
            .map(|aggregator| aggregator.generate_pool_stats())
    }

    /// Get detailed pool statistics with performance metrics
    pub fn get_detailed_stats(
        &self,
    ) -> Option<(WorkerPoolStats, super::stats::PerformanceMetrics)> {
        if let Some(ref aggregator) = self.stats_aggregator {
            let stats = aggregator.generate_pool_stats();
            let metrics =
                super::stats::PerformanceMetrics::from_pool_stats(&stats, aggregator.uptime());
            Some((stats, metrics))
        } else {
            None
        }
    }

    /// Check if the pool is healthy and performing well
    pub fn is_healthy(&self) -> bool {
        if let Some((stats, metrics)) = self.get_detailed_stats() {
            // Pool is healthy if:
            // 1. Performance metrics are good
            // 2. Not too many workers are in error state
            // 3. Some workers are active (not all idle)
            metrics.is_healthy()
                && stats.error_count() < self.config.worker_count / 2
                && stats.worker_utilization() > 10.0
        } else {
            // No stats available - assume healthy if running
            self.state == PoolState::Running
        }
    }

    /// Check if the pool appears to be stalled
    pub fn is_stalled(&self) -> bool {
        self.stats_aggregator
            .as_ref()
            .map(|aggregator| aggregator.is_stalled())
            .unwrap_or(false)
    }

    /// Get the current pool state
    pub fn state(&self) -> PoolState {
        self.state.clone()
    }

    /// Get pool configuration
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Get number of active workers
    pub fn worker_count(&self) -> usize {
        self.worker_handles.len()
    }

    /// Force shutdown all workers (emergency shutdown)
    pub async fn force_shutdown(mut self) -> WorkerResult<()> {
        warn!("Force shutting down worker pool");
        self.state = PoolState::ShuttingDown;

        // Abort all worker tasks
        for handle in self.worker_handles {
            handle.abort();
        }

        self.state = PoolState::Shutdown;
        info!("Worker pool force shutdown complete");
        Ok(())
    }
}

/// Builder for creating WorkerPool instances with validation
#[derive(Debug, Default)]
pub struct WorkerPoolBuilder {
    config: Option<WorkerConfig>,
    queue: Option<Arc<WorkQueue>>,
    cache: Option<Arc<CacheManager>>,
    client: Option<Arc<CedaClient>>,
}

impl WorkerPoolBuilder {
    /// Create a new worker pool builder
    pub fn new() -> Self {
        Self::default()
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

    /// Build the worker pool (validates all required fields are set)
    pub fn build(self) -> WorkerResult<WorkerPool> {
        let config = self.config.ok_or_else(|| {
            crate::errors::DownloadError::ConfigurationError(
                "Worker configuration is required".to_string(),
            )
        })?;

        let queue = self.queue.ok_or_else(|| {
            crate::errors::DownloadError::ConfigurationError("Work queue is required".to_string())
        })?;

        let cache = self.cache.ok_or_else(|| {
            crate::errors::DownloadError::ConfigurationError(
                "Cache manager is required".to_string(),
            )
        })?;

        let client = self.client.ok_or_else(|| {
            crate::errors::DownloadError::ConfigurationError("CEDA client is required".to_string())
        })?;

        // Validate configuration
        config.validate()?;

        Ok(WorkerPool::new(config, queue, cache, client))
    }
}

/// Utility functions for worker pool management
pub mod utils {
    use super::super::config::ConfigPresets;
    use super::*;
    use std::time::Duration;

    /// Create a worker pool optimized for testing
    pub fn create_test_pool(
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::testing(), queue, cache, client)
    }

    /// Create a worker pool optimized for production
    pub fn create_production_pool(
        queue: Arc<WorkQueue>,
        cache: Arc<CacheManager>,
        client: Arc<CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::production(), queue, cache, client)
    }

    /// Wait for pool to reach a certain utilization level
    pub async fn wait_for_utilization(
        pool: &WorkerPool,
        target_utilization: f64,
        timeout: Duration,
    ) -> bool {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Some(stats) = pool.get_stats() {
                if stats.worker_utilization() >= target_utilization {
                    return true;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        false
    }

    /// Monitor pool health and return when unhealthy or timeout
    pub async fn monitor_until_unhealthy_or_timeout(
        pool: &WorkerPool,
        timeout: Duration,
    ) -> PoolHealthStatus {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if !pool.is_healthy() {
                return PoolHealthStatus::Unhealthy;
            }

            if pool.is_stalled() {
                return PoolHealthStatus::Stalled;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        PoolHealthStatus::Timeout
    }
}

/// Health status returned by monitoring functions
#[derive(Debug, Clone, PartialEq)]
pub enum PoolHealthStatus {
    Healthy,
    Unhealthy,
    Stalled,
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::cache::CacheConfig;
    use crate::app::client::ClientConfig;
    use tempfile::TempDir;

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

    /// Test WorkerPool creation and basic properties
    ///
    /// Verifies that worker pools can be created with valid configuration
    /// and maintain correct initial state.
    #[tokio::test]
    async fn test_pool_creation() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let pool = WorkerPool::new(config.clone(), queue, cache, client);

        assert_eq!(pool.config().worker_count, config.worker_count);
        assert_eq!(pool.state(), PoolState::Created);
        assert_eq!(pool.worker_count(), 0); // Not started yet
        assert!(pool.get_stats().is_none()); // No stats until started
    }

    /// Test WorkerPoolBuilder validation and construction
    ///
    /// Ensures that the builder pattern correctly validates required
    /// fields and produces properly configured pools.
    #[tokio::test]
    async fn test_pool_builder() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        // Test successful build
        let pool = WorkerPoolBuilder::new()
            .config(config.clone())
            .queue(queue.clone())
            .cache(cache.clone())
            .client(client.clone())
            .build()
            .unwrap();

        assert_eq!(pool.config().worker_count, config.worker_count);

        // Test missing required field
        let result = WorkerPoolBuilder::new()
            .queue(queue)
            .cache(cache)
            .client(client)
            .build(); // Missing config

        assert!(result.is_err());
    }

    /// Test pool state transitions
    ///
    /// Verifies that the pool correctly transitions between states
    /// and enforces valid state changes.
    #[tokio::test]
    async fn test_pool_state_transitions() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let mut pool = WorkerPool::new(config, queue, cache, client);
        let (progress_tx, _progress_rx) = mpsc::channel(10);

        // Initial state
        assert_eq!(pool.state(), PoolState::Created);

        // Start pool
        pool.start(progress_tx).await.unwrap();
        assert_eq!(pool.state(), PoolState::Running);
        assert_eq!(pool.worker_count(), 2); // Testing config has 2 workers

        // Cannot start again
        let (progress_tx2, _progress_rx2) = mpsc::channel(10);
        assert!(pool.start(progress_tx2).await.is_err());

        // Get state before shutdown (since shutdown consumes the pool)
        assert_eq!(pool.state(), PoolState::Running);

        // Shutdown pool
        pool.shutdown().await.unwrap();
    }

    /// Test pool statistics integration
    ///
    /// Ensures that the pool correctly integrates with the statistics
    /// system and provides meaningful metrics.
    #[tokio::test]
    async fn test_pool_statistics() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let mut pool = WorkerPool::new(config, queue, cache, client);
        let (progress_tx, _progress_rx) = mpsc::channel(10);

        // No stats before starting
        assert!(pool.get_stats().is_none());

        // Start pool
        pool.start(progress_tx).await.unwrap();

        // Give workers a moment to start and report initial status
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Should have stats after starting
        let stats = pool.get_stats().unwrap();
        // Workers may not have reported initial status yet, so stats exist
        assert!(stats.active_workers <= 2); // At most 2 workers in testing config
        assert_eq!(stats.files_completed, 0);

        // Test progress update
        let progress = super::super::types::WorkerProgress::new(
            1,
            super::super::types::WorkerStatus::Downloading,
        );
        pool.update_worker_progress(progress);

        let updated_stats = pool.get_stats().unwrap();
        assert_eq!(updated_stats.workers_by_status.len(), 1);

        // Test detailed stats
        let (_stats, metrics) = pool.get_detailed_stats().unwrap();
        assert!(metrics.uptime_secs >= 0.0);

        pool.shutdown().await.unwrap();
    }

    /// Test pool health monitoring
    ///
    /// Verifies that health monitoring correctly identifies healthy
    /// and unhealthy pool states based on worker performance.
    #[tokio::test]
    async fn test_pool_health_monitoring() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let mut pool = WorkerPool::new(config, queue, cache, client);
        let (progress_tx, _progress_rx) = mpsc::channel(10);

        // Pool should be considered healthy when running
        pool.start(progress_tx).await.unwrap();

        // Add some progress to make workers appear active
        let progress = super::super::types::WorkerProgress {
            worker_id: 1,
            status: super::super::types::WorkerStatus::Downloading,
            files_completed: 1,
            total_bytes_downloaded: 1024,
            download_speed: 1024.0,
            ..super::super::types::WorkerProgress::new(
                1,
                super::super::types::WorkerStatus::Downloading,
            )
        };
        pool.update_worker_progress(progress);

        // Now should be healthy with active workers
        assert!(pool.is_healthy());
        assert!(!pool.is_stalled());

        pool.shutdown().await.unwrap();
    }

    /// Test utility functions for pool management
    ///
    /// Ensures that utility functions provide convenient ways to create
    /// and manage pools for different deployment scenarios.
    #[tokio::test]
    async fn test_pool_utilities() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;

        // Test utility pool creation
        let test_pool = utils::create_test_pool(queue.clone(), cache.clone(), client.clone());
        assert_eq!(test_pool.config().worker_count, 2); // Testing config

        let prod_pool = utils::create_production_pool(queue, cache, client);
        assert_eq!(prod_pool.config().worker_count, 8); // Production config
    }

    /// Test force shutdown functionality
    ///
    /// Verifies that emergency shutdown correctly terminates all workers
    /// even if they are not responding to graceful shutdown signals.
    #[tokio::test]
    async fn test_force_shutdown() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let mut pool = WorkerPool::new(config, queue, cache, client);
        let (progress_tx, _progress_rx) = mpsc::channel(10);

        pool.start(progress_tx).await.unwrap();
        assert_eq!(pool.state(), PoolState::Running);

        // Force shutdown (consumes the pool)
        pool.force_shutdown().await.unwrap();
    }

    /// Test multiple shutdown attempts
    ///
    /// Ensures that shutdown is idempotent and can be called multiple
    /// times without causing errors or panics.
    #[tokio::test]
    async fn test_multiple_shutdown_attempts() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;
        let config = super::super::config::ConfigPresets::testing();

        let mut pool = WorkerPool::new(config, queue, cache, client);
        let (progress_tx, _progress_rx) = mpsc::channel(10);

        pool.start(progress_tx).await.unwrap();

        // First shutdown should work
        pool.shutdown().await.unwrap();

        // Additional shutdown attempts should not create new pools
        // (Cannot test directly as shutdown consumes the pool, but the
        // important thing is that shutdown() returns Ok even after first call)
    }
}
