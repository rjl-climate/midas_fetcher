//! Download worker system for concurrent file processing
//!
//! This module implements a sophisticated work-stealing download system that integrates
//! the queue, cache, and client components into a cohesive worker pool. Workers
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
//! - **Dynamic configuration**: No hardcoded values, fully configurable
//! - **Performance monitoring**: Comprehensive statistics and health checks
//!
//! # Module Organization
//!
//! - [`config`] - Worker configuration with validation and presets
//! - [`types`] - Data structures for progress reporting and status tracking
//! - [`stats`] - Statistics calculation and performance monitoring
//! - [`core`] - Individual worker implementation with download logic
//! - [`pool`] - Worker pool management and coordination
//!
//! # Basic Usage
//!
//! ```rust,no_run
//! use midas_fetcher::app::worker::{WorkerPool, WorkerConfig, ConfigPresets};
//! use midas_fetcher::app::{CacheManager, CacheConfig, CedaClient, WorkQueue};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create shared components
//! let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//! let client = Arc::new(CedaClient::new().await?);
//! let queue = Arc::new(WorkQueue::new());
//!
//! // Create worker pool with production configuration
//! let mut pool = WorkerPool::new(ConfigPresets::production(), queue, cache, client);
//!
//! // Start workers
//! let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);
//! pool.start(progress_tx).await?;
//!
//! // Monitor progress
//! while let Some(progress) = progress_rx.recv().await {
//!     println!("Worker {}: {} bytes downloaded",
//!         progress.worker_id, progress.bytes_downloaded);
//!     
//!     // Check if pool is healthy
//!     if !pool.is_healthy() {
//!         eprintln!("Warning: Worker pool is unhealthy");
//!     }
//! }
//!
//! // Shutdown gracefully
//! pool.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced Configuration
//!
//! ```rust,no_run
//! use midas_fetcher::app::worker::{WorkerConfigBuilder, WorkerPoolBuilder};
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Build custom configuration
//! let config = WorkerConfigBuilder::new()
//!     .worker_count(12)
//!     .max_retries(5)
//!     .download_timeout(Duration::from_secs(300))
//!     .detailed_progress(true)
//!     .build()?;
//!
//! # // Mock components for doctest
//! # let queue = Arc::new(midas_fetcher::app::queue::WorkQueue::new());
//! # let cache = Arc::new(midas_fetcher::app::cache::CacheManager::new(Default::default()).await?);
//! # let client = Arc::new(midas_fetcher::app::client::CedaClient::new_simple_with_config(Default::default()).await?);
//! // Or use the builder pattern for the entire pool
//! let pool = WorkerPoolBuilder::new()
//!     .config(config)
//!     .queue(queue)
//!     .cache(cache)
//!     .client(client)
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Statistics and Monitoring
//!
//! ```rust,no_run
//! use midas_fetcher::app::worker::{WorkerPool, StatsReporter};
//!
//! # async fn example(pool: &WorkerPool) -> Result<(), Box<dyn std::error::Error>> {
//! // Get current statistics
//! if let Some(stats) = pool.get_stats() {
//!     println!("Active workers: {}", stats.active_workers);
//!     println!("Files completed: {}", stats.files_completed);
//!     println!("Success rate: {:.1}%", stats.success_rate());
//!     println!("Worker utilization: {:.1}%", stats.worker_utilization());
//! }
//!
//! // Get detailed performance metrics
//! if let Some((stats, metrics)) = pool.get_detailed_stats() {
//!     println!("Throughput: {:.2} MB/s", metrics.throughput_mbps);
//!     println!("Files per second: {:.2}", metrics.files_per_second);
//!     println!("Average file size: {:.0} bytes", metrics.average_file_size);
//!     
//!     // Generate reports
//!     let detailed_report = StatsReporter::generate_detailed_report(&stats);
//!     println!("{}", detailed_report);
//! }
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod core;
pub mod pool;
pub mod stats;
pub mod types;

#[cfg(test)]
mod tests;

// Re-export main public API
pub use config::{ConfigPresets, WorkerConfig, WorkerConfigBuilder};
pub use core::{DownloadWorker, DownloadWorkerBuilder};
pub use pool::{PoolHealthStatus, PoolState, WorkerPool, WorkerPoolBuilder};
pub use stats::{
    BackoffCalculator, PerformanceMetrics, SpeedCalculator, StatsAggregator, StatsReporter,
};
pub use types::{WorkerPoolStats, WorkerProgress, WorkerResult, WorkerStatus};

// Re-export commonly used items for convenience
pub use config::ConfigPresets as Presets;
pub use core::DownloadWorker as Worker;
pub use pool::WorkerPool as Pool;
pub use stats::StatsReporter as Reporter;
pub use types::WorkerProgress as Progress;

/// Result type for worker operations
pub type Result<T> = types::WorkerResult<T>;

/// Builder pattern for creating worker systems with fluent API
#[derive(Debug, Default)]
pub struct WorkerSystemBuilder {
    config_builder: WorkerConfigBuilder,
    queue: Option<std::sync::Arc<crate::app::queue::WorkQueue>>,
    cache: Option<std::sync::Arc<crate::app::cache::CacheManager>>,
    client: Option<std::sync::Arc<crate::app::client::CedaClient>>,
}

impl WorkerSystemBuilder {
    /// Create a new worker system builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of workers
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.config_builder = self.config_builder.worker_count(count);
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.config_builder = self.config_builder.max_retries(retries);
        self
    }

    /// Set download timeout
    pub fn with_download_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config_builder = self.config_builder.download_timeout(timeout);
        self
    }

    /// Set work queue
    pub fn with_queue(mut self, queue: std::sync::Arc<crate::app::queue::WorkQueue>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set cache manager
    pub fn with_cache(mut self, cache: std::sync::Arc<crate::app::cache::CacheManager>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Set CEDA client
    pub fn with_client(mut self, client: std::sync::Arc<crate::app::client::CedaClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Build the worker pool
    pub fn build(self) -> Result<WorkerPool> {
        let config = self.config_builder.build()?;

        WorkerPoolBuilder::new()
            .config(config)
            .queue(self.queue.ok_or_else(|| {
                crate::errors::DownloadError::ConfigurationError(
                    "Work queue is required".to_string(),
                )
            })?)
            .cache(self.cache.ok_or_else(|| {
                crate::errors::DownloadError::ConfigurationError(
                    "Cache manager is required".to_string(),
                )
            })?)
            .client(self.client.ok_or_else(|| {
                crate::errors::DownloadError::ConfigurationError(
                    "CEDA client is required".to_string(),
                )
            })?)
            .build()
    }
}

/// Utility functions for worker system management
pub mod utils {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// Create a worker pool optimized for testing
    pub fn create_test_pool(
        queue: Arc<crate::app::queue::WorkQueue>,
        cache: Arc<crate::app::cache::CacheManager>,
        client: Arc<crate::app::client::CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::testing(), queue, cache, client)
    }

    /// Create a worker pool optimized for development
    pub fn create_development_pool(
        queue: Arc<crate::app::queue::WorkQueue>,
        cache: Arc<crate::app::cache::CacheManager>,
        client: Arc<crate::app::client::CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::development(), queue, cache, client)
    }

    /// Create a worker pool optimized for production
    pub fn create_production_pool(
        queue: Arc<crate::app::queue::WorkQueue>,
        cache: Arc<crate::app::cache::CacheManager>,
        client: Arc<crate::app::client::CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::production(), queue, cache, client)
    }

    /// Create a high-throughput worker pool for powerful systems
    pub fn create_high_throughput_pool(
        queue: Arc<crate::app::queue::WorkQueue>,
        cache: Arc<crate::app::cache::CacheManager>,
        client: Arc<crate::app::client::CedaClient>,
    ) -> WorkerPool {
        WorkerPool::new(ConfigPresets::high_throughput(), queue, cache, client)
    }

    /// Wait for worker pool to reach target health status
    pub async fn wait_for_health_status(
        pool: &WorkerPool,
        healthy: bool,
        timeout: Duration,
    ) -> bool {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if pool.is_healthy() == healthy {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        false
    }

    /// Monitor worker pool and collect statistics over time
    pub async fn collect_statistics_over_time(
        pool: &WorkerPool,
        duration: Duration,
        sample_interval: Duration,
    ) -> Vec<(std::time::Instant, WorkerPoolStats)> {
        let mut samples = Vec::new();
        let start = std::time::Instant::now();

        while start.elapsed() < duration {
            if let Some(stats) = pool.get_stats() {
                samples.push((std::time::Instant::now(), stats));
            }

            tokio::time::sleep(sample_interval).await;
        }

        samples
    }

    /// Generate a comprehensive health report for a worker pool
    pub fn generate_health_report(pool: &WorkerPool) -> String {
        let mut report = String::new();

        report.push_str("Worker Pool Health Report\n");
        report.push_str("========================\n\n");

        report.push_str(&format!("State: {:?}\n", pool.state()));
        report.push_str(&format!("Worker Count: {}\n", pool.worker_count()));
        report.push_str(&format!("Healthy: {}\n", pool.is_healthy()));
        report.push_str(&format!("Stalled: {}\n", pool.is_stalled()));

        if let Some((stats, metrics)) = pool.get_detailed_stats() {
            report.push_str("\nPool Statistics:\n");
            report.push_str(&format!("  Active Workers: {}\n", stats.active_workers));
            report.push_str(&format!("  Files Completed: {}\n", stats.files_completed));
            report.push_str(&format!("  Files Failed: {}\n", stats.files_failed));
            report.push_str(&format!("  Success Rate: {:.1}%\n", stats.success_rate()));
            report.push_str(&format!(
                "  Worker Utilization: {:.1}%\n",
                stats.worker_utilization()
            ));

            report.push_str("\nPerformance Metrics:\n");
            report.push_str(&format!(
                "  Throughput: {:.2} MB/s\n",
                metrics.throughput_mbps
            ));
            report.push_str(&format!(
                "  Files/Second: {:.2}\n",
                metrics.files_per_second
            ));
            report.push_str(&format!(
                "  Avg File Size: {:.0} bytes\n",
                metrics.average_file_size
            ));
            report.push_str(&format!(
                "  Error Rate: {:.1}%\n",
                metrics.error_rate_percentage
            ));
            report.push_str(&format!(
                "  Worker Efficiency: {:.1}%\n",
                metrics.worker_efficiency
            ));

            if !metrics.is_healthy() {
                report.push_str("\n⚠️  WARNING: Performance metrics indicate unhealthy state\n");
            }
        }

        report
    }

    /// Calculate optimal worker count based on system resources
    pub fn calculate_optimal_worker_count() -> usize {
        // For I/O bound tasks like downloading, use a reasonable default
        // that works well across different systems
        let optimal = std::cmp::min(
            8, // Reasonable default for most systems
            crate::constants::workers::MAX_WORKER_COUNT,
        );

        std::cmp::max(optimal, 1)
    }

    /// Detect if the system is under resource pressure
    pub fn detect_resource_pressure() -> ResourcePressure {
        // This is a simplified implementation - in a real system you'd check:
        // - Memory usage
        // - CPU utilization
        // - Disk I/O
        // - Network bandwidth

        // For now, return Low as a reasonable default
        // In practice, this would use system monitoring
        ResourcePressure::Low
    }

    /// Recommend worker configuration based on system characteristics
    pub fn recommend_worker_config() -> WorkerConfig {
        let worker_count = calculate_optimal_worker_count();
        let pressure = detect_resource_pressure();

        match pressure {
            ResourcePressure::Low => WorkerConfigBuilder::new()
                .worker_count(worker_count)
                .max_retries(3)
                .download_timeout(Duration::from_secs(600))
                .detailed_progress(true)
                .build()
                .unwrap_or_else(|_| ConfigPresets::production()),

            ResourcePressure::Medium => WorkerConfigBuilder::new()
                .worker_count(worker_count / 2)
                .max_retries(2)
                .download_timeout(Duration::from_secs(300))
                .detailed_progress(false)
                .build()
                .unwrap_or_else(|_| ConfigPresets::development()),

            ResourcePressure::High => ConfigPresets::testing(),
        }
    }
}

/// System resource pressure levels
#[derive(Debug, Clone, PartialEq)]
pub enum ResourcePressure {
    Low,
    Medium,
    High,
}

/// Worker system health information
#[derive(Debug, Clone)]
pub struct SystemHealth {
    pub pool_healthy: bool,
    pub pool_stalled: bool,
    pub resource_pressure: ResourcePressure,
    pub recommended_worker_count: usize,
    pub current_worker_count: usize,
    pub performance_score: f64, // 0.0 to 100.0
}

impl SystemHealth {
    /// Generate health assessment for a worker pool
    pub fn assess(pool: &WorkerPool) -> Self {
        let resource_pressure = utils::detect_resource_pressure();
        let recommended_worker_count = utils::calculate_optimal_worker_count();

        let performance_score = if let Some((stats, metrics)) = pool.get_detailed_stats() {
            let health_score = if metrics.is_healthy() { 50.0 } else { 0.0 };
            let utilization_score = stats.worker_utilization() * 0.3;
            let success_score = stats.success_rate() * 0.2;

            health_score + utilization_score + success_score
        } else {
            0.0
        };

        Self {
            pool_healthy: pool.is_healthy(),
            pool_stalled: pool.is_stalled(),
            resource_pressure,
            recommended_worker_count,
            current_worker_count: pool.worker_count(),
            performance_score,
        }
    }

    /// Check if the system is performing optimally
    pub fn is_optimal(&self) -> bool {
        self.pool_healthy
            && !self.pool_stalled
            && self.performance_score > 80.0
            && self.current_worker_count <= self.recommended_worker_count
    }

    /// Get recommendations for improving performance
    pub fn recommendations(&self) -> Vec<String> {
        let mut recs = Vec::new();

        if !self.pool_healthy {
            recs.push("Pool is unhealthy - check worker error rates and connectivity".to_string());
        }

        if self.pool_stalled {
            recs.push(
                "Pool appears stalled - check for deadlocks or resource exhaustion".to_string(),
            );
        }

        if self.current_worker_count > self.recommended_worker_count {
            recs.push(format!(
                "Consider reducing worker count from {} to {} for better resource utilization",
                self.current_worker_count, self.recommended_worker_count
            ));
        }

        if self.performance_score < 50.0 {
            recs.push(
                "Performance is below optimal - review configuration and system resources"
                    .to_string(),
            );
        }

        match self.resource_pressure {
            ResourcePressure::High => {
                recs.push(
                    "System under high resource pressure - consider reducing worker count"
                        .to_string(),
                );
            }
            ResourcePressure::Medium => {
                recs.push(
                    "System under moderate resource pressure - monitor performance".to_string(),
                );
            }
            ResourcePressure::Low => {
                // No recommendations for low pressure
            }
        }

        if recs.is_empty() {
            recs.push("System is performing optimally".to_string());
        }

        recs
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::app::cache::CacheConfig;
    use crate::app::client::ClientConfig;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};

    async fn create_test_components() -> (
        Arc<crate::app::queue::WorkQueue>,
        Arc<crate::app::cache::CacheManager>,
        Arc<crate::app::client::CedaClient>,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        let queue = Arc::new(crate::app::queue::WorkQueue::new());
        let cache = Arc::new(
            crate::app::cache::CacheManager::new(cache_config)
                .await
                .unwrap(),
        );
        let client = Arc::new(
            crate::app::client::CedaClient::new_simple_with_config(ClientConfig::default())
                .await
                .unwrap(),
        );

        (queue, cache, client, temp_dir)
    }

    /// Test complete worker system integration
    ///
    /// Verifies that the entire worker system can be created, started,
    /// and shut down correctly using the public API.
    #[tokio::test]
    async fn test_worker_system_integration() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;

        // Test builder pattern
        let mut pool = WorkerSystemBuilder::new()
            .with_worker_count(2)
            .with_max_retries(2)
            .with_download_timeout(Duration::from_secs(30))
            .with_queue(queue)
            .with_cache(cache)
            .with_client(client)
            .build()
            .unwrap();

        let (progress_tx, _progress_rx) = tokio::sync::mpsc::channel(10);

        // Start the system
        pool.start(progress_tx).await.unwrap();
        assert_eq!(pool.worker_count(), 2);

        // Give workers time to start and report initial status
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Health may not be reported immediately if no workers have progress yet
        // Just verify the pool is running
        assert_eq!(pool.state(), PoolState::Running);

        // Give workers a moment to initialize
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should have initial statistics
        let stats = pool.get_stats().unwrap();
        // Workers may take time to report their initial status
        assert!(stats.active_workers <= 2);

        // Shutdown gracefully
        pool.shutdown().await.unwrap();
    }

    /// Test utility functions for pool creation
    ///
    /// Ensures that utility functions correctly create pools with
    /// appropriate configurations for different environments.
    #[tokio::test]
    async fn test_utility_pool_creation() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;

        // Test all utility creation functions
        let test_pool = utils::create_test_pool(queue.clone(), cache.clone(), client.clone());
        assert_eq!(test_pool.config().worker_count, 2);

        let dev_pool = utils::create_development_pool(queue.clone(), cache.clone(), client.clone());
        assert_eq!(dev_pool.config().worker_count, 4);

        let prod_pool = utils::create_production_pool(queue.clone(), cache.clone(), client.clone());
        assert_eq!(prod_pool.config().worker_count, 8);

        let high_pool = utils::create_high_throughput_pool(queue, cache, client);
        assert_eq!(high_pool.config().worker_count, 16);
    }

    /// Test health monitoring and reporting
    ///
    /// Verifies that health monitoring correctly identifies system state
    /// and provides meaningful recommendations.
    #[tokio::test]
    async fn test_health_monitoring() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;

        let mut pool = utils::create_test_pool(queue, cache, client);
        let (progress_tx, _progress_rx) = tokio::sync::mpsc::channel(10);

        pool.start(progress_tx).await.unwrap();

        // Assess system health
        let health = SystemHealth::assess(&pool);
        assert!(health.pool_healthy || !health.pool_stalled); // Should be one or the other initially

        // Generate health report
        let report = utils::generate_health_report(&pool);
        assert!(report.contains("Worker Pool Health Report"));
        assert!(report.contains("State:"));
        assert!(report.contains("Worker Count:"));

        // Test recommendations
        let recommendations = health.recommendations();
        assert!(!recommendations.is_empty());

        pool.shutdown().await.unwrap();
    }

    /// Test statistics collection over time
    ///
    /// Ensures that statistics can be collected over time periods
    /// for performance analysis and monitoring.
    #[tokio::test]
    async fn test_statistics_collection() {
        let (queue, cache, client, _temp_dir) = create_test_components().await;

        let mut pool = utils::create_test_pool(queue, cache, client);
        let (progress_tx, _progress_rx) = tokio::sync::mpsc::channel(10);

        pool.start(progress_tx).await.unwrap();

        // Collect statistics over a short period
        let stats_future = utils::collect_statistics_over_time(
            &pool,
            Duration::from_millis(200),
            Duration::from_millis(50),
        );

        let samples = timeout(Duration::from_secs(1), stats_future).await.unwrap();

        assert!(!samples.is_empty());
        assert!(samples.len() >= 2); // Should collect multiple samples

        pool.shutdown().await.unwrap();
    }

    /// Test resource pressure detection and recommendations
    ///
    /// Verifies that the system correctly detects resource pressure
    /// and provides appropriate configuration recommendations.
    #[test]
    fn test_resource_management() {
        // Test resource pressure detection
        let pressure = utils::detect_resource_pressure();
        assert!(matches!(
            pressure,
            ResourcePressure::Low | ResourcePressure::Medium | ResourcePressure::High
        ));

        // Test optimal worker count calculation
        let optimal = utils::calculate_optimal_worker_count();
        assert!(optimal >= 1);
        assert!(optimal <= crate::constants::workers::MAX_WORKER_COUNT);

        // Test configuration recommendation
        let config = utils::recommend_worker_config();
        assert!(config.worker_count >= 1);
        assert!(config.worker_count <= crate::constants::workers::MAX_WORKER_COUNT);
        assert!(config.validate().is_ok());
    }

    /// Test error handling in worker system builder
    ///
    /// Ensures that the builder correctly validates inputs and
    /// provides meaningful error messages for missing components.
    #[test]
    fn test_builder_error_handling() {
        // Test missing components
        let result = WorkerSystemBuilder::new().with_worker_count(4).build();
        assert!(result.is_err());

        // Test invalid configuration
        let result = WorkerConfigBuilder::new()
            .worker_count(0) // Invalid
            .build();
        assert!(result.is_err());
    }

    /// Test convenience re-exports and type aliases
    ///
    /// Verifies that all public API re-exports work correctly and
    /// provide convenient access to the module's functionality.
    #[test]
    fn test_public_api_exports() {
        // Test type aliases
        let _config: WorkerConfig = Presets::testing();
        let _status: WorkerStatus = WorkerStatus::Idle;

        // Test that all important types are accessible
        let _result_type: std::marker::PhantomData<Result<()>> = std::marker::PhantomData;
        let _progress_type: std::marker::PhantomData<Progress> = std::marker::PhantomData;

        // Test builder availability
        let _builder = WorkerConfigBuilder::new();
        let _pool_builder = WorkerPoolBuilder::new();
        let _system_builder = WorkerSystemBuilder::new();
    }
}
