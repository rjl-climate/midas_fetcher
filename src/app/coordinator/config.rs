//! Configuration structures for the download coordinator
//!
//! This module defines the configuration options for coordinating downloads,
//! including worker pool settings, progress reporting, and shutdown behavior.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::app::worker::WorkerConfig;
use crate::constants::workers;

/// Configuration for the download coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            progress_batch_size: 10000,
            worker_config: WorkerConfig::default(),
        }
    }
}

impl CoordinatorConfig {
    /// Create a new coordinator configuration with custom worker count
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self.worker_config.worker_count = count;
        self
    }

    /// Enable or disable verbose logging
    pub fn with_verbose_logging(mut self, enabled: bool) -> Self {
        self.verbose_logging = enabled;
        self
    }

    /// Set progress update interval
    pub fn with_progress_interval(mut self, interval: Duration) -> Self {
        self.progress_update_interval = interval;
        self
    }

    /// Set shutdown timeout
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.progress_update_interval.is_zero() {
            return Err("Progress update interval cannot be zero".to_string());
        }

        if self.shutdown_timeout.is_zero() {
            return Err("Shutdown timeout cannot be zero".to_string());
        }

        if self.progress_batch_size == 0 {
            return Err("Progress batch size cannot be zero".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that default configuration is valid
    ///
    /// Ensures that the default coordinator configuration passes validation
    /// and has sensible values for production use.
    #[test]
    fn test_default_config_is_valid() {
        let config = CoordinatorConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.worker_count, workers::DEFAULT_WORKER_COUNT);
        assert!(config.enable_progress_bar);
        assert!(!config.verbose_logging);
    }

    /// Test configuration builder methods
    ///
    /// Verifies that the builder pattern methods correctly modify
    /// configuration options and maintain valid state.
    #[test]
    fn test_config_builder_methods() {
        let config = CoordinatorConfig::default()
            .with_worker_count(16)
            .with_verbose_logging(true)
            .with_progress_interval(Duration::from_millis(100))
            .with_shutdown_timeout(Duration::from_secs(60));

        assert_eq!(config.worker_count, 16);
        assert_eq!(config.worker_config.worker_count, 16);
        assert!(config.verbose_logging);
        assert_eq!(config.progress_update_interval, Duration::from_millis(100));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
        assert!(config.validate().is_ok());
    }

    /// Test configuration validation
    ///
    /// Ensures that invalid configurations are properly detected
    /// and appropriate error messages are returned.
    #[test]
    fn test_config_validation() {
        // Test zero progress interval
        let mut config = CoordinatorConfig {
            progress_update_interval: Duration::ZERO,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Reset and test zero shutdown timeout
        config = CoordinatorConfig::default();
        config.shutdown_timeout = Duration::ZERO;
        assert!(config.validate().is_err());

        // Reset and test zero batch size
        config = CoordinatorConfig::default();
        config.progress_batch_size = 0;
        assert!(config.validate().is_err());
    }
}
