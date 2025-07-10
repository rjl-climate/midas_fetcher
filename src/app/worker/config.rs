//! Worker configuration management
//!
//! This module provides configuration structures and validation for download workers,
//! supporting dynamic configuration without hardcoded values and providing sensible
//! defaults for different deployment scenarios.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::workers;
use crate::errors::DownloadResult;

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
    /// Number of samples for speed calculation
    pub speed_calculation_samples: usize,
    /// Maximum backoff multiplier for idle workers
    pub max_backoff_multiplier: u32,
    /// Jitter percentage for backoff randomization
    pub backoff_jitter_percentage: f64,
    /// Maximum idle sleep duration (milliseconds)
    pub max_idle_sleep_ms: u64,
    /// Sleep duration on errors
    pub error_sleep_duration: Duration,
    /// Exponential backoff multiplier for retries
    pub retry_backoff_multiplier: u32,
    /// Minimum progress update interval (milliseconds)
    pub min_progress_update_interval_ms: u64,
    /// Queue wait time threshold for logging (milliseconds)
    pub queue_wait_log_threshold_ms: u64,
    /// Short queue wait time threshold for logging (milliseconds)
    pub short_queue_wait_log_threshold_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_count: workers::DEFAULT_WORKER_COUNT,
            max_retries: workers::MAX_RETRIES,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(30),
            idle_sleep_duration: Duration::from_millis(100),
            progress_buffer_size: workers::CHANNEL_BUFFER_SIZE,
            download_timeout: workers::DEFAULT_DOWNLOAD_TIMEOUT,
            detailed_progress: true,
            speed_calculation_samples: workers::SPEED_CALCULATION_SAMPLES,
            max_backoff_multiplier: workers::MAX_BACKOFF_MULTIPLIER,
            backoff_jitter_percentage: workers::BACKOFF_JITTER_PERCENTAGE,
            max_idle_sleep_ms: workers::MAX_IDLE_SLEEP_MS,
            error_sleep_duration: workers::ERROR_SLEEP_DURATION,
            retry_backoff_multiplier: workers::RETRY_BACKOFF_MULTIPLIER,
            min_progress_update_interval_ms: workers::MIN_PROGRESS_UPDATE_INTERVAL_MS,
            queue_wait_log_threshold_ms: workers::QUEUE_WAIT_LOG_THRESHOLD_MS,
            short_queue_wait_log_threshold_ms: workers::SHORT_QUEUE_WAIT_LOG_THRESHOLD_MS,
        }
    }
}

impl WorkerConfig {
    /// Validate configuration values and return errors for invalid settings
    pub fn validate(&self) -> DownloadResult<()> {
        if self.worker_count == 0 {
            return Err(crate::errors::DownloadError::ConfigurationError(
                "Worker count cannot be zero".to_string(),
            ));
        }

        if self.worker_count > workers::MAX_WORKER_COUNT {
            return Err(crate::errors::DownloadError::ConfigurationError(format!(
                "Worker count ({}) exceeds maximum ({})",
                self.worker_count,
                workers::MAX_WORKER_COUNT
            )));
        }

        if self.retry_base_delay >= self.retry_max_delay {
            return Err(crate::errors::DownloadError::ConfigurationError(
                "Retry base delay must be less than max delay".to_string(),
            ));
        }

        if self.backoff_jitter_percentage < 0.0 || self.backoff_jitter_percentage > 1.0 {
            return Err(crate::errors::DownloadError::ConfigurationError(
                "Backoff jitter percentage must be between 0.0 and 1.0".to_string(),
            ));
        }

        if self.speed_calculation_samples == 0 {
            return Err(crate::errors::DownloadError::ConfigurationError(
                "Speed calculation samples must be greater than zero".to_string(),
            ));
        }

        Ok(())
    }
}

/// Builder for WorkerConfig following the established pattern
#[derive(Debug, Default)]
pub struct WorkerConfigBuilder {
    config: WorkerConfig,
}

impl WorkerConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            config: WorkerConfig::default(),
        }
    }

    /// Set the number of workers
    pub fn worker_count(mut self, count: usize) -> Self {
        self.config.worker_count = count;
        self
    }

    /// Set maximum retry attempts
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set retry base delay
    pub fn retry_base_delay(mut self, delay: Duration) -> Self {
        self.config.retry_base_delay = delay;
        self
    }

    /// Set retry max delay
    pub fn retry_max_delay(mut self, delay: Duration) -> Self {
        self.config.retry_max_delay = delay;
        self
    }

    /// Set idle sleep duration
    pub fn idle_sleep_duration(mut self, duration: Duration) -> Self {
        self.config.idle_sleep_duration = duration;
        self
    }

    /// Set progress buffer size
    pub fn progress_buffer_size(mut self, size: usize) -> Self {
        self.config.progress_buffer_size = size;
        self
    }

    /// Set download timeout
    pub fn download_timeout(mut self, timeout: Duration) -> Self {
        self.config.download_timeout = timeout;
        self
    }

    /// Enable or disable detailed progress reporting
    pub fn detailed_progress(mut self, enabled: bool) -> Self {
        self.config.detailed_progress = enabled;
        self
    }

    /// Set speed calculation samples
    pub fn speed_calculation_samples(mut self, samples: usize) -> Self {
        self.config.speed_calculation_samples = samples;
        self
    }

    /// Set backoff jitter percentage
    pub fn backoff_jitter_percentage(mut self, percentage: f64) -> Self {
        self.config.backoff_jitter_percentage = percentage;
        self
    }

    /// Build and validate the configuration
    pub fn build(self) -> DownloadResult<WorkerConfig> {
        self.config.validate()?;
        Ok(self.config)
    }

    /// Build without validation (for testing)
    pub fn build_unchecked(self) -> WorkerConfig {
        self.config
    }
}

/// Configuration presets for different deployment scenarios
pub struct ConfigPresets;

impl ConfigPresets {
    /// Production configuration with conservative defaults
    pub fn production() -> WorkerConfig {
        WorkerConfig {
            worker_count: 8,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(1000),
            retry_max_delay: Duration::from_secs(60),
            idle_sleep_duration: Duration::from_millis(100),
            download_timeout: Duration::from_secs(600), // 10 minutes
            detailed_progress: false,                   // Reduce overhead
            ..Default::default()
        }
    }

    /// Development configuration with shorter timeouts and more logging
    pub fn development() -> WorkerConfig {
        WorkerConfig {
            worker_count: 4,
            max_retries: 2,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
            idle_sleep_duration: Duration::from_millis(50),
            download_timeout: Duration::from_secs(120), // 2 minutes
            detailed_progress: true,
            ..Default::default()
        }
    }

    /// Testing configuration with fast timeouts and minimal retries
    pub fn testing() -> WorkerConfig {
        WorkerConfig {
            worker_count: 2,
            max_retries: 1,
            retry_base_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_millis(100),
            idle_sleep_duration: Duration::from_millis(10),
            download_timeout: Duration::from_secs(5),
            detailed_progress: true,
            progress_buffer_size: 10,
            ..Default::default()
        }
    }

    /// High throughput configuration for powerful systems
    pub fn high_throughput() -> WorkerConfig {
        WorkerConfig {
            worker_count: 16,
            max_retries: 5,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(30),
            idle_sleep_duration: Duration::from_millis(50),
            download_timeout: Duration::from_secs(300), // 5 minutes
            detailed_progress: false,                   // Reduce overhead
            progress_buffer_size: 200,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test WorkerConfig default values match constants
    ///
    /// Ensures configuration defaults are consistent with defined constants
    /// and provide reasonable values for production use.
    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.worker_count, workers::DEFAULT_WORKER_COUNT);
        assert_eq!(config.max_retries, workers::MAX_RETRIES);
        assert_eq!(config.progress_buffer_size, workers::CHANNEL_BUFFER_SIZE);
        assert_eq!(config.download_timeout, workers::DEFAULT_DOWNLOAD_TIMEOUT);
        assert!(config.retry_base_delay < config.retry_max_delay);
        assert!(config.detailed_progress);
    }

    /// Test configuration validation catches invalid values
    ///
    /// Verifies that configuration validation properly rejects
    /// invalid worker counts, delay values, and other parameters.
    #[test]
    fn test_config_validation() {
        // Test zero worker count
        let config = WorkerConfig {
            worker_count: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test excessive worker count
        let config = WorkerConfig {
            worker_count: workers::MAX_WORKER_COUNT + 1,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test invalid retry delays
        let config = WorkerConfig {
            worker_count: 4,
            retry_base_delay: Duration::from_secs(60),
            retry_max_delay: Duration::from_secs(30),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test invalid jitter percentage
        let config = WorkerConfig {
            backoff_jitter_percentage: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test zero speed calculation samples
        let config = WorkerConfig {
            speed_calculation_samples: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test valid configuration
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
    }

    /// Test WorkerConfigBuilder fluent API
    ///
    /// Verifies that the builder pattern works correctly and produces
    /// valid configurations with custom values.
    #[test]
    fn test_config_builder() {
        let config = WorkerConfigBuilder::new()
            .worker_count(4)
            .max_retries(5)
            .download_timeout(Duration::from_secs(300))
            .detailed_progress(false)
            .build()
            .unwrap();

        assert_eq!(config.worker_count, 4);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.download_timeout, Duration::from_secs(300));
        assert!(!config.detailed_progress);
    }

    /// Test configuration presets provide different valid configurations
    ///
    /// Ensures that all preset configurations are valid and have
    /// appropriate values for their intended use cases.
    #[test]
    fn test_config_presets() {
        let prod = ConfigPresets::production();
        let dev = ConfigPresets::development();
        let test = ConfigPresets::testing();
        let high = ConfigPresets::high_throughput();

        // All presets should be valid
        assert!(prod.validate().is_ok());
        assert!(dev.validate().is_ok());
        assert!(test.validate().is_ok());
        assert!(high.validate().is_ok());

        // Test specific characteristics
        assert!(prod.worker_count >= dev.worker_count);
        assert!(dev.worker_count >= test.worker_count);
        assert!(high.worker_count >= prod.worker_count);

        assert!(test.download_timeout < dev.download_timeout);
        assert!(dev.download_timeout < prod.download_timeout);

        assert!(!prod.detailed_progress); // Production should be efficient
        assert!(dev.detailed_progress); // Development should be verbose
        assert!(test.detailed_progress); // Testing should be verbose
    }
}
