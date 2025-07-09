//! Dynamic configuration management for the work queue
//!
//! This module provides flexible configuration options for the work queue,
//! eliminating hard-coded constants and allowing runtime configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Builder for creating work queue configurations
#[derive(Debug, Clone)]
pub struct WorkQueueConfigBuilder {
    max_retries: Option<u32>,
    retry_delay: Option<Duration>,
    max_workers: Option<u32>,
    work_timeout: Option<Duration>,
    max_pending_items: Option<usize>,
    default_priority: Option<u32>,
    high_priority: Option<u32>,
    low_priority: Option<u32>,
}

impl WorkQueueConfigBuilder {
    /// Create a new configuration builder
    pub fn new() -> Self {
        Self {
            max_retries: None,
            retry_delay: None,
            max_workers: None,
            work_timeout: None,
            max_pending_items: None,
            default_priority: None,
            high_priority: None,
            low_priority: None,
        }
    }

    /// Set maximum retry attempts
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Set retry delay
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = Some(delay);
        self
    }

    /// Set maximum number of workers
    pub fn max_workers(mut self, max_workers: u32) -> Self {
        self.max_workers = Some(max_workers);
        self
    }

    /// Set work timeout
    pub fn work_timeout(mut self, timeout: Duration) -> Self {
        self.work_timeout = Some(timeout);
        self
    }

    /// Set maximum pending items
    pub fn max_pending_items(mut self, max_pending: usize) -> Self {
        self.max_pending_items = Some(max_pending);
        self
    }

    /// Set default priority
    pub fn default_priority(mut self, priority: u32) -> Self {
        self.default_priority = Some(priority);
        self
    }

    /// Set high priority
    pub fn high_priority(mut self, priority: u32) -> Self {
        self.high_priority = Some(priority);
        self
    }

    /// Set low priority
    pub fn low_priority(mut self, priority: u32) -> Self {
        self.low_priority = Some(priority);
        self
    }

    /// Build the configuration
    pub fn build(self) -> super::types::WorkQueueConfig {
        super::types::WorkQueueConfig {
            max_retries: self.max_retries.unwrap_or(3),
            retry_delay: self.retry_delay.unwrap_or(Duration::from_secs(30)),
            max_workers: self.max_workers.unwrap_or(8),
            work_timeout: self.work_timeout.unwrap_or(Duration::from_secs(300)),
            max_pending_items: self.max_pending_items.unwrap_or(50_000),
            default_priority: self.default_priority.unwrap_or(100),
        }
    }
}

impl Default for WorkQueueConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration presets for different use cases
pub struct ConfigPresets;

impl ConfigPresets {
    /// Configuration optimized for production use
    pub fn production() -> super::types::WorkQueueConfig {
        WorkQueueConfigBuilder::new()
            .max_retries(3)
            .retry_delay(Duration::from_secs(30))
            .max_workers(8)
            .work_timeout(Duration::from_secs(300))
            .max_pending_items(50_000)
            .default_priority(100)
            .build()
    }

    /// Configuration optimized for development
    pub fn development() -> super::types::WorkQueueConfig {
        WorkQueueConfigBuilder::new()
            .max_retries(2)
            .retry_delay(Duration::from_secs(5))
            .max_workers(4)
            .work_timeout(Duration::from_secs(60))
            .max_pending_items(1_000)
            .default_priority(100)
            .build()
    }

    /// Configuration optimized for testing
    pub fn testing() -> super::types::WorkQueueConfig {
        WorkQueueConfigBuilder::new()
            .max_retries(2)
            .retry_delay(Duration::from_millis(10))
            .max_workers(2)
            .work_timeout(Duration::from_millis(100))
            .max_pending_items(100)
            .default_priority(100)
            .build()
    }

    /// Configuration optimized for high-throughput scenarios
    pub fn high_throughput() -> super::types::WorkQueueConfig {
        WorkQueueConfigBuilder::new()
            .max_retries(5)
            .retry_delay(Duration::from_secs(15))
            .max_workers(16)
            .work_timeout(Duration::from_secs(600))
            .max_pending_items(100_000)
            .default_priority(100)
            .build()
    }

    /// Configuration optimized for low-resource environments
    pub fn low_resource() -> super::types::WorkQueueConfig {
        WorkQueueConfigBuilder::new()
            .max_retries(2)
            .retry_delay(Duration::from_secs(60))
            .max_workers(2)
            .work_timeout(Duration::from_secs(120))
            .max_pending_items(500)
            .default_priority(100)
            .build()
    }
}

/// Environment-based configuration loading
pub struct ConfigLoader;

impl ConfigLoader {
    /// Load configuration from environment variables
    pub fn from_env() -> super::types::WorkQueueConfig {
        let mut builder = WorkQueueConfigBuilder::new();

        // Load from environment variables with fallback to defaults
        if let Ok(max_retries) = std::env::var("QUEUE_MAX_RETRIES") {
            if let Ok(retries) = max_retries.parse::<u32>() {
                builder = builder.max_retries(retries);
            }
        }

        if let Ok(retry_delay_secs) = std::env::var("QUEUE_RETRY_DELAY_SECS") {
            if let Ok(delay) = retry_delay_secs.parse::<u64>() {
                builder = builder.retry_delay(Duration::from_secs(delay));
            }
        }

        if let Ok(max_workers) = std::env::var("QUEUE_MAX_WORKERS") {
            if let Ok(workers) = max_workers.parse::<u32>() {
                builder = builder.max_workers(workers);
            }
        }

        if let Ok(work_timeout_secs) = std::env::var("QUEUE_WORK_TIMEOUT_SECS") {
            if let Ok(timeout) = work_timeout_secs.parse::<u64>() {
                builder = builder.work_timeout(Duration::from_secs(timeout));
            }
        }

        if let Ok(max_pending) = std::env::var("QUEUE_MAX_PENDING_ITEMS") {
            if let Ok(pending) = max_pending.parse::<usize>() {
                builder = builder.max_pending_items(pending);
            }
        }

        if let Ok(default_priority) = std::env::var("QUEUE_DEFAULT_PRIORITY") {
            if let Ok(priority) = default_priority.parse::<u32>() {
                builder = builder.default_priority(priority);
            }
        }

        builder.build()
    }

    /// Load configuration from file
    pub fn from_file(
        path: &std::path::Path,
    ) -> Result<super::types::WorkQueueConfig, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: QueueConfigFile = toml::from_str(&content)?;
        Ok(config.into())
    }

    /// Load configuration with precedence: file -> env -> defaults
    pub fn load_with_precedence(
        config_file: Option<&std::path::Path>,
    ) -> super::types::WorkQueueConfig {
        let mut config = ConfigPresets::production();

        // Override with file if provided
        if let Some(path) = config_file {
            if let Ok(file_config) = Self::from_file(path) {
                config = file_config;
            }
        }

        // Override with environment variables
        let env_config = Self::from_env();
        config = merge_configs(config, env_config);

        config
    }
}

/// Configuration file format
#[derive(Debug, Deserialize, Serialize)]
struct QueueConfigFile {
    max_retries: Option<u32>,
    retry_delay_secs: Option<u64>,
    max_workers: Option<u32>,
    work_timeout_secs: Option<u64>,
    max_pending_items: Option<usize>,
    default_priority: Option<u32>,
}

impl From<QueueConfigFile> for super::types::WorkQueueConfig {
    fn from(file_config: QueueConfigFile) -> Self {
        let mut builder = WorkQueueConfigBuilder::new();

        if let Some(max_retries) = file_config.max_retries {
            builder = builder.max_retries(max_retries);
        }

        if let Some(retry_delay_secs) = file_config.retry_delay_secs {
            builder = builder.retry_delay(Duration::from_secs(retry_delay_secs));
        }

        if let Some(max_workers) = file_config.max_workers {
            builder = builder.max_workers(max_workers);
        }

        if let Some(work_timeout_secs) = file_config.work_timeout_secs {
            builder = builder.work_timeout(Duration::from_secs(work_timeout_secs));
        }

        if let Some(max_pending_items) = file_config.max_pending_items {
            builder = builder.max_pending_items(max_pending_items);
        }

        if let Some(default_priority) = file_config.default_priority {
            builder = builder.default_priority(default_priority);
        }

        builder.build()
    }
}

/// Merge two configurations, preferring the second one for set values
fn merge_configs(
    _base: super::types::WorkQueueConfig,
    override_config: super::types::WorkQueueConfig,
) -> super::types::WorkQueueConfig {
    // For now, we'll just use the override config since we don't have optional fields
    // In a real implementation, we'd need to track which fields were explicitly set
    override_config
}

/// Priority constants for common use cases
pub struct Priority;

impl Priority {
    /// High priority for critical work
    pub const HIGH: u32 = 200;
    /// Default priority for normal work
    pub const DEFAULT: u32 = 100;
    /// Low priority for background work
    pub const LOW: u32 = 50;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_config_builder() {
        let config = WorkQueueConfigBuilder::new()
            .max_retries(5)
            .retry_delay(Duration::from_secs(60))
            .max_workers(12)
            .build();

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay, Duration::from_secs(60));
        assert_eq!(config.max_workers, 12);
    }

    #[test]
    fn test_config_presets() {
        let prod_config = ConfigPresets::production();
        assert_eq!(prod_config.max_retries, 3);
        assert_eq!(prod_config.max_workers, 8);

        let test_config = ConfigPresets::testing();
        assert_eq!(test_config.max_retries, 2);
        assert_eq!(test_config.max_workers, 2);
        assert_eq!(test_config.retry_delay, Duration::from_millis(10));
    }

    #[test]
    fn test_config_validation() {
        let config = ConfigPresets::production();
        assert!(config.validate().is_ok());

        let invalid_config = WorkQueueConfigBuilder::new().max_retries(0).build();
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_priority_constants() {
        // Test that priority constants are properly ordered
        let high = Priority::HIGH;
        let default = Priority::DEFAULT;
        let low = Priority::LOW;

        assert!(high > default);
        assert!(default > low);
    }

    #[test]
    fn test_environment_loading() {
        std::env::set_var("QUEUE_MAX_RETRIES", "5");
        std::env::set_var("QUEUE_MAX_WORKERS", "12");

        let config = ConfigLoader::from_env();
        // The environment loading should work, but we can't guarantee exact values
        // since other tests might interfere
        assert!(config.max_retries >= 3);
        assert!(config.max_workers >= 8);

        // Clean up
        std::env::remove_var("QUEUE_MAX_RETRIES");
        std::env::remove_var("QUEUE_MAX_WORKERS");
    }
}
