//! Configuration management for MIDAS Fetcher
//!
//! This module provides unified configuration management with automatic
//! first-run initialization, multi-source loading, and zero-config defaults.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::app::{
    CacheConfig, ClientConfig, CoordinatorConfig, ManifestConfig, WorkQueueConfig, WorkerConfig,
};
use crate::constants::{limits, workers};
use crate::errors::{AppError, Result};

/// Unified application configuration for TOML serialization
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    /// Cache management settings
    pub cache: CacheConfigToml,
    /// HTTP client settings  
    pub client: ClientConfigToml,
    /// Download coordinator settings
    pub coordinator: CoordinatorConfigToml,
    /// Work queue settings
    pub queue: WorkQueueConfigToml,
    /// Manifest processing settings
    pub manifest: ManifestConfigToml,
    /// Logging configuration
    pub logging: LoggingConfig,
}

/// TOML-friendly cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfigToml {
    /// Cache directory path
    pub cache_root: Option<PathBuf>,
    /// Maximum cache size in bytes (0 = unlimited)
    pub max_cache_size: u64,
    /// Enable fast verification using manifest hashes
    pub fast_verification: bool,
    /// Reservation timeout in seconds
    pub reservation_timeout_secs: u64,
    /// Enable automatic cleanup of old files
    pub auto_cleanup: bool,
    /// Minimum free space to maintain (bytes)
    pub min_free_space: u64,
}

impl Default for CacheConfigToml {
    fn default() -> Self {
        Self {
            cache_root: None,
            max_cache_size: 0,
            fast_verification: true,
            reservation_timeout_secs: 30,
            auto_cleanup: false,
            min_free_space: 1_073_741_824, // 1GB
        }
    }
}

/// TOML-friendly client configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfigToml {
    /// Enable HTTP/2 support
    pub http2: bool,
    /// TCP keep-alive timeout in seconds (None = disabled)
    pub tcp_keepalive_secs: Option<u64>,
    /// TCP nodelay setting
    pub tcp_nodelay: bool,
    /// Connection pool idle timeout in seconds (None = no timeout)
    pub pool_idle_timeout_secs: Option<u64>,
    /// Maximum connections per host
    pub pool_max_per_host: usize,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    /// Connect timeout in seconds
    pub connect_timeout_secs: u64,
    /// Rate limit (requests per second)
    pub rate_limit_rps: u32,
}

impl Default for ClientConfigToml {
    fn default() -> Self {
        Self {
            http2: false, // Disabled for CEDA compatibility
            tcp_keepalive_secs: Some(30),
            tcp_nodelay: true,
            pool_idle_timeout_secs: Some(90),
            pool_max_per_host: 8,
            request_timeout_secs: 60,
            connect_timeout_secs: 30,
            rate_limit_rps: limits::DEFAULT_RATE_LIMIT_RPS,
        }
    }
}

/// TOML-friendly coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfigToml {
    /// Number of concurrent workers
    pub worker_count: usize,
    /// Progress update interval in milliseconds
    pub progress_update_interval_ms: u64,
    /// Shutdown timeout in seconds
    pub shutdown_timeout_secs: u64,
    /// Enable progress bar display
    pub enable_progress_bar: bool,
    /// Enable verbose logging
    pub verbose_logging: bool,
    /// Progress batch size
    pub progress_batch_size: usize,
    /// Worker configuration (embedded)
    pub worker: WorkerConfigToml,
}

impl Default for CoordinatorConfigToml {
    fn default() -> Self {
        Self {
            worker_count: workers::DEFAULT_WORKER_COUNT,
            progress_update_interval_ms: 500,
            shutdown_timeout_secs: 30,
            enable_progress_bar: true,
            verbose_logging: false,
            progress_batch_size: 10_000,
            worker: WorkerConfigToml::default(),
        }
    }
}

/// TOML-friendly queue configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueConfigToml {
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Maximum number of workers
    pub max_workers: u32,
    /// Work timeout in seconds
    pub work_timeout_secs: u64,
}

impl Default for WorkQueueConfigToml {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 1000,
            max_workers: workers::DEFAULT_WORKER_COUNT as u32,
            work_timeout_secs: 300,
        }
    }
}

/// TOML-friendly manifest configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConfigToml {
    /// Root directory for file destinations
    pub destination_root: PathBuf,
    /// Maximum tracked hashes
    pub max_tracked_hashes: usize,
    /// Allow duplicate files
    pub allow_duplicates: bool,
    /// Progress batch size
    pub progress_batch_size: usize,
}

/// TOML-friendly worker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfigToml {
    /// Number of concurrent workers to spawn
    pub worker_count: usize,
    /// Maximum retry attempts per file
    pub max_retries: u32,
    /// Base delay between retries in milliseconds
    pub retry_base_delay_ms: u64,
    /// Maximum retry delay in seconds
    pub retry_max_delay_secs: u64,
    /// Sleep duration when no work is available in milliseconds
    pub idle_sleep_duration_ms: u64,
    /// Channel buffer size for progress reporting
    pub progress_buffer_size: usize,
    /// Timeout for individual downloads in seconds
    pub download_timeout_secs: u64,
    /// Enable detailed progress reporting
    pub detailed_progress: bool,
}

impl Default for WorkerConfigToml {
    fn default() -> Self {
        Self {
            worker_count: workers::DEFAULT_WORKER_COUNT,
            max_retries: 3,
            retry_base_delay_ms: 100,
            retry_max_delay_secs: 30,
            idle_sleep_duration_ms: 100,
            progress_buffer_size: 100,
            download_timeout_secs: 600,
            detailed_progress: true,
        }
    }
}

impl Default for ManifestConfigToml {
    fn default() -> Self {
        Self {
            destination_root: PathBuf::from(""), // Empty means use cache directory
            max_tracked_hashes: 1_000_000,
            allow_duplicates: false,
            progress_batch_size: 10_000,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Default log level for the application
    pub level: String,
    /// Enable file logging
    pub file_logging: bool,
    /// Log file path (if file_logging is enabled)
    pub log_file: Option<PathBuf>,
    /// Enable colored output
    pub colored_output: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_logging: false,
            log_file: None,
            colored_output: true,
        }
    }
}

impl AppConfig {
    /// Convert TOML-friendly configuration to runtime configuration
    pub fn to_runtime_config(
        &self,
    ) -> (
        CacheConfig,
        ClientConfig,
        CoordinatorConfig,
        WorkQueueConfig,
        ManifestConfig,
    ) {
        (
            self.cache.to_runtime_config(),
            self.client.to_runtime_config(),
            self.coordinator.to_runtime_config(),
            self.queue.to_runtime_config(),
            self.manifest.to_runtime_config(),
        )
    }

    /// Load configuration with multi-source precedence:
    /// 1. Default values
    /// 2. Config file (if exists)
    /// 3. Environment variables  
    /// 4. CLI arguments
    pub async fn load(config_file_override: Option<PathBuf>) -> Result<Self> {
        let mut config = Self::default();

        // Try to load from config file
        let config_path = if let Some(ref path) = config_file_override {
            // Use explicit config file
            Some(path.clone())
        } else {
            // Look for default config file locations
            Self::find_config_file().await?
        };

        if let Some(path) = config_path {
            if path.exists() {
                debug!("Loading config from: {}", path.display());
                config = Self::load_from_file(&path).await?;
            } else if config_file_override.is_some() {
                return Err(AppError::generic(format!(
                    "Specified config file not found: {}",
                    path.display()
                )));
            }
        }

        // TODO: Apply environment variable overrides
        // TODO: Apply CLI argument overrides

        Ok(config)
    }

    /// Initialize configuration on first run
    ///
    /// Creates a default config file if none exists and notifies the user
    pub async fn initialize_first_run() -> Result<Option<PathBuf>> {
        let config_path = Self::get_default_config_path()?;

        if config_path.exists() {
            // Config already exists, nothing to do
            return Ok(Some(config_path));
        }

        // Create default config file
        info!("Creating default configuration file...");

        // Ensure parent directory exists
        if let Some(parent) = config_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                AppError::generic(format!(
                    "Failed to create config directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        // Generate default config with helpful comments
        let config_content = Self::generate_default_config_content();

        tokio::fs::write(&config_path, config_content)
            .await
            .map_err(|e| {
                AppError::generic(format!(
                    "Failed to write config file {}: {}",
                    config_path.display(),
                    e
                ))
            })?;

        // Notify user
        println!("📁 Created default configuration file:");
        println!("   {}", config_path.display());
        println!("   You can customize settings by editing this file.");
        println!();

        Ok(Some(config_path))
    }

    /// Find configuration file in standard locations
    async fn find_config_file() -> Result<Option<PathBuf>> {
        let search_paths = vec![
            // Project-local config
            PathBuf::from("./midas-fetcher.toml"),
            PathBuf::from("./config.toml"),
            // User config
            Self::get_default_config_path()?,
            // System config (Unix only)
            #[cfg(unix)]
            PathBuf::from("/etc/midas-fetcher/config.toml"),
        ];

        for path in search_paths {
            if path.exists() {
                debug!("Found config file: {}", path.display());
                return Ok(Some(path));
            }
        }

        debug!("No config file found in standard locations");
        Ok(None)
    }

    /// Get the default config file path for the current user
    fn get_default_config_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| AppError::generic("Could not determine user config directory"))?;

        Ok(config_dir.join("midas-fetcher").join("config.toml"))
    }

    /// Load configuration from a TOML file
    async fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = tokio::fs::read_to_string(path).await.map_err(|e| {
            AppError::generic(format!(
                "Failed to read config file {}: {}",
                path.display(),
                e
            ))
        })?;

        let config: AppConfig = toml::from_str(&content).map_err(|e| {
            AppError::generic(format!(
                "Failed to parse config file {}: {}",
                path.display(),
                e
            ))
        })?;

        info!("Loaded configuration from: {}", path.display());
        Ok(config)
    }

    /// Generate default configuration content with helpful comments
    fn generate_default_config_content() -> String {
        let default_cache_path = dirs::config_dir()
            .map(|dir| dir.join("midas-fetcher").join("cache"))
            .unwrap_or_else(|| PathBuf::from("./cache"));

        format!(
            r#"# MIDAS Fetcher Configuration
# This file was automatically generated on first run.
# You can customize any of these settings to suit your needs.

[cache]
# Cache directory (leave empty to use system default)
# cache_root = "/path/to/custom/cache"

# Maximum cache size in bytes (0 = unlimited)
max_cache_size = 0

# Enable fast verification using manifest hashes
fast_verification = true

# Reservation timeout in seconds
reservation_timeout_secs = 30

# Enable automatic cleanup of old files
auto_cleanup = false

# Minimum free space to maintain (1GB)
min_free_space = 1073741824

[client]
# HTTP client settings
http2 = false  # Disabled for CEDA compatibility
tcp_keepalive_secs = 30
tcp_nodelay = true
pool_idle_timeout_secs = 90
pool_max_per_host = 8
request_timeout_secs = 60
connect_timeout_secs = 30
rate_limit_rps = {}

[coordinator]
# Download coordination settings
worker_count = {}
progress_update_interval_ms = 500
shutdown_timeout_secs = 30
enable_progress_bar = true
verbose_logging = false
progress_batch_size = 10000

[coordinator.worker]
# Worker-specific settings
worker_count = {}
max_retries = 3
retry_base_delay_ms = 100
retry_max_delay_secs = 30
idle_sleep_duration_ms = 100
progress_buffer_size = 100
download_timeout_secs = 600
detailed_progress = true

[queue]
# Work queue settings
max_retries = 3
retry_delay_ms = 1000
max_workers = {}
work_timeout_secs = 300

[manifest]
# Manifest processing settings  
# Default destination: {}
destination_root = ""  # Empty = use default cache directory
max_tracked_hashes = 1000000
allow_duplicates = false
progress_batch_size = 10000

[logging]
# Logging configuration
level = "info"  # error, warn, info, debug, trace
file_logging = false
colored_output = true
# log_file = "/path/to/log/file.log"  # Uncomment to enable file logging
"#,
            limits::DEFAULT_RATE_LIMIT_RPS,
            workers::DEFAULT_WORKER_COUNT,
            workers::DEFAULT_WORKER_COUNT,
            workers::DEFAULT_WORKER_COUNT,
            default_cache_path.display()
        )
    }
}

impl CacheConfigToml {
    /// Convert to runtime CacheConfig
    pub fn to_runtime_config(&self) -> CacheConfig {
        CacheConfig {
            cache_root: self.cache_root.clone(),
            max_cache_size: self.max_cache_size,
            fast_verification: self.fast_verification,
            reservation_timeout: Duration::from_secs(self.reservation_timeout_secs),
            auto_cleanup: self.auto_cleanup,
            min_free_space: self.min_free_space,
        }
    }
}

impl ClientConfigToml {
    /// Convert to runtime ClientConfig
    pub fn to_runtime_config(&self) -> ClientConfig {
        ClientConfig {
            http2: self.http2,
            tcp_keepalive: self.tcp_keepalive_secs.map(Duration::from_secs),
            tcp_nodelay: self.tcp_nodelay,
            pool_idle_timeout: self.pool_idle_timeout_secs.map(Duration::from_secs),
            pool_max_per_host: self.pool_max_per_host,
            request_timeout: Duration::from_secs(self.request_timeout_secs),
            connect_timeout: Duration::from_secs(self.connect_timeout_secs),
            rate_limit_rps: self.rate_limit_rps,
        }
    }
}

impl CoordinatorConfigToml {
    /// Convert to runtime CoordinatorConfig
    pub fn to_runtime_config(&self) -> CoordinatorConfig {
        CoordinatorConfig {
            worker_count: self.worker_count,
            progress_update_interval: Duration::from_millis(self.progress_update_interval_ms),
            shutdown_timeout: Duration::from_secs(self.shutdown_timeout_secs),
            enable_progress_bar: self.enable_progress_bar,
            verbose_logging: self.verbose_logging,
            progress_batch_size: self.progress_batch_size,
            worker_config: self.worker.to_runtime_config(),
        }
    }
}

impl WorkQueueConfigToml {
    /// Convert to runtime WorkQueueConfig
    pub fn to_runtime_config(&self) -> WorkQueueConfig {
        WorkQueueConfig {
            max_retries: self.max_retries,
            retry_delay: Duration::from_millis(self.retry_delay_ms),
            max_workers: self.max_workers,
            work_timeout: Duration::from_secs(self.work_timeout_secs),
            max_pending_items: 50_000,
            default_priority: 100,
        }
    }
}

impl ManifestConfigToml {
    /// Convert to runtime ManifestConfig
    pub fn to_runtime_config(&self) -> ManifestConfig {
        // If destination_root is empty, use default() which resolves to cache directory
        let destination_root = if self.destination_root.as_os_str().is_empty() {
            ManifestConfig::default().destination_root
        } else {
            self.destination_root.clone()
        };

        ManifestConfig {
            destination_root,
            max_tracked_hashes: self.max_tracked_hashes,
            allow_duplicates: self.allow_duplicates,
            progress_batch_size: self.progress_batch_size,
        }
    }
}

impl WorkerConfigToml {
    /// Convert to runtime WorkerConfig
    pub fn to_runtime_config(&self) -> WorkerConfig {
        WorkerConfig {
            worker_count: self.worker_count,
            max_retries: self.max_retries,
            retry_base_delay: Duration::from_millis(self.retry_base_delay_ms),
            retry_max_delay: Duration::from_secs(self.retry_max_delay_secs),
            idle_sleep_duration: Duration::from_millis(self.idle_sleep_duration_ms),
            progress_buffer_size: self.progress_buffer_size,
            download_timeout: Duration::from_secs(self.download_timeout_secs),
            detailed_progress: self.detailed_progress,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_default_config_creation() {
        let config = AppConfig::default();

        // Verify defaults are reasonable
        assert_eq!(
            config.coordinator.worker_count,
            workers::DEFAULT_WORKER_COUNT
        );
        assert_eq!(config.client.rate_limit_rps, limits::DEFAULT_RATE_LIMIT_RPS);
        assert_eq!(config.logging.level, "info");
        assert!(config.cache.fast_verification);
    }

    #[tokio::test]
    async fn test_config_file_generation() {
        let content = AppConfig::generate_default_config_content();

        // Should be valid TOML
        let parsed: AppConfig = toml::from_str(&content).unwrap();

        // Should have sensible defaults
        assert_eq!(
            parsed.coordinator.worker_count,
            workers::DEFAULT_WORKER_COUNT
        );
        assert!(content.contains("# MIDAS Fetcher Configuration"));
        assert!(content.contains("[cache]"));
        assert!(content.contains("[client]"));
    }

    #[tokio::test]
    async fn test_config_loading_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("nonexistent.toml");

        // Should fail when explicitly specified
        let result = AppConfig::load(Some(config_path)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_loading_from_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        // Create a test config file
        let test_config = r#"
[cache]
max_cache_size = 0
fast_verification = true
reservation_timeout_secs = 30
auto_cleanup = false
min_free_space = 1073741824

[client]
http2 = false
tcp_keepalive_secs = 30
tcp_nodelay = true
pool_idle_timeout_secs = 90
pool_max_per_host = 8
request_timeout_secs = 60
connect_timeout_secs = 30
rate_limit_rps = 15

[coordinator]
worker_count = 16
progress_update_interval_ms = 500
shutdown_timeout_secs = 30
enable_progress_bar = true
verbose_logging = false
progress_batch_size = 10000

[coordinator.worker]
worker_count = 8
max_retries = 3
retry_base_delay_ms = 100
retry_max_delay_secs = 30
idle_sleep_duration_ms = 100
progress_buffer_size = 100
download_timeout_secs = 600
detailed_progress = true

[queue]
max_retries = 3
retry_delay_ms = 1000
max_workers = 8
work_timeout_secs = 300

[manifest]
destination_root = "./cache"
max_tracked_hashes = 1000000
allow_duplicates = false
progress_batch_size = 10000

[logging]
level = "debug"
file_logging = false
colored_output = true
"#;

        tokio::fs::write(&config_path, test_config).await.unwrap();

        // Load config
        let config = AppConfig::load(Some(config_path)).await.unwrap();

        // Verify custom values were loaded
        assert_eq!(config.coordinator.worker_count, 16);
        assert_eq!(config.logging.level, "debug");

        // Verify defaults are still present for unspecified values
        assert_eq!(config.client.rate_limit_rps, limits::DEFAULT_RATE_LIMIT_RPS);
    }
}
