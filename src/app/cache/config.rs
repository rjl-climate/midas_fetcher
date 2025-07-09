//! Cache configuration types and defaults
//!
//! This module contains the configuration structures for the cache system,
//! including default values and validation logic.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::cache;

/// Configuration for the cache management system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Root directory for cache storage (OS-specific if None)
    pub cache_root: Option<PathBuf>,
    /// Maximum cache size in bytes (0 = unlimited)
    pub max_cache_size: u64,
    /// Enable fast verification using manifest hashes
    pub fast_verification: bool,
    /// Timeout for reservation locks
    pub reservation_timeout: Duration,
    /// Enable automatic cleanup of old files
    pub auto_cleanup: bool,
    /// Minimum free space to maintain (bytes)
    pub min_free_space: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_root: None,  // Will use OS-specific cache directory
            max_cache_size: 0, // Unlimited
            fast_verification: cache::FAST_VERIFICATION,
            reservation_timeout: Duration::from_secs(180), // 3 minutes (shorter than work timeout)
            auto_cleanup: false,
            min_free_space: 1024 * 1024 * 1024, // 1 GB
        }
    }
}

impl CacheConfig {
    /// Create a new cache configuration with custom cache root
    pub fn with_cache_root(cache_root: PathBuf) -> Self {
        Self {
            cache_root: Some(cache_root),
            ..Default::default()
        }
    }

    /// Set maximum cache size in bytes
    pub fn with_max_cache_size(mut self, max_size: u64) -> Self {
        self.max_cache_size = max_size;
        self
    }

    /// Set reservation timeout
    pub fn with_reservation_timeout(mut self, timeout: Duration) -> Self {
        self.reservation_timeout = timeout;
        self
    }

    /// Enable or disable fast verification
    pub fn with_fast_verification(mut self, enabled: bool) -> Self {
        self.fast_verification = enabled;
        self
    }

    /// Set minimum free space requirement
    pub fn with_min_free_space(mut self, min_space: u64) -> Self {
        self.min_free_space = min_space;
        self
    }

    /// Enable automatic cleanup of old files
    pub fn with_auto_cleanup(mut self, enabled: bool) -> Self {
        self.auto_cleanup = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.cache_root, None);
        assert_eq!(config.max_cache_size, 0);
        assert_eq!(config.fast_verification, cache::FAST_VERIFICATION);
        assert_eq!(config.reservation_timeout, Duration::from_secs(180));
        assert!(!config.auto_cleanup);
        assert_eq!(config.min_free_space, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_builder() {
        let cache_root = PathBuf::from("/tmp/test");
        let config = CacheConfig::with_cache_root(cache_root.clone())
            .with_max_cache_size(1024 * 1024)
            .with_reservation_timeout(Duration::from_secs(60))
            .with_fast_verification(false)
            .with_min_free_space(512 * 1024 * 1024)
            .with_auto_cleanup(true);

        assert_eq!(config.cache_root, Some(cache_root));
        assert_eq!(config.max_cache_size, 1024 * 1024);
        assert_eq!(config.reservation_timeout, Duration::from_secs(60));
        assert!(!config.fast_verification);
        assert_eq!(config.min_free_space, 512 * 1024 * 1024);
        assert!(config.auto_cleanup);
    }
}
