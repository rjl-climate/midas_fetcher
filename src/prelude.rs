//! Prelude module for MIDAS Fetcher Library
//!
//! This module re-exports the most commonly used items from the library,
//! providing a convenient way to import everything needed for typical usage
//! with a single `use midas_fetcher::prelude::*;` statement.
//!
//! # Usage
//!
//! ```rust,no_run
//! use midas_fetcher::prelude::*;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // All common types are now available
//!     let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//!     let client = Arc::new(CedaClient::new().await?);
//!     let queue = Arc::new(WorkQueue::new());
//!     
//!     // Continue with download setup...
//!     Ok(())
//! }
//! ```

// Core result types
pub use crate::errors::{AppError, Result};

// Essential app components that are used in most integrations
pub use crate::app::{
    CacheConfig,
    // Essential components
    CacheManager,
    CacheStats,

    CedaClient,
    ClientConfig,
    // Core orchestration
    Coordinator,
    CoordinatorConfig,

    DatasetFileInfo,
    // Result and status types
    DownloadStats,
    // Data types
    FileInfo,
    ManifestConfig,
    ManifestStreamer,
    Md5Hash,

    QualityControlVersion,
    QueueStats,
    SessionResult,
    WorkQueue,
    WorkQueueConfig,

    // Manifest functions (most commonly used)
    collect_all_files,
    collect_datasets_and_years,
    filter_manifest_files,
};

// Authentication functions
pub use crate::auth::{
    AuthStatus, check_credentials, get_auth_status, setup_credentials, verify_credentials,
};

// Commonly used constants
pub use crate::constants::{
    DEFAULT_RATE_LIMIT_RPS, DEFAULT_WORKER_COUNT, ENV_PASSWORD, ENV_USERNAME, USER_AGENT,
};

// Standard library re-exports that are commonly needed
pub use std::path::{Path, PathBuf};
pub use std::sync::Arc;

// Common external crate re-exports for convenience
// Note: Only re-export types that users will commonly need,
// not the entire crates which would pollute the namespace
pub use tokio;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prelude_imports() {
        // Verify that all essential types are available through prelude
        let _config = CacheConfig::default();
        let _coordinator_config = CoordinatorConfig::default();
        let _client_config = ClientConfig::default();
        let _queue_config = WorkQueueConfig::default();

        // Test that auth functions are available
        let _has_creds = check_credentials();
        let _auth_status = get_auth_status();

        // Test that constants are available
        assert_eq!(DEFAULT_WORKER_COUNT, 8);
        assert!(USER_AGENT.contains("MIDAS-Fetcher"));
    }

    #[tokio::test]
    async fn test_prelude_integration_pattern() {
        // Test that the common integration pattern works with prelude imports
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        // Should be able to create all core components
        let _cache = Arc::new(CacheManager::new(cache_config).await.unwrap());
        let queue = Arc::new(WorkQueue::new());

        // Verify basic functionality
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 0);

        // Test that we can create a coordinator
        let coordinator_config = CoordinatorConfig::default();
        // Note: Not testing full coordinator creation here as it requires a client
        // which needs authentication, but we've verified the types are accessible
        assert_eq!(coordinator_config.worker_count, DEFAULT_WORKER_COUNT);
    }

    #[test]
    fn test_std_reexports() {
        // Test that standard library re-exports work
        let _path = PathBuf::from("/tmp/test");

        // Arc should be available for shared ownership patterns
        let data = Arc::new(42);
        assert_eq!(*data, 42);
    }
}
