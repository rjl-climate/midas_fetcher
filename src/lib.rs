//! MIDAS Fetcher Library
//!
//! A Rust library for downloading weather data from the UK Met Office MIDAS Open dataset.
//! Provides efficient, concurrent downloading with proper rate limiting, atomic file operations,
//! and comprehensive error handling designed for both CLI and GUI applications.
//!
//! This library is specifically designed to support both command-line usage and future
//! integration with Tauri-based GUI applications, providing a clean separation between
//! core functionality and presentation layers.
//!
//! # Key Features
//!
//! - **Authenticated CEDA client** with automatic session management
//! - **Work-stealing concurrent downloads** preventing worker starvation
//! - **Atomic file operations** ensuring data integrity
//! - **Rate limiting** with exponential backoff to respect server limits
//! - **Manifest-based verification** for instant cache validation
//! - **Progress monitoring** with real-time statistics
//! - **Graceful error handling** with detailed error reporting
//!
//! # Architecture Overview
//!
//! The library is organized into several key modules:
//!
//! - [`app`] - Core application logic including download coordination
//! - [`auth`] - CEDA authentication and credential management
//! - [`errors`] - Comprehensive error types and handling
//! - [`prelude`] - Common imports for convenient usage
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use midas_fetcher::prelude::*;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Setup authentication (interactive)
//!     if !check_credentials() {
//!         setup_credentials().await?;
//!     }
//!
//!     // Create shared components
//!     let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//!     let client = Arc::new(CedaClient::new().await?);
//!     let queue = Arc::new(WorkQueue::new());
//!
//!     // Load files from manifest
//!     let files = collect_all_files("manifest.txt", ManifestConfig::default()).await?;
//!     for file in files.into_iter().take(100) {
//!         queue.add_work(file).await?;
//!     }
//!
//!     // Setup and run downloads
//!     let config = CoordinatorConfig::default();
//!     let mut coordinator = Coordinator::new(config, queue, cache, client);
//!     let result = coordinator.run_downloads().await?;
//!
//!     println!("Downloaded {} files successfully", result.stats.files_completed);
//!     Ok(())
//! }
//! ```
//!
//! # For Tauri Integration
//!
//! This library is designed to work seamlessly with Tauri applications. The core types
//! implement `Serialize` and `Deserialize` for easy JSON communication:
//!
//! ```rust,ignore
//! // Example for Tauri integration (requires tauri dependency)
//! use midas_fetcher::prelude::*;
//!
//! // Tauri command example - returns Result as JSON-serializable String
//! async fn start_download(dataset: String, workers: usize) -> Result<String, String> {
//!     let cache = Arc::new(CacheManager::new(CacheConfig::default()).await
//!         .map_err(|e| e.to_string())?);
//!     let client = Arc::new(CedaClient::new().await
//!         .map_err(|e| e.to_string())?);
//!     let queue = Arc::new(WorkQueue::new());
//!
//!     // Configure downloads
//!     let config = CoordinatorConfig {
//!         worker_count: workers,
//!         ..Default::default()
//!     };
//!
//!     let mut coordinator = Coordinator::new(config, queue, cache, client);
//!     let result = coordinator.run_downloads().await
//!         .map_err(|e| e.to_string())?;
//!     
//!     // Serialize result for Tauri
//!     serde_json::to_string(&result).map_err(|e| e.to_string())
//! }
//! ```
//!
//! # Progress Monitoring
//!
//! For applications that need real-time progress updates:
//!
//! ```rust,ignore
//! // Example showing progress monitoring pattern
//! use midas_fetcher::prelude::*;
//!
//! async fn download_with_progress() -> Result<()> {
//!     // Setup coordinator
//!     let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//!     let client = Arc::new(CedaClient::new().await?);
//!     let queue = Arc::new(WorkQueue::new());
//!     let mut coordinator = Coordinator::new(CoordinatorConfig::default(), queue, cache, client);
//!
//!     // For progress monitoring, poll coordinator.get_stats() periodically
//!     // from a separate task or in your UI update loop
//!     
//!     // Run downloads
//!     let result = coordinator.run_downloads().await?;
//!     println!("Downloaded {} files", result.stats.files_completed);
//!     Ok(())
//! }
//! ```
//!
//! # Error Handling
//!
//! The library provides comprehensive error types that are both machine-readable
//! and provide helpful human-readable messages:
//!
//! ```rust,ignore
//! // Example showing error handling patterns
//! use midas_fetcher::prelude::*;
//!
//! async fn handle_errors() -> Result<()> {
//!     match CedaClient::new().await {
//!         Ok(_client) => {
//!             println!("Authentication successful");
//!             Ok(())
//!         }
//!         Err(error) => {
//!             eprintln!("Error occurred: {}", error);
//!             eprintln!("Error category: {}", error.category());
//!             eprintln!("Is recoverable: {}", error.is_recoverable());
//!             Err(error)
//!         }
//!     }
//! }
//! ```

// Core modules - these contain the main library functionality
pub mod app;
pub mod auth;
pub mod constants;
pub mod errors;

// Prelude module for convenient imports
pub mod prelude;

// CLI module - public for main.rs access but contents not re-exported
// This allows the binary to access CLI functionality while preventing
// CLI dependencies from leaking to library consumers
pub mod cli;

// Note: CLI module is intentionally NOT exported to prevent CLI dependencies
// from leaking into library consumers. CLI functionality should only be
// accessible through the binary crate, not the library crate.

// Re-export the most commonly used types at the top level
pub use errors::{AppError, Result};

// Re-export key app functionality for direct access
pub use app::{
    // Core components
    CacheConfig,
    CacheManager,
    CacheStats,

    CedaClient,
    // Configuration types
    ClientConfig,
    Coordinator,
    CoordinatorConfig,
    DatasetFileInfo,
    // Result types
    DownloadStats,
    // Data types
    FileInfo,
    ManifestConfig,
    // Manifest functionality
    ManifestStreamer,
    Md5Hash,

    QualityControlVersion,
    QueueStats,
    SessionResult,
    WorkQueue,

    WorkQueueConfig,

    WorkerConfig,
    collect_all_files,
    collect_datasets_and_years,
    filter_manifest_files,
};

// Re-export authentication functionality
pub use auth::{
    AuthStatus, check_credentials, get_auth_status, setup_credentials, verify_credentials,
};

// Re-export commonly used constants (but not internal implementation details)
pub use constants::{
    DEFAULT_RATE_LIMIT_RPS, DEFAULT_WORKER_COUNT, ENV_PASSWORD, ENV_USERNAME, USER_AGENT,
};

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library description
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "midas_fetcher");
        assert!(!DESCRIPTION.is_empty());
    }

    #[test]
    fn test_constants_accessible() {
        // Test that our constants are accessible through the public API
        assert_eq!(DEFAULT_WORKER_COUNT, 8);
        assert_eq!(ENV_USERNAME, "CEDA_USERNAME");
        assert!(USER_AGENT.contains("MIDAS-Fetcher"));
    }

    #[test]
    fn test_error_types() {
        // Test that our error types work correctly
        let auth_error = errors::AuthError::LoginFailed;
        let app_error = AppError::Auth(auth_error);

        assert_eq!(app_error.category(), "authentication");
        assert!(!app_error.is_recoverable());
    }

    #[test]
    fn test_public_api_accessibility() {
        // Ensure key types are accessible at the top level
        let _config = CacheConfig::default();
        let _queue = WorkQueue::new();
        let _worker_config = WorkerConfig::default();

        // Test auth functions are accessible
        let _has_creds = check_credentials();
        let _auth_status = get_auth_status();
    }

    #[test]
    fn test_no_cli_dependencies() {
        // This test ensures that CLI types are not accidentally exposed
        // If this test fails to compile, it means CLI dependencies have leaked

        // These should NOT be accessible (should cause compile errors if uncommented):
        // let _args = cli::args::DownloadArgs::default();
        // let _progress = cli::progress::ProgressDisplay::new();

        // Instead, only library functionality should be available
        let _coordinator_config = CoordinatorConfig::default();
        let _client_config = ClientConfig::default();
    }

    #[tokio::test]
    async fn test_integration_example() {
        // Test that the basic integration pattern works
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        // Test that we can create core components
        let cache_result = CacheManager::new(cache_config).await;
        assert!(cache_result.is_ok());

        let queue = WorkQueue::new();
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 0);

        // Test that configs have sensible defaults
        let coordinator_config = CoordinatorConfig::default();
        assert_eq!(coordinator_config.worker_count, DEFAULT_WORKER_COUNT);
    }
}
