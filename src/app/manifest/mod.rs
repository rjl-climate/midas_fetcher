//! Manifest parsing and streaming functionality
//!
//! This module provides efficient streaming parsing of MIDAS manifest files,
//! with duplicate detection and memory-bounded processing for large manifests.
//! It supports multiple manifest versions, dataset analysis, and flexible
//! filtering capabilities.
//!
//! # Key Features
//!
//! - **Streaming parsing**: Memory-efficient processing of large manifest files
//! - **Duplicate detection**: Prevents redundant file processing with configurable limits
//! - **Version management**: Handles multiple manifest versions with compatibility checking
//! - **Dataset analysis**: Extracts dataset summaries and metadata for interactive selection
//! - **Flexible filtering**: Supports filtering by dataset, county, and quality version
//! - **Configurable URLs**: Uses centralized constants for maintainable URL management
//!
//! # Module Organization
//!
//! - [`types`] - Core data structures (ManifestStats, ManifestConfig, ManifestVersion, DatasetSummary)
//! - [`version_manager`] - Manifest version discovery, compatibility checking, and management
//! - [`streaming`] - Core streaming parser with duplicate detection and memory management
//! - [`analysis`] - Dataset discovery, filtering, and summary generation
//! - [`utils`] - Convenience functions for common operations
//! - [`tests`] - Integration tests for complex scenarios
//!
//! # Examples
//!
//! ## Basic Streaming
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use midas_fetcher::app::manifest::{ManifestStreamer, ManifestConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ManifestConfig::default();
//! let mut streamer = ManifestStreamer::with_config(config);
//! let mut stream = streamer.stream("manifest.txt").await?;
//!
//! while let Some(result) = stream.next().await {
//!     match result {
//!         Ok(file_info) => {
//!             println!("Found file: {}", file_info.file_name);
//!         }
//!         Err(e) => {
//!             eprintln!("Error processing line: {}", e);
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Dataset Analysis
//!
//! ```rust,no_run
//! use midas_fetcher::app::manifest::collect_datasets_and_years;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let datasets = collect_datasets_and_years("manifest.txt").await?;
//!
//! for (name, summary) in &datasets {
//!     println!("Dataset: {}", name);
//!     println!("  Versions: {:?}", summary.versions);
//!     println!("  Counties: {:?}", summary.counties);
//!     println!("  Files: {}", summary.file_count);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Filtering Files
//!
//! ```rust,no_run
//! use midas_fetcher::app::manifest::filter_manifest_files;
//! use midas_fetcher::app::models::QualityControlVersion;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let files = filter_manifest_files(
//!     "manifest.txt",
//!     Some("uk-daily-temperature-obs"),
//!     Some("devon"),
//!     &QualityControlVersion::V1,
//! ).await?;
//!
//! println!("Found {} filtered files", files.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Version Management
//!
//! ```rust,no_run
//! use midas_fetcher::app::manifest::ManifestVersionManager;
//! use midas_fetcher::app::CedaClient;
//! use std::path::PathBuf;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cache_dir = PathBuf::from("./cache");
//! let mut manager = ManifestVersionManager::new(cache_dir);
//! let client = CedaClient::new().await?;
//!
//! // Discover available versions
//! manager.discover_available_versions(&client).await?;
//!
//! // Auto-select latest compatible version
//! if let Some(version) = manager.auto_select_compatible_version(&client).await? {
//!     println!("Selected version: {}", version.version);
//! }
//! # Ok(())
//! # }
//! ```

pub mod analysis;
pub mod streaming;
pub mod types;
pub mod utils;
pub mod version_manager;

#[cfg(test)]
pub mod tests;

// Re-export main public API for backward compatibility
pub use analysis::{
    collect_datasets_and_years, fill_queue_from_manifest, filter_manifest_files,
    filter_manifest_stream, get_selection_options,
};
pub use streaming::ManifestStreamer;
pub use types::{DatasetSummary, ManifestConfig, ManifestStats, ManifestVersion};
pub use utils::{collect_all_files, validate_manifest};
pub use version_manager::{parse_manifest_version, ManifestVersionManager};

// Re-export for convenience
pub use streaming::ManifestStreamer as Streamer;
pub use version_manager::ManifestVersionManager as VersionManager;
