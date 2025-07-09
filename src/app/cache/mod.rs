//! Cache management system with reservation and atomic operations
//!
//! This module provides a sophisticated cache management system that prevents
//! duplicate downloads through a reservation system, ensures data integrity
//! through atomic file operations, and enables fast verification using
//! manifest-based hash comparison.
//!
//! # Key Features
//!
//! - **OS-specific cache directories**: Uses standard system cache locations
//! - **Reservation system**: Prevents concurrent downloads of the same file
//! - **Atomic operations**: Ensures file integrity with temp-file + rename pattern
//! - **Fast verification**: Manifest-based hash verification for instant cache validation
//! - **Structured storage**: Organizes data files by dataset/quality/county/station, capability files by dataset/capability/county/station
//!
//! # Module Organization
//!
//! - [`config`] - Configuration types and defaults
//! - [`reservation`] - File reservation system for preventing duplicate downloads
//! - [`verification`] - Hash verification and integrity checking
//! - [`path`] - File path generation and organization
//! - [`stats`] - Cache statistics and disk usage monitoring
//! - [`manager`] - Core cache manager with atomic operations
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::cache::{CacheManager, CacheConfig, ReservationStatus};
//! use midas_fetcher::app::models::{FileInfo, DatasetFileInfo, QualityControlVersion};
//! use midas_fetcher::app::hash::Md5Hash;
//! use std::path::PathBuf;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CacheConfig::default();
//! let cache = CacheManager::new(config).await?;
//!
//! // Create a test file info
//! let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e")?;
//! let dataset_info = DatasetFileInfo {
//!     dataset_name: "test-dataset".to_string(),
//!     version: "202407".to_string(),
//!     county: Some("devon".to_string()),
//!     station_id: Some("01381".to_string()),
//!     station_name: Some("twist".to_string()),
//!     quality_version: Some(QualityControlVersion::V1),
//!     year: Some("1980".to_string()),
//!     file_type: Some("data".to_string()),
//! };
//! let file_info = FileInfo {
//!     hash,
//!     relative_path: "./data/test.csv".to_string(),
//!     file_name: "test.csv".to_string(),
//!     dataset_info,
//!     manifest_version: Some(202507),
//!     retry_count: 0,
//!     last_attempt: None,
//!     estimated_size: None,
//!     destination_path: PathBuf::from("/tmp/test.csv"),
//! };
//!
//! // Check if file exists or needs downloading
//! match cache.check_and_reserve(&file_info).await? {
//!     ReservationStatus::AlreadyExists => {
//!         println!("File already cached");
//!     }
//!     ReservationStatus::Reserved => {
//!         // Download the file content (simulation)
//!         let content = b"test file content";
//!         cache.save_file_atomic(content, &file_info).await?;
//!     }
//!     ReservationStatus::ReservedByOther { worker_id } => {
//!         println!("Worker {} is downloading this file", worker_id);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod manager;
pub mod path;
pub mod reservation;
pub mod stats;
pub mod verification;

#[cfg(test)]
pub mod tests;

// Re-export main public API
pub use config::CacheConfig;
pub use manager::CacheManager;
pub use path::PathGenerator;
pub use reservation::{ReservationInfo, ReservationState, ReservationStatus};
pub use stats::CacheStats;
pub use verification::{VerificationFailure, VerificationReport};
