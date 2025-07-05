//! Core application logic for MIDAS Fetcher
//!
//! This module contains the main application components including the HTTP client,
//! data models, manifest parsing, caching system, and orchestration logic.
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::{CacheManager, CacheConfig, CedaClient, ManifestStreamer, ReservationStatus};
//! use futures::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create cache manager
//! let cache = CacheManager::new(CacheConfig::default()).await?;
//!
//! // Create authenticated CEDA client
//! let client = CedaClient::new().await?;
//!
//! // Stream files from a manifest
//! let mut streamer = ManifestStreamer::new();
//! let mut stream = streamer.stream("manifest.txt").await?;
//!
//! while let Some(result) = stream.next().await {
//!     match result {
//!         Ok(file_info) => {
//!             println!("Found file: {}", file_info.file_name);
//!             
//!             // Check cache and reserve if needed
//!             match cache.check_and_reserve(&file_info).await? {
//!                 ReservationStatus::AlreadyExists => {
//!                     println!("File already cached");
//!                 }
//!                 ReservationStatus::Reserved => {
//!                     // Download and save file (simplified for doctest)
//!                     let url = file_info.download_url("https://data.ceda.ac.uk");
//!                     println!("Would download from: {}", url);
//!                     // In real code: let content = client.download_file(&url, &destination, false).await?;
//!                     let content = b"simulated file content";
//!                     cache.save_file_atomic(content, &file_info).await?;
//!                 }
//!                 ReservationStatus::ReservedByOther { worker_id } => {
//!                     println!("Worker {} is downloading this file", worker_id);
//!                 }
//!             }
//!         }
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod cache;
pub mod client;
pub mod hash;
pub mod manifest;
pub mod models;
pub mod queue;

// Re-export main public API
pub use cache::{
    CacheConfig, CacheManager, CacheStats, ReservationInfo, ReservationState, ReservationStatus,
    VerificationFailure, VerificationReport,
};
pub use client::{CedaClient, ClientConfig};
pub use hash::Md5Hash;
pub use manifest::{
    ManifestConfig, ManifestStats, ManifestStreamer, collect_all_files, validate_manifest,
};
pub use models::{
    DatasetInfo, FileInfo, QualityControlVersion, generate_file_id, parse_manifest_line,
};
pub use queue::{QueueStats, WorkInfo, WorkQueue, WorkQueueConfig, WorkStatus};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_structure() {
        // Ensure public API is accessible
        let config = ClientConfig::default();
        assert!(config.tcp_nodelay);
    }
}
