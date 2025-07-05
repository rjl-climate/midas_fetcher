//! Core application logic for MIDAS Fetcher
//!
//! This module contains the main application components including the HTTP client,
//! data models, manifest parsing, caching system, and orchestration logic.
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::{CedaClient, ManifestStreamer};
//! use futures::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
//!             // Download using client
//!             let url = file_info.download_url("https://data.ceda.ac.uk");
//!             // client.download_file(...).await?;
//!         }
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod manifest;
pub mod models;
pub mod queue;

// Re-export main public API
pub use client::{CedaClient, ClientConfig};
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
