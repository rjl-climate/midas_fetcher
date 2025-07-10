//! Core application logic for MIDAS Fetcher
//!
//! This module contains the main application components including the HTTP client,
//! data models, manifest parsing, caching system, and orchestration logic.
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::{
//!     CacheManager, CacheConfig, CedaClient, ManifestStreamer, WorkQueue,
//!     WorkerPool, WorkerConfig
//! };
//! use futures::StreamExt;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create shared components
//! let cache = Arc::new(CacheManager::new(CacheConfig::default()).await?);
//! let client = Arc::new(CedaClient::new().await?);
//! let queue = Arc::new(WorkQueue::new());
//!
//! // Load work into queue from manifest
//! let mut streamer = ManifestStreamer::new();
//! let mut stream = streamer.stream("manifest.txt").await?;
//! while let Some(result) = stream.next().await {
//!     match result {
//!         Ok(file_info) => {
//!             queue.add_work(file_info).await?;
//!         }
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//!
//! // Create and start worker pool
//! let config = WorkerConfig::default();
//! let mut pool = WorkerPool::new(config, queue, cache, client);
//!
//! let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(100);
//! pool.start(progress_tx).await?;
//!
//! // Monitor progress
//! tokio::spawn(async move {
//!     while let Some(progress) = progress_rx.recv().await {
//!         println!("Worker {}: {} files completed",
//!                  progress.worker_id, progress.files_completed);
//!     }
//! });
//!
//! // Shutdown when work is complete (simplified)
//! pool.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod cache;
pub mod client;
pub mod coordinator;
pub mod hash;
pub mod manifest;
pub mod models;
pub mod queue;
pub mod worker;

// Re-export main public API
pub use cache::{
    CacheConfig, CacheManager, CacheStats, PathGenerator, ReservationInfo, ReservationState,
    ReservationStatus, VerificationFailure, VerificationReport,
};
pub use client::{CedaClient, ClientConfig};
pub use coordinator::{Coordinator, CoordinatorConfig, DownloadStats, SessionResult};
pub use hash::Md5Hash;
pub use manifest::{
    collect_all_files, collect_datasets_and_years, fill_queue_from_manifest, filter_manifest_files,
    get_selection_options, validate_manifest, DatasetSummary, ManifestConfig, ManifestStats,
    ManifestStreamer, ManifestVersionManager,
};
pub use models::{
    generate_file_id, parse_manifest_line, DatasetFileInfo, FileInfo, QualityControlVersion,
};
pub use queue::{
    ConfigPresets, QueueStats, StatsReporter, WorkInfo, WorkQueue, WorkQueueConfig, WorkStatus,
};
pub use worker::{
    ConfigPresets as WorkerConfigPresets, DownloadWorker, PerformanceMetrics,
    StatsReporter as WorkerStatsReporter, WorkerConfig, WorkerPool, WorkerPoolStats,
    WorkerProgress, WorkerStatus,
};

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
