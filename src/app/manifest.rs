//! Manifest parsing and streaming functionality
//!
//! This module provides efficient streaming parsing of MIDAS manifest files,
//! with duplicate detection and memory-bounded processing for large manifests.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tracing::{debug, error, info, warn};

use crate::app::models::FileInfo;
use crate::constants::workers;
use crate::errors::{ManifestError, ManifestResult};

/// Statistics about manifest processing
#[derive(Debug, Clone, Default)]
pub struct ManifestStats {
    /// Total lines processed
    pub lines_processed: usize,
    /// Valid file entries found
    pub valid_entries: usize,
    /// Invalid/malformed lines skipped
    pub invalid_lines: usize,
    /// Duplicate hashes encountered
    pub duplicate_hashes: usize,
    /// Empty lines skipped
    pub empty_lines: usize,
}

impl ManifestStats {
    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.lines_processed == 0 {
            0.0
        } else {
            (self.valid_entries as f64 / self.lines_processed as f64) * 100.0
        }
    }

    /// Get total skipped lines
    pub fn total_skipped(&self) -> usize {
        self.invalid_lines + self.duplicate_hashes + self.empty_lines
    }
}

/// Configuration for manifest streaming
#[derive(Debug, Clone)]
pub struct ManifestConfig {
    /// Root directory for file destinations
    pub destination_root: PathBuf,
    /// Maximum number of unique hashes to track (memory limit)
    pub max_tracked_hashes: usize,
    /// Whether to include duplicate files (first occurrence only)
    pub allow_duplicates: bool,
    /// Batch size for progress reporting
    pub progress_batch_size: usize,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            destination_root: PathBuf::from("./cache"),
            max_tracked_hashes: 1_000_000, // 1M hashes (~64MB memory)
            allow_duplicates: false,
            progress_batch_size: workers::MANIFEST_BATCH_SIZE,
        }
    }
}

/// Streaming manifest parser with duplicate detection
pub struct ManifestStreamer {
    /// Configuration for parsing
    config: ManifestConfig,
    /// Set of seen hashes for duplicate detection
    seen_hashes: HashSet<String>,
    /// Current processing statistics
    stats: ManifestStats,
    /// Current line number for error reporting
    current_line: usize,
}

impl ManifestStreamer {
    /// Create a new manifest streamer with default configuration
    pub fn new() -> Self {
        Self::with_config(ManifestConfig::default())
    }

    /// Create a new manifest streamer with custom configuration
    pub fn with_config(config: ManifestConfig) -> Self {
        Self {
            config,
            seen_hashes: HashSet::new(),
            stats: ManifestStats::default(),
            current_line: 0,
        }
    }

    /// Stream FileInfo entries from a manifest file
    ///
    /// # Arguments
    ///
    /// * `manifest_path` - Path to the manifest file
    ///
    /// # Returns
    ///
    /// An async stream of `ManifestResult<FileInfo>` that yields valid file entries
    /// while logging errors for malformed lines without stopping processing.
    ///
    /// # Errors
    ///
    /// Returns `ManifestError` if the manifest file cannot be opened or read.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use futures::StreamExt;
    /// use midas_fetcher::app::manifest::ManifestStreamer;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut streamer = ManifestStreamer::new();
    /// let mut stream = streamer.stream("manifest.txt").await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(file_info) => {
    ///             println!("Found file: {}", file_info.file_name);
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Error processing line: {}", e);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream<P: AsRef<Path>>(
        &mut self,
        manifest_path: P,
    ) -> ManifestResult<impl Stream<Item = ManifestResult<FileInfo>> + '_> {
        let file = File::open(manifest_path.as_ref()).await?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        info!(
            "Starting manifest streaming from: {}",
            manifest_path.as_ref().display()
        );

        Ok(FileInfoStream {
            lines,
            streamer: self,
        })
    }

    /// Process a single manifest line
    fn process_line(&mut self, line: String) -> Option<ManifestResult<FileInfo>> {
        self.current_line += 1;
        self.stats.lines_processed += 1;

        // Skip empty lines
        if line.trim().is_empty() {
            self.stats.empty_lines += 1;
            return None;
        }

        // Parse the line
        let (hash, path) = match crate::app::models::parse_manifest_line(&line) {
            Ok((hash, path)) => (hash, path),
            Err(mut e) => {
                // Update line number in error for better diagnostics
                if let ManifestError::InvalidFormat { ref mut line, .. } = e {
                    *line = self.current_line;
                }

                self.stats.invalid_lines += 1;
                warn!(
                    "Skipping malformed line {}: {}",
                    self.current_line,
                    line.trim()
                );

                return Some(Err(e));
            }
        };

        // Check for duplicates
        if !self.config.allow_duplicates {
            if self.seen_hashes.contains(&hash) {
                self.stats.duplicate_hashes += 1;
                debug!("Skipping duplicate hash: {}", hash);
                return None;
            }

            // Check memory limits
            if self.seen_hashes.len() >= self.config.max_tracked_hashes {
                warn!(
                    "Reached maximum tracked hashes limit ({}), clearing cache",
                    self.config.max_tracked_hashes
                );
                self.seen_hashes.clear();
            }

            self.seen_hashes.insert(hash.clone());
        }

        // Create FileInfo
        match FileInfo::new(hash, path, &self.config.destination_root) {
            Ok(file_info) => {
                self.stats.valid_entries += 1;

                // Log progress periodically
                if self.stats.valid_entries % self.config.progress_batch_size == 0 {
                    debug!(
                        "Processed {} valid entries from {} lines ({}% success rate)",
                        self.stats.valid_entries,
                        self.stats.lines_processed,
                        self.stats.success_rate()
                    );
                }

                Some(Ok(file_info))
            }
            Err(e) => {
                self.stats.invalid_lines += 1;
                warn!(
                    "Failed to create FileInfo for line {}: {}",
                    self.current_line, e
                );
                Some(Err(e))
            }
        }
    }

    /// Get current processing statistics
    pub fn stats(&self) -> &ManifestStats {
        &self.stats
    }

    /// Reset the streamer state for reuse
    pub fn reset(&mut self) {
        self.seen_hashes.clear();
        self.stats = ManifestStats::default();
        self.current_line = 0;
    }

    /// Get memory usage estimate in bytes
    pub fn estimated_memory_usage(&self) -> usize {
        // Rough estimate: 32 bytes per hash + HashSet overhead
        self.seen_hashes.len() * 48 + std::mem::size_of::<Self>()
    }
}

impl Default for ManifestStreamer {
    fn default() -> Self {
        Self::new()
    }
}

/// Stream implementation for FileInfo entries
struct FileInfoStream<'a> {
    lines: Lines<BufReader<File>>,
    streamer: &'a mut ManifestStreamer,
}

impl Stream for FileInfoStream<'_> {
    type Item = ManifestResult<FileInfo>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Poll for next line
        match Pin::new(&mut this.lines).poll_next_line(cx) {
            Poll::Ready(Ok(Some(line))) => {
                // Process the line
                match this.streamer.process_line(line) {
                    Some(result) => Poll::Ready(Some(result)),
                    None => {
                        // Line was skipped (empty or duplicate), poll again
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(Ok(None)) => {
                // End of file reached
                let stats = this.streamer.stats();
                info!(
                    "Manifest processing completed: {} valid entries from {} lines ({:.1}% success rate)",
                    stats.valid_entries,
                    stats.lines_processed,
                    stats.success_rate()
                );

                if stats.duplicate_hashes > 0 {
                    info!("Skipped {} duplicate entries", stats.duplicate_hashes);
                }

                if stats.invalid_lines > 0 {
                    warn!("Encountered {} invalid lines", stats.invalid_lines);
                }

                Poll::Ready(None)
            }
            Poll::Ready(Err(e)) => {
                // I/O error reading file
                error!("Error reading manifest file: {}", e);
                Poll::Ready(Some(Err(ManifestError::Io(e))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Convenience function to collect all valid entries from a manifest
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
/// * `config` - Configuration for parsing
///
/// # Returns
///
/// A vector of all valid FileInfo entries, with errors logged but not returned
///
/// # Example
///
/// ```rust,no_run
/// use midas_fetcher::app::manifest::{collect_all_files, ManifestConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ManifestConfig::default();
/// let files = collect_all_files("manifest.txt", config).await?;
/// println!("Found {} files", files.len());
/// # Ok(())
/// # }
/// ```
pub async fn collect_all_files<P: AsRef<Path>>(
    manifest_path: P,
    config: ManifestConfig,
) -> ManifestResult<Vec<FileInfo>> {
    let mut streamer = ManifestStreamer::with_config(config);
    let mut stream = streamer.stream(manifest_path).await?;
    let mut files = Vec::new();

    while let Some(result) = stream.next().await {
        match result {
            Ok(file_info) => files.push(file_info),
            Err(e) => {
                // Log error but continue processing
                warn!("Skipping invalid entry: {}", e);
            }
        }
    }

    Ok(files)
}

/// Validate a manifest file without processing all entries
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
/// * `sample_size` - Number of lines to validate (0 = all lines)
///
/// # Returns
///
/// Statistics about the manifest validation
pub async fn validate_manifest<P: AsRef<Path>>(
    manifest_path: P,
    sample_size: usize,
) -> ManifestResult<ManifestStats> {
    let mut streamer = ManifestStreamer::new();
    let mut stream = streamer.stream(manifest_path).await?;
    let mut processed = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(_) => {
                // Valid entry
            }
            Err(e) => {
                debug!("Validation error: {}", e);
            }
        }

        processed += 1;
        if sample_size > 0 && processed >= sample_size {
            break;
        }
    }

    // Drop the stream to release the mutable borrow on streamer
    drop(stream);

    Ok(streamer.stats().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    async fn create_test_manifest(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    /// Test basic manifest streaming functionality with valid entries.
    /// Verifies that the streamer can parse a simple 2-line manifest file,
    /// correctly extract hashes and paths, and maintain accurate statistics.
    #[tokio::test]
    async fn test_manifest_streaming_basic() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_another/test2.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);

        let files = {
            let mut stream = streamer.stream(manifest_file.path()).await.unwrap();
            let mut files = Vec::new();
            while let Some(result) = stream.next().await {
                files.push(result.unwrap());
            }
            files
        }; // stream is dropped here, ending the mutable borrow

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].hash, "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(files[1].hash, "9734faa872681f96b144f60d29d52011");

        let stats = streamer.stats();
        assert_eq!(stats.valid_entries, 2);
        assert_eq!(stats.lines_processed, 2);
        assert_eq!(stats.invalid_lines, 0);
    }

    /// Test duplicate hash detection and filtering.
    /// Verifies that when `allow_duplicates: false`, only unique file hashes
    /// are processed and duplicate entries are properly tracked in statistics.
    #[tokio::test]
    async fn test_duplicate_detection() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01384_twist/test4.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            allow_duplicates: false,
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);

        let files = {
            let mut stream = streamer.stream(manifest_file.path()).await.unwrap();
            let mut files = Vec::new();
            while let Some(result) = stream.next().await {
                files.push(result.unwrap());
            }
            files
        }; // stream is dropped here, ending the mutable borrow

        // Should only get 3 unique files (duplicate skipped)
        assert_eq!(files.len(), 3);

        let stats = streamer.stats();
        assert_eq!(stats.valid_entries, 3);
        assert_eq!(stats.duplicate_hashes, 1);
        assert_eq!(stats.lines_processed, 4);
    }

    /// Test error handling for malformed manifest lines.
    /// Verifies that the streamer gracefully handles empty lines, invalid hashes,
    /// incorrect path formats, while continuing to process valid entries.
    #[tokio::test]
    async fn test_malformed_lines() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv

invalid_hash  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
50c9d1c465f3cbff652be1509c2e2a4e  data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01384_twist/test4.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);

        let (valid_files, errors) = {
            let mut stream = streamer.stream(manifest_file.path()).await.unwrap();
            let mut valid_files = 0;
            let mut errors = 0;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(_) => valid_files += 1,
                    Err(_) => errors += 1,
                }
            }
            (valid_files, errors)
        }; // stream is dropped here, ending the mutable borrow

        assert_eq!(valid_files, 2); // Only 2 valid files
        assert_eq!(errors, 2); // 2 malformed lines (invalid hash, wrong path)

        let stats = streamer.stats();
        assert_eq!(stats.valid_entries, 2);
        assert_eq!(stats.invalid_lines, 2);
        assert_eq!(stats.empty_lines, 1);
        assert_eq!(stats.lines_processed, 5);
    }

    /// Test the convenience function for collecting all valid manifest entries.
    /// Verifies that `collect_all_files()` returns only valid FileInfo objects
    /// while logging errors for invalid entries without returning them.
    #[tokio::test]
    async fn test_collect_all_files() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv
invalid_hash  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let files = collect_all_files(manifest_file.path(), config)
            .await
            .unwrap();

        // Should only get valid files (errors logged but not returned)
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].hash, "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(files[1].hash, "9734faa872681f96b144f60d29d52011");
    }

    /// Test manifest validation without collecting files.
    /// Verifies that `validate_manifest()` processes a manifest and returns
    /// detailed statistics about parsing success/failure rates without storing files.
    #[tokio::test]
    async fn test_validate_manifest() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv
invalid_hash  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv"#;

        let manifest_file = create_test_manifest(content).await;

        let stats = validate_manifest(manifest_file.path(), 0).await.unwrap();

        assert_eq!(stats.lines_processed, 3);
        assert_eq!(stats.valid_entries, 2);
        assert_eq!(stats.invalid_lines, 1);
        assert!(stats.success_rate() > 60.0);
    }

    /// Test memory-bounded processing for large manifests.
    /// Verifies that when hash tracking limits are exceeded, the cache is cleared
    /// automatically while continuing to process all files without data loss.
    #[tokio::test]
    async fn test_memory_limits() {
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            max_tracked_hashes: 2, // Very small limit for testing
            allow_duplicates: false,
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);

        // Process enough unique hashes to trigger limit
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv
3b71d64ef33dbd6f76497d3ebd4ab976  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01384_twist/test4.csv"#;

        let manifest_file = create_test_manifest(content).await;

        let files = {
            let mut stream = streamer.stream(manifest_file.path()).await.unwrap();
            let mut files = Vec::new();
            while let Some(result) = stream.next().await {
                files.push(result.unwrap());
            }
            files
        }; // stream is dropped here, ending the mutable borrow

        // Should get all 4 files even though we exceeded the hash limit
        assert_eq!(files.len(), 4);

        // Memory usage should be reasonable
        assert!(streamer.estimated_memory_usage() < 1024); // Less than 1KB
    }

    /// Test parsing of real MIDAS manifest file format.
    /// Verifies that the streamer correctly handles actual manifest entries
    /// from CEDA, including both capability and data files with complex paths.
    #[tokio::test]
    async fn test_real_manifest_sample() {
        // Use the actual manifest format from the example
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1992.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let files = collect_all_files(manifest_file.path(), config)
            .await
            .unwrap();

        assert_eq!(files.len(), 2);

        // Check first file (capability file)
        let capability_file = &files[0];
        assert_eq!(capability_file.hash, "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(
            capability_file.dataset_info.dataset_name,
            "uk-daily-temperature-obs"
        );
        assert_eq!(capability_file.dataset_info.version, "202407");
        assert_eq!(
            capability_file.dataset_info.county,
            Some("devon".to_string())
        );
        assert_eq!(
            capability_file.dataset_info.station_id,
            Some("01381".to_string())
        );
        assert_eq!(
            capability_file.dataset_info.station_name,
            Some("twist".to_string())
        );
        assert_eq!(
            capability_file.dataset_info.file_type,
            Some("capability".to_string())
        );

        // Check second file (data file)
        let data_file = &files[1];
        assert_eq!(data_file.hash, "9734faa872681f96b144f60d29d52011");
        assert_eq!(data_file.dataset_info.year, Some("1992".to_string()));
        assert_eq!(
            data_file.dataset_info.quality_version,
            Some(crate::app::models::QualityControlVersion::V1)
        );
        assert_eq!(data_file.dataset_info.file_type, Some("data".to_string()));
    }

    /// Test comprehensive FileInfo generation from manifest entries.
    /// Verifies that complete FileInfo objects are created with all parsed
    /// dataset metadata, proper filenames, and destination paths.
    #[tokio::test]
    async fn test_comprehensive_fileinfo_generation() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let files = collect_all_files(manifest_file.path(), config)
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
        let file_info = &files[0];

        // Test basic fields from manifest line
        assert_eq!(file_info.hash, "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(
            file_info.relative_path,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv"
        );

        // Test extracted filename
        assert_eq!(
            file_info.file_name,
            "midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv"
        );

        // Test comprehensive dataset parsing
        let dataset = &file_info.dataset_info;
        assert_eq!(dataset.dataset_name, "uk-daily-temperature-obs");
        assert_eq!(dataset.version, "202407");
        assert_eq!(dataset.county, Some("devon".to_string()));
        assert_eq!(dataset.station_id, Some("01381".to_string()));
        assert_eq!(dataset.station_name, Some("twist".to_string()));
        assert_eq!(dataset.quality_version, None); // Capability files don't have quality version
        assert_eq!(dataset.year, None); // Capability files don't have year
        assert_eq!(dataset.file_type, Some("capability".to_string()));

        // Test destination path construction
        let expected_dest = temp_dir.path().join("data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv");
        assert_eq!(file_info.destination_path, expected_dest);

        // Test initial state
        assert_eq!(file_info.retry_count, 0);
        assert!(file_info.last_attempt.is_none());
        assert!(file_info.estimated_size.is_none());

        // Test download URL generation
        let download_url = file_info.download_url("https://data.ceda.ac.uk");
        assert_eq!(
            download_url,
            "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv"
        );

        // Test display name generation
        let display_name = dataset.display_name();
        assert_eq!(display_name, "uk-daily-temperature-obs-v202407-devon-twist");

        println!("✅ Generated complete FileInfo from manifest line:");
        println!("   Hash: {}", file_info.hash);
        println!("   Dataset: {}", dataset.dataset_name);
        println!("   County: {:?}", dataset.county);
        println!(
            "   Station: {} ({})",
            dataset.station_name.as_ref().unwrap(),
            dataset.station_id.as_ref().unwrap()
        );
        println!("   File Type: {:?}", dataset.file_type);
        println!("   Download URL: {}", download_url);
    }
}
