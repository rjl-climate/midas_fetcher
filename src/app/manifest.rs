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

use crate::app::hash::Md5Hash;
use crate::app::models::{parse_manifest_line, FileInfo};
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
    seen_hashes: HashSet<Md5Hash>,
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
        let (hash, path) = match parse_manifest_line(&line) {
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

            self.seen_hashes.insert(hash);
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
        assert_eq!(files[0].hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(files[1].hash.to_hex(), "9734faa872681f96b144f60d29d52011");

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
        assert_eq!(files[0].hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(files[1].hash.to_hex(), "9734faa872681f96b144f60d29d52011");
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
        assert_eq!(
            capability_file.hash.to_hex(),
            "50c9d1c465f3cbff652be1509c2e2a4e"
        );
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
        assert_eq!(data_file.hash.to_hex(), "9734faa872681f96b144f60d29d52011");
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
        assert_eq!(file_info.hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
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

    /// Test that collect_datasets_and_years extracts dataset version years correctly
    #[tokio::test]
    async fn test_collect_datasets_version_years() {
        // Create test manifest with 202507 version (should extract 2025 as dataset version year)
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01381_twist_qcv-1_1993.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01382_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01382_twist_qcv-1_1994.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let datasets = collect_datasets_and_years(manifest_file.path())
            .await
            .unwrap();

        assert_eq!(datasets.len(), 1);

        let temp_dataset = datasets.get("uk-daily-temperature-obs").unwrap();

        // Should extract "202507" as the complete dataset version, not "1993" or "1994" data years
        assert_eq!(temp_dataset.versions, vec!["202507".to_string()]);
        assert_eq!(temp_dataset.file_count, 2);
    }

    /// Test that filter_manifest_files returns all dataset files from manifest
    #[tokio::test]
    async fn test_filter_by_dataset_version_year() {
        // Create test manifest with files from multiple dataset versions
        // Note: In practice each manifest represents one dataset version, but this tests filtering
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01381_twist_qcv-1_1993.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01382_twist_qcv-1_1994.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01383_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01383_twist_qcv-1_1995.csv"#;

        let manifest_file = create_test_manifest(content).await;

        // Filter by dataset only - should return ALL files for that dataset from the manifest
        let files = filter_manifest_files(
            manifest_file.path(),
            Some("uk-daily-temperature-obs"),
            None,
            &crate::app::models::QualityControlVersion::V1,
            false,
            false,
        )
        .await
        .unwrap();

        // Should return all 3 files (from both dataset versions in this manifest)
        assert_eq!(files.len(), 3);

        // Files can be from different dataset versions - that's expected now
        let mut versions: Vec<String> = files
            .iter()
            .map(|f| f.dataset_info.version.clone())
            .collect();
        versions.sort();
        versions.dedup();
        assert!(versions.contains(&"202407".to_string()));
        assert!(versions.contains(&"202507".to_string()));
    }

    /// Test complete workflow: dataset collection and filtering consistency
    #[tokio::test]
    async fn test_dataset_year_selection_and_filtering_consistency() {
        // Create test manifest with multiple dataset versions
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01381_twist_qcv-1_1993.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01382_twist_qcv-1_1994.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-rain-obs/dataset-version-202507/durham/01892_stanhope/qc-version-0/midas-open_uk-daily-rain-obs_dv-202507_durham_01892_stanhope_qcv-0_1995.csv"#;

        let manifest_file = create_test_manifest(content).await;

        // Step 1: Collect datasets and their versions from the manifest
        let datasets = collect_datasets_and_years(manifest_file.path())
            .await
            .unwrap();

        // Verify we get complete dataset versions, not data years
        let temp_dataset = datasets.get("uk-daily-temperature-obs").unwrap();
        assert_eq!(temp_dataset.versions, vec!["202407", "202507"]); // Complete dataset versions, not 1993, 1994

        let rain_dataset = datasets.get("uk-daily-rain-obs").unwrap();
        assert_eq!(rain_dataset.versions, vec!["202507"]); // Complete dataset version, not 1995

        // Step 2: Test that filtering by dataset returns all files for that dataset
        // (Note: No year-based filtering anymore - each manifest represents one time period)
        let temp_files = filter_manifest_files(
            manifest_file.path(),
            Some("uk-daily-temperature-obs"),
            None,
            &crate::app::models::QualityControlVersion::V1,
            false,
            false,
        )
        .await
        .unwrap();

        // Should find all temperature files (from both versions in this test manifest)
        assert_eq!(temp_files.len(), 2, "Should find both temperature files");

        // Verify files are from the expected dataset
        for file in &temp_files {
            assert_eq!(file.dataset_info.dataset_name, "uk-daily-temperature-obs");
            assert!(temp_dataset.versions.contains(&file.dataset_info.version));
        }

        // Test rain dataset filtering
        let rain_files = filter_manifest_files(
            manifest_file.path(),
            Some("uk-daily-rain-obs"),
            None,
            &crate::app::models::QualityControlVersion::V0, // Note: rain uses QCV0
            false,
            false,
        )
        .await
        .unwrap();

        assert_eq!(rain_files.len(), 1, "Should find one rain file");
        assert_eq!(rain_files[0].dataset_info.dataset_name, "uk-daily-rain-obs");
        assert_eq!(rain_files[0].dataset_info.version, "202507");

        println!("✅ Dataset collection and filtering are consistent!");
        println!(
            "   Temperature dataset versions: {:?}",
            temp_dataset.versions
        );
        println!("   Rain dataset versions: {:?}", rain_dataset.versions);
    }
}

/// Information about available datasets and versions from manifest analysis
#[derive(Debug, Clone)]
pub struct DatasetSummary {
    /// Dataset name (e.g., "uk-daily-temperature-obs")
    pub name: String,
    /// Available dataset versions for this dataset (e.g., ["202407", "202507"])
    pub versions: Vec<String>,
    /// Available counties/regions
    pub counties: Vec<String>,
    /// Available quality control versions
    pub quality_versions: Vec<crate::app::models::QualityControlVersion>,
    /// Individual data years found in files (e.g., ["1980", "1981", "2023"])
    pub years: Vec<String>,
    /// Total number of files for this dataset
    pub file_count: usize,
    /// Example file for reference
    pub example_file: Option<String>,
}

impl DatasetSummary {
    /// Get the latest available version
    pub fn latest_version(&self) -> Option<&String> {
        self.versions.iter().max()
    }

    /// Get the latest available year (for backward compatibility)
    pub fn latest_year(&self) -> Option<&String> {
        self.latest_version()
    }

    /// Check if a specific version is available
    pub fn has_version(&self, version: &str) -> bool {
        self.versions.contains(&version.to_string())
    }

    /// Check if a specific year is available (for backward compatibility)
    pub fn has_year(&self, year: &str) -> bool {
        self.has_version(year)
    }

    /// Check if a specific county is available
    pub fn has_county(&self, county: &str) -> bool {
        self.counties.iter().any(|c| c == county)
    }

    /// Check if a specific quality version is available
    pub fn has_quality_version(&self, qv: &crate::app::models::QualityControlVersion) -> bool {
        self.quality_versions.contains(qv)
    }

    /// Get the earliest available data year
    pub fn earliest_year(&self) -> Option<&String> {
        self.years.iter().min()
    }

    /// Get the latest available data year
    pub fn latest_data_year(&self) -> Option<&String> {
        self.years.iter().max()
    }

    /// Format the year range as a string (e.g., "1980-2023" or "1980" if single year)
    pub fn year_range(&self) -> String {
        if self.years.is_empty() {
            return "N/A".to_string();
        }

        let earliest = self.earliest_year().unwrap();
        let latest = self.latest_data_year().unwrap();

        if earliest == latest {
            earliest.clone()
        } else {
            format!("{}-{}", earliest, latest)
        }
    }
}

/// Collect available datasets and dataset versions from a manifest file
///
/// This function analyzes a manifest file to discover what datasets and
/// dataset versions are available. It extracts complete dataset version
/// strings from the directory structure (e.g., "dataset-version-202507" -> "202507")
/// rather than individual data years from filenames.
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
///
/// # Returns
///
/// A map of dataset names to their available metadata
///
/// # Errors
///
/// Returns `ManifestError` if the manifest cannot be read or parsed
pub async fn collect_datasets_and_years<P: AsRef<Path>>(
    manifest_path: P,
) -> ManifestResult<std::collections::HashMap<String, DatasetSummary>> {
    use futures::StreamExt;
    use std::collections::HashMap;

    let config = ManifestConfig::default();
    let mut streamer = ManifestStreamer::with_config(config);
    let mut stream = streamer.stream(manifest_path).await?;

    let mut datasets: HashMap<String, DatasetSummary> = HashMap::new();

    while let Some(result) = stream.next().await {
        let file_info = result?;
        let dataset_info = &file_info.dataset_info;
        let dataset_name = dataset_info.dataset_name.clone();

        let entry = datasets
            .entry(dataset_name.clone())
            .or_insert_with(|| DatasetSummary {
                name: dataset_name,
                versions: Vec::new(),
                counties: Vec::new(),
                quality_versions: Vec::new(),
                years: Vec::new(),
                file_count: 0,
                example_file: None,
            });

        // Update file count
        entry.file_count += 1;

        // Set example file if not already set
        if entry.example_file.is_none() {
            entry.example_file = Some(file_info.relative_path.clone());
        }

        // Collect complete dataset version strings
        let version = &dataset_info.version;
        if !entry.versions.contains(version) {
            entry.versions.push(version.clone());
        }

        // Collect counties
        if let Some(ref county) = dataset_info.county {
            if !entry.counties.contains(county) {
                entry.counties.push(county.clone());
            }
        }

        // Collect quality versions
        if let Some(ref qv) = dataset_info.quality_version {
            if !entry.quality_versions.contains(qv) {
                entry.quality_versions.push(qv.clone());
            }
        }

        // Collect individual data years (skip capability files that don't have years)
        if let Some(ref year) = dataset_info.year {
            if !entry.years.contains(year) {
                entry.years.push(year.clone());
            }
        }
    }

    // Sort collected data for consistent output
    for summary in datasets.values_mut() {
        summary.versions.sort();
        summary.counties.sort();
        summary.years.sort();
        summary.quality_versions.sort_by_key(|qv| match qv {
            crate::app::models::QualityControlVersion::V0 => 0,
            crate::app::models::QualityControlVersion::V1 => 1,
        });
    }

    debug!("Discovered {} datasets from manifest", datasets.len());

    Ok(datasets)
}

/// Filter files from manifest based on criteria
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
/// * `dataset_name` - Optional dataset filter
/// * `county` - Optional county filter
/// * `quality_version` - Quality control version to filter by
/// * `metadata_only` - Only include metadata/capability files
/// * `data_only` - Only include data files (exclude metadata)
///
/// # Returns
///
/// Vector of FileInfo matching the criteria
pub async fn filter_manifest_files<P: AsRef<Path>>(
    manifest_path: P,
    dataset_name: Option<&str>,
    county: Option<&str>,
    quality_version: &crate::app::models::QualityControlVersion,
    metadata_only: bool,
    data_only: bool,
) -> ManifestResult<Vec<FileInfo>> {
    use futures::StreamExt;

    let config = ManifestConfig::default();
    let mut streamer = ManifestStreamer::with_config(config);
    let mut stream = streamer.stream(manifest_path).await?;

    let mut filtered_files = Vec::new();

    while let Some(result) = stream.next().await {
        let file_info = result?;
        let dataset_info = &file_info.dataset_info;

        // Apply dataset filter
        if let Some(filter_dataset) = dataset_name {
            if dataset_info.dataset_name != filter_dataset {
                continue;
            }
        }

        // Apply county filter
        if let Some(filter_county) = county {
            if dataset_info.county.as_ref() != Some(&filter_county.to_string()) {
                continue;
            }
        }

        // Apply quality version filter (only for data files)
        if let Some(ref file_qv) = dataset_info.quality_version {
            if file_qv != quality_version {
                continue;
            }
        } else {
            // File has no quality version (capability/metadata file)
            // Include it unless data_only is specified
            if data_only {
                continue;
            }
        }

        // Apply file type filters
        if let Some(ref file_type) = dataset_info.file_type {
            if metadata_only && file_type != "capability" && file_type != "metadata" {
                continue;
            }
            if data_only && (file_type == "capability" || file_type == "metadata") {
                continue;
            }
        }

        filtered_files.push(file_info);
    }

    info!(
        "Filtered to {} files matching criteria",
        filtered_files.len()
    );

    Ok(filtered_files)
}

/// Get a summary of available options for interactive selection
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
///
/// # Returns
///
/// A tuple of (datasets, all_versions) for selection menus
pub async fn get_selection_options<P: AsRef<Path>>(
    manifest_path: P,
) -> ManifestResult<(Vec<String>, Vec<String>)> {
    use std::collections::HashSet;

    let datasets_map = collect_datasets_and_years(manifest_path).await?;

    let mut all_datasets: Vec<String> = datasets_map.keys().cloned().collect();
    all_datasets.sort();

    let mut all_versions: HashSet<String> = HashSet::new();
    for summary in datasets_map.values() {
        all_versions.extend(summary.versions.iter().cloned());
    }

    let mut versions_vec: Vec<String> = all_versions.into_iter().collect();
    versions_vec.sort();

    Ok((all_datasets, versions_vec))
}
