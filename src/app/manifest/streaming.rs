//! Manifest streaming functionality for memory-efficient processing
//!
//! This module provides efficient streaming parsing of MIDAS manifest files,
//! with duplicate detection and memory-bounded processing for large manifests.

use std::collections::HashSet;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tracing::{debug, error, info, warn};

use super::types::{ManifestConfig, ManifestStats};
use super::version_manager::parse_manifest_version;
use crate::app::hash::Md5Hash;
use crate::app::models::{parse_manifest_line, FileInfo};
use crate::errors::{ManifestError, ManifestResult};

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
    /// Manifest version for folder naming
    manifest_version: Option<u32>,
}

impl ManifestStreamer {
    /// Extract manifest version from a path (e.g., "/path/to/midas-open-v202407-md5s.txt" -> Some(202407))
    pub fn extract_manifest_version_from_path(manifest_path: &Path) -> Option<u32> {
        if let Some(filename) = manifest_path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                return parse_manifest_version(filename_str);
            }
        }
        None
    }

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
            manifest_version: None,
        }
    }

    /// Create a new manifest streamer with manifest version
    pub fn with_version(manifest_version: Option<u32>) -> Self {
        Self {
            config: ManifestConfig::default(),
            seen_hashes: HashSet::new(),
            stats: ManifestStats::default(),
            current_line: 0,
            manifest_version,
        }
    }

    /// Create a new manifest streamer with custom configuration and manifest version
    pub fn with_config_and_version(config: ManifestConfig, manifest_version: Option<u32>) -> Self {
        Self {
            config,
            seen_hashes: HashSet::new(),
            stats: ManifestStats::default(),
            current_line: 0,
            manifest_version,
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

        // Extract manifest version from filename if not already set
        if self.manifest_version.is_none() {
            if let Some(filename) = manifest_path.as_ref().file_name() {
                if let Some(filename_str) = filename.to_str() {
                    self.manifest_version = parse_manifest_version(filename_str);
                }
            }
        }

        info!(
            "Starting manifest streaming from: {} (version: {:?})",
            manifest_path.as_ref().display(),
            self.manifest_version
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
        match FileInfo::new(
            hash,
            path,
            &self.config.destination_root,
            self.manifest_version,
        ) {
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
pub struct FileInfoStream<'a> {
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
    ///
    /// Purpose: Verifies that the streamer can parse a simple 2-line manifest file,
    /// correctly extract hashes and paths, and maintain accurate statistics.
    /// Benefit: Ensures core streaming functionality works as expected.
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
    ///
    /// Purpose: Verifies that when `allow_duplicates: false`, only unique file hashes
    /// are processed and duplicate entries are properly tracked in statistics.
    /// Benefit: Ensures duplicate detection prevents redundant processing.
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

    /// Test memory-bounded processing for large manifests.
    ///
    /// Purpose: Verifies that when hash tracking limits are exceeded, the cache is cleared
    /// automatically while continuing to process all files without data loss.
    /// Benefit: Ensures memory usage remains bounded for very large manifests.
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
}
