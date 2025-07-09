//! Utility functions for manifest processing
//!
//! This module provides convenience functions for common manifest operations
//! like collecting all files and validating manifest integrity.

use std::path::Path;

use futures::StreamExt;
use tracing::{debug, warn};

use super::streaming::ManifestStreamer;
use super::types::{ManifestConfig, ManifestStats};
use crate::app::models::FileInfo;
use crate::errors::ManifestResult;

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
    let manifest_version =
        ManifestStreamer::extract_manifest_version_from_path(manifest_path.as_ref());
    let mut streamer = ManifestStreamer::with_config_and_version(config, manifest_version);
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
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    async fn create_test_manifest(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    /// Test the convenience function for collecting all valid manifest entries.
    ///
    /// Purpose: Verifies that `collect_all_files()` returns only valid FileInfo objects
    /// while logging errors for invalid entries without returning them.
    /// Benefit: Ensures the utility function provides clean, error-free results.
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
    ///
    /// Purpose: Verifies that `validate_manifest()` processes a manifest and returns
    /// detailed statistics about parsing success/failure rates without storing files.
    /// Benefit: Provides a lightweight way to check manifest quality and integrity.
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

    /// Test sample-based validation.
    ///
    /// Purpose: Verifies that validation can be limited to a specific number of lines
    /// for quick checks of large manifests.
    /// Benefit: Enables fast manifest quality assessment without full processing.
    #[tokio::test]
    async fn test_validate_manifest_with_sample() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv
invalid_hash  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/test2.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01383_twist/test3.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01384_twist/test4.csv"#;

        let manifest_file = create_test_manifest(content).await;

        // Validate only first 2 lines
        let stats = validate_manifest(manifest_file.path(), 2).await.unwrap();

        assert_eq!(stats.lines_processed, 2);
        assert_eq!(stats.valid_entries, 1);
        assert_eq!(stats.invalid_lines, 1);
    }
}
