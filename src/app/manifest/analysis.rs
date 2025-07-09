//! Dataset analysis and filtering functionality
//!
//! This module provides functions to analyze manifest files for available datasets,
//! filter files based on criteria, and generate summaries for user selection.

use std::collections::HashMap;
use std::path::Path;

use futures::StreamExt;
use tracing::{debug, info, warn};

use super::streaming::ManifestStreamer;
use super::types::{DatasetSummary, ManifestConfig};
use crate::app::models::{FileInfo, QualityControlVersion};
use crate::errors::{ManifestError, ManifestResult};

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
) -> ManifestResult<HashMap<String, DatasetSummary>> {
    let config = ManifestConfig::default();
    let manifest_version =
        ManifestStreamer::extract_manifest_version_from_path(manifest_path.as_ref());
    let mut streamer = ManifestStreamer::with_config_and_version(config, manifest_version);
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

        // Apply default QC version filtering - only count QC version 1 files and non-QC files
        // This matches the default download behavior
        let should_count = match &dataset_info.quality_version {
            Some(qv) => qv == &QualityControlVersion::V1, // Only count QC version 1
            None => true, // Always count non-QC files (capability files, etc.)
        };

        if should_count {
            // Update file count
            entry.file_count += 1;

            // Set example file if not already set
            if entry.example_file.is_none() {
                entry.example_file = Some(file_info.relative_path.clone());
            }
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
            QualityControlVersion::V0 => 0,
            QualityControlVersion::V1 => 1,
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
///
/// # Returns
///
/// Vector of FileInfo matching the criteria
pub async fn filter_manifest_files<P: AsRef<Path>>(
    manifest_path: P,
    dataset_name: Option<&str>,
    county: Option<&str>,
    quality_version: &QualityControlVersion,
) -> ManifestResult<Vec<FileInfo>> {
    let config = ManifestConfig::default();
    let manifest_version =
        ManifestStreamer::extract_manifest_version_from_path(manifest_path.as_ref());
    let mut streamer = ManifestStreamer::with_config_and_version(config, manifest_version);
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

        // Apply quality version filter for data files only
        // Always include special files (station-metadata, change-log, station-log) and capability files
        if let Some(ref file_qv) = dataset_info.quality_version {
            if file_qv != quality_version {
                continue;
            }
        }
        // Files without quality version (capability files and special files) are always included

        filtered_files.push(file_info);
    }

    info!(
        "Filtered to {} files matching criteria",
        filtered_files.len()
    );

    Ok(filtered_files)
}

/// Create a filtered stream of FileInfo entries from a manifest
///
/// This function returns a stream that yields FileInfo entries matching the
/// specified criteria, allowing for memory-efficient processing of large manifests.
/// Unlike `filter_manifest_files`, this doesn't collect all entries into memory.
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
/// * `dataset_name` - Optional dataset name filter
/// * `county` - Optional county filter
/// * `quality_version` - Quality version filter
///
/// # Returns
///
/// A stream of FileInfo entries that match the specified criteria
pub async fn filter_manifest_stream<P: AsRef<Path>>(
    manifest_path: P,
    dataset_name: Option<&str>,
    county: Option<&str>,
    quality_version: &QualityControlVersion,
) -> ManifestResult<impl futures::Stream<Item = FileInfo>> {
    // For now, use the existing function to avoid lifetime issues
    // This is a transitional implementation - in the future we can optimize further
    let files = filter_manifest_files(manifest_path, dataset_name, county, quality_version).await?;

    // Convert to stream
    let file_stream = futures::stream::iter(files);

    Ok(file_stream)
}

/// Fill a work queue directly from a manifest stream with pull-based backpressure
///
/// This function implements true streaming where the queue pulls from the manifest
/// only when it has capacity. No intermediate collection into Vec occurs.
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file
/// * `queue` - Work queue to fill
/// * `dataset_name` - Optional dataset name filter
/// * `county` - Optional county filter
/// * `quality_version` - Quality version filter
/// * `limit` - Optional limit on number of files to process
///
/// # Returns
///
/// Number of files successfully added to the queue
pub async fn fill_queue_from_manifest<P: AsRef<Path>>(
    manifest_path: P,
    queue: &crate::app::queue::WorkQueue,
    dataset_name: Option<&str>,
    county: Option<&str>,
    quality_version: &QualityControlVersion,
    limit: Option<usize>,
) -> ManifestResult<usize> {
    let config = ManifestConfig::default();
    let manifest_version =
        ManifestStreamer::extract_manifest_version_from_path(manifest_path.as_ref());
    let mut streamer = ManifestStreamer::with_config_and_version(config, manifest_version);
    let mut stream = streamer.stream(manifest_path).await?;

    let mut added_count = 0;
    let mut processed_count = 0;

    info!("Starting pull-based manifest processing");

    while let Some(result) = stream.next().await {
        // Check if we've hit the limit
        if let Some(limit) = limit {
            if processed_count >= limit {
                info!("Reached limit of {} files", limit);
                break;
            }
        }

        let file_info = match result {
            Ok(file_info) => file_info,
            Err(e) => {
                warn!("Skipping invalid entry: {}", e);
                continue;
            }
        };

        // Apply filters
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
        }

        processed_count += 1;

        // Pull-based backpressure: only add if queue has capacity
        while !queue.has_capacity().await {
            // Wait for workers to consume some items
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Add to queue
        if queue.add_work(file_info).await.map_err(|e| {
            ManifestError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })? {
            added_count += 1;
        }

        // Log progress periodically
        if processed_count % 10000 == 0 {
            info!(
                "Processed {} files, added {} to queue",
                processed_count, added_count
            );
        }
    }

    info!(
        "Manifest processing complete: processed {}, added {}",
        processed_count, added_count
    );
    Ok(added_count)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn create_test_manifest(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    /// Test that collect_datasets_and_years extracts dataset version years correctly
    ///
    /// Purpose: Verifies that dataset analysis correctly extracts complete dataset versions
    /// rather than individual data years from filenames.
    /// Benefit: Ensures proper understanding of dataset versioning structure.
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
    ///
    /// Purpose: Verifies that filtering by dataset name returns all files for that dataset
    /// regardless of dataset version or other attributes.
    /// Benefit: Ensures proper dataset-based filtering functionality.
    #[tokio::test]
    async fn test_filter_by_dataset() {
        // Create test manifest with files from multiple dataset versions
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01381_twist_qcv-1_1993.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01382_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01382_twist_qcv-1_1994.csv
ef4718f5cb7b83d0f7bb24a3a598b3a7  ./data/uk-daily-temperature-obs/dataset-version-202507/devon/01383_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202507_devon_01383_twist_qcv-1_1995.csv"#;

        let manifest_file = create_test_manifest(content).await;

        // Filter by dataset only - should return ALL files for that dataset from the manifest
        let files = filter_manifest_files(
            manifest_file.path(),
            Some("uk-daily-temperature-obs"),
            None,
            &QualityControlVersion::V1,
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
    ///
    /// Purpose: Verifies that dataset discovery and subsequent filtering produce
    /// consistent and expected results across multiple datasets and versions.
    /// Benefit: Ensures the analysis and filtering workflow integrates properly.
    #[tokio::test]
    async fn test_dataset_analysis_and_filtering_consistency() {
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
        let temp_files = filter_manifest_files(
            manifest_file.path(),
            Some("uk-daily-temperature-obs"),
            None,
            &QualityControlVersion::V1,
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
            &QualityControlVersion::V0, // Note: rain uses QCV0
        )
        .await
        .unwrap();

        assert_eq!(rain_files.len(), 1, "Should find one rain file");
        assert_eq!(rain_files[0].dataset_info.dataset_name, "uk-daily-rain-obs");
        assert_eq!(rain_files[0].dataset_info.version, "202507");
    }
}
