//! Core types for manifest processing
//!
//! This module contains the fundamental data structures used throughout
//! the manifest processing system, including configuration, statistics,
//! and versioning information.

use crate::constants::workers;
use chrono;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        // Resolve the default cache directory (unified with Application Support)
        let destination_root = dirs::config_dir()
            .map(|dir| dir.join("midas-fetcher").join("cache"))
            .unwrap_or_else(|| PathBuf::from("./cache"));

        Self {
            destination_root,
            max_tracked_hashes: 1_000_000, // 1M hashes (~64MB memory)
            allow_duplicates: false,
            progress_batch_size: workers::MANIFEST_BATCH_SIZE,
        }
    }
}

/// Information about a manifest version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestVersion {
    /// Version identifier (e.g., 202407)
    pub version: u32,
    /// Filename of the manifest
    pub filename: String,
    /// Local path to the manifest file
    pub local_path: Option<PathBuf>,
    /// Remote URL for downloading
    pub remote_url: String,
    /// When this manifest was last updated
    pub last_updated: Option<chrono::DateTime<chrono::Utc>>,
    /// Dataset versions referenced in this manifest
    pub dataset_versions: Vec<String>,
    /// Whether this manifest is compatible with current data
    pub is_compatible: Option<bool>,
}

impl ManifestVersion {
    /// Create a new manifest version
    pub fn new(version: u32, filename: String, remote_url: String) -> Self {
        Self {
            version,
            filename,
            local_path: None,
            remote_url,
            last_updated: None,
            dataset_versions: Vec::new(),
            is_compatible: None,
        }
    }

    /// Check if this manifest version is newer than another
    pub fn is_newer_than(&self, other: &ManifestVersion) -> bool {
        self.version > other.version
    }

    /// Get the version as a formatted string (e.g., "202407")
    pub fn version_string(&self) -> String {
        self.version.to_string()
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
