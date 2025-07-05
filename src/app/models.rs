//! Data models for MIDAS Fetcher
//!
//! This module defines the core data structures used throughout the application,
//! including file information, dataset metadata, and manifest parsing logic.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::app::hash::Md5Hash;
use crate::errors::{ManifestError, ManifestResult};

/// Quality control version for MIDAS data files
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualityControlVersion {
    /// Quality control version 0 (qcv-0)
    V0,
    /// Quality control version 1 (qcv-1)
    V1,
}

impl QualityControlVersion {
    /// Convert from directory name format (e.g., "qc-version-1")
    pub fn from_directory_name(name: &str) -> Option<Self> {
        match name {
            "qc-version-0" => Some(Self::V0),
            "qc-version-1" => Some(Self::V1),
            _ => None,
        }
    }

    /// Convert from filename format (e.g., "qcv-1")
    pub fn from_filename_format(name: &str) -> Option<Self> {
        match name {
            "qcv-0" => Some(Self::V0),
            "qcv-1" => Some(Self::V1),
            _ => None,
        }
    }

    /// Get the directory name format (e.g., "qc-version-1")
    pub fn to_directory_name(&self) -> &'static str {
        match self {
            Self::V0 => "qc-version-0",
            Self::V1 => "qc-version-1",
        }
    }

    /// Get the filename format (e.g., "qcv-1")
    pub fn to_filename_format(&self) -> &'static str {
        match self {
            Self::V0 => "qcv-0",
            Self::V1 => "qcv-1",
        }
    }
}

impl std::fmt::Display for QualityControlVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_filename_format())
    }
}

/// Information about a MIDAS dataset extracted from file paths
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetInfo {
    /// Dataset name (e.g., "uk-daily-temperature-obs")
    pub dataset_name: String,
    /// Version string (e.g., "202407")
    pub version: String,
    /// County/region name (e.g., "devon")
    pub county: Option<String>,
    /// Station ID (e.g., "01381")
    pub station_id: Option<String>,
    /// Station name (e.g., "twist")
    pub station_name: Option<String>,
    /// Quality control version (e.g., QualityControlVersion::V1)
    pub quality_version: Option<QualityControlVersion>,
    /// Year for the data (e.g., "1980")
    pub year: Option<String>,
    /// File type (e.g., "capability", "data")
    pub file_type: Option<String>,
}

impl DatasetInfo {
    /// Parse dataset information from a file path
    ///
    /// # Arguments
    ///
    /// * `path` - The relative path from the manifest
    ///
    /// # Errors
    ///
    /// Returns `ManifestError::InvalidPath` if the path cannot be parsed
    ///
    /// # Example Path Structure
    ///
    /// ```text
    /// ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1980.csv
    /// ```
    pub fn from_path(path: &str) -> ManifestResult<Self> {
        // Remove leading "./" if present
        let normalized_path = path.strip_prefix("./").unwrap_or(path);

        // Split path into components
        let components: Vec<&str> = normalized_path.split('/').collect();

        // Validate minimum path structure
        if components.len() < 3 {
            return Err(ManifestError::InvalidPath {
                path: path.to_string(),
            });
        }

        // Expected structures:
        // Normal data file: data/{dataset}/{version}/{county}/{station}/qc-version-{X}/{filename}
        // Capability file:  data/{dataset}/{version}/{county}/{station}/{filename}
        let mut dataset_name = String::new();
        let mut version = String::new();
        let mut county = None;
        let mut station_id = None;
        let mut station_name = None;
        let mut quality_version = None;
        let mut year = None;
        let mut file_type = None;

        // Parse components based on expected structure
        for (i, component) in components.iter().enumerate() {
            match i {
                0 => {
                    // Should be "data"
                    if *component != "data" {
                        return Err(ManifestError::InvalidPath {
                            path: path.to_string(),
                        });
                    }
                }
                1 => {
                    // Dataset name
                    dataset_name = component.to_string();
                }
                2 => {
                    // Version (expect "dataset-version-XXXXXX" format)
                    if let Some(version_str) = component.strip_prefix("dataset-version-") {
                        version = version_str.to_string();
                    } else {
                        version = component.to_string();
                    }
                }
                3 => {
                    // County/region
                    county = Some(component.to_string());
                }
                4 => {
                    // Station ID and name (expect "12345_name" format)
                    if let Some((id, name)) = component.split_once('_') {
                        station_id = Some(id.to_string());
                        station_name = Some(name.to_string());
                    } else {
                        station_id = Some(component.to_string());
                    }
                }
                5 => {
                    // This could be either a quality version directory or a filename
                    if component.starts_with("qc-version-") {
                        // This is a quality version directory
                        quality_version = QualityControlVersion::from_directory_name(component);
                    } else if component.ends_with(".csv") {
                        // This is a filename (capability file without quality version)
                        let filename = *component;

                        // Try to extract year from filename
                        // Pattern: ..._{YYYY}.csv
                        if let Some(stem) = filename.strip_suffix(".csv") {
                            let parts: Vec<&str> = stem.split('_').collect();
                            for part in parts.iter().rev() {
                                if part.len() == 4 && part.chars().all(|c| c.is_ascii_digit()) {
                                    year = Some(part.to_string());
                                    break;
                                }
                            }

                            // Determine file type
                            if filename.contains("capability") {
                                file_type = Some("capability".to_string());
                            } else if filename.contains("metadata") {
                                file_type = Some("metadata".to_string());
                            } else {
                                file_type = Some("data".to_string());
                            }
                        }
                    } else {
                        // Unknown component
                        return Err(ManifestError::InvalidPath {
                            path: path.to_string(),
                        });
                    }
                }
                _ => {
                    // Filename - extract additional metadata
                    if i == components.len() - 1 {
                        // This is the filename
                        let filename = *component;

                        // Try to extract year from filename
                        // Pattern: ..._{YYYY}.csv
                        if let Some(stem) = filename.strip_suffix(".csv") {
                            let parts: Vec<&str> = stem.split('_').collect();
                            for part in parts.iter().rev() {
                                if part.len() == 4 && part.chars().all(|c| c.is_ascii_digit()) {
                                    year = Some(part.to_string());
                                    break;
                                }
                            }

                            // Determine file type
                            if filename.contains("capability") {
                                file_type = Some("capability".to_string());
                            } else if filename.contains("metadata") {
                                file_type = Some("metadata".to_string());
                            } else {
                                file_type = Some("data".to_string());
                            }
                        }
                    }
                }
            }
        }

        // Validate required fields
        if dataset_name.is_empty() {
            return Err(ManifestError::InvalidPath {
                path: path.to_string(),
            });
        }

        Ok(DatasetInfo {
            dataset_name,
            version,
            county,
            station_id,
            station_name,
            quality_version,
            year,
            file_type,
        })
    }

    /// Get a display-friendly description of this dataset
    pub fn display_name(&self) -> String {
        let mut parts = vec![self.dataset_name.clone()];

        if !self.version.is_empty() {
            parts.push(format!("v{}", self.version));
        }

        if let Some(ref county) = self.county {
            parts.push(county.clone());
        }

        if let Some(ref station) = self.station_name {
            parts.push(station.clone());
        }

        if let Some(ref year) = self.year {
            parts.push(year.clone());
        }

        parts.join("-")
    }
}

/// Complete information about a file from the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// MD5 hash for verification and deduplication
    pub hash: Md5Hash,
    /// Original relative path from manifest
    pub relative_path: String,
    /// Extracted filename for display/logging
    pub file_name: String,
    /// Parsed dataset metadata
    pub dataset_info: DatasetInfo,
    /// Number of retry attempts for this file
    pub retry_count: u32,
    /// Timestamp of last download attempt (skipped during serialization)
    #[serde(skip)]
    pub last_attempt: Option<Instant>,
    /// Estimated file size in bytes (if known)
    pub estimated_size: Option<u64>,
    /// Destination path where file should be saved
    pub destination_path: PathBuf,
}

impl FileInfo {
    /// Create a new FileInfo with the given destination root
    ///
    /// # Arguments
    ///
    /// * `hash` - MD5 hash (efficient byte array representation)
    /// * `relative_path` - Path from manifest
    /// * `destination_root` - Base directory for file storage
    ///
    /// # Errors
    ///
    /// Returns `ManifestError` if path parsing fails
    pub fn new(
        hash: Md5Hash,
        relative_path: String,
        destination_root: &Path,
    ) -> ManifestResult<Self> {
        // Parse dataset info from path
        let dataset_info = DatasetInfo::from_path(&relative_path)?;

        // Extract filename
        let file_name = Path::new(&relative_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Build destination path (preserve directory structure)
        let normalized_path = relative_path.strip_prefix("./").unwrap_or(&relative_path);
        let destination_path = destination_root.join(normalized_path);

        Ok(FileInfo {
            hash,
            relative_path,
            file_name,
            dataset_info,
            retry_count: 0,
            last_attempt: None,
            estimated_size: None,
            destination_path,
        })
    }

    /// Check if this file needs to be retried
    ///
    /// # Arguments
    ///
    /// * `max_retries` - Maximum number of retry attempts
    /// * `retry_delay` - Minimum time between retries
    pub fn should_retry(&self, max_retries: u32, retry_delay: std::time::Duration) -> bool {
        if self.retry_count >= max_retries {
            return false;
        }

        if let Some(last_attempt) = self.last_attempt {
            last_attempt.elapsed() >= retry_delay
        } else {
            true
        }
    }

    /// Mark a retry attempt
    pub fn mark_retry_attempt(&mut self) {
        self.retry_count += 1;
        self.last_attempt = Some(Instant::now());
    }

    /// Reset retry state (e.g., after successful download)
    pub fn reset_retry_state(&mut self) {
        self.retry_count = 0;
        self.last_attempt = None;
    }

    /// Get the URL for downloading this file from CEDA
    ///
    /// # Arguments
    ///
    /// * `base_url` - CEDA base URL (e.g., "https://data.ceda.ac.uk")
    pub fn download_url(&self, base_url: &str) -> String {
        let base_url = base_url.trim_end_matches('/');
        let relative_path = self
            .relative_path
            .strip_prefix("./")
            .unwrap_or(&self.relative_path);
        // MIDAS Open data is stored under /badc/ukmo-midas-open/ on CEDA
        format!("{}/badc/ukmo-midas-open/{}", base_url, relative_path)
    }

    /// Check if the file exists at the destination path
    pub fn exists_at_destination(&self) -> bool {
        self.destination_path.exists()
    }

    /// Get the directory that should contain this file
    pub fn destination_directory(&self) -> Option<&Path> {
        self.destination_path.parent()
    }
}

impl Hash for FileInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq for FileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for FileInfo {}

/// Parse a manifest line into (hash, path) tuple
///
/// Expected format: "hash  ./path/to/file"
/// Hash should be 32-character hexadecimal MD5
/// Path should start with "./"
pub fn parse_manifest_line(line: &str) -> ManifestResult<(Md5Hash, String)> {
    let line = line.trim();

    // Skip empty lines
    if line.is_empty() {
        return Err(ManifestError::InvalidFormat {
            line: 0,
            content: "Empty line".to_string(),
        });
    }

    // Split on whitespace - expect hash and path
    let mut parts = line.split_whitespace();
    let hash = parts.next().ok_or_else(|| ManifestError::InvalidFormat {
        line: 0,
        content: "Missing hash".to_string(),
    })?;

    let path = parts.next().ok_or_else(|| ManifestError::InvalidFormat {
        line: 0,
        content: "Missing path".to_string(),
    })?;

    // Validate no extra parts
    if parts.next().is_some() {
        return Err(ManifestError::InvalidFormat {
            line: 0,
            content: "Too many fields".to_string(),
        });
    }

    // Parse and validate hash format
    let hash = Md5Hash::from_hex(hash)?;

    // Validate path format
    if !path.starts_with("./") {
        return Err(ManifestError::InvalidPath {
            path: path.to_string(),
        });
    }

    Ok((hash, path.to_string()))
}

/// Generate a unique identifier for a FileInfo based on its content hash
pub fn generate_file_id(file_info: &FileInfo) -> u64 {
    let mut hasher = DefaultHasher::new();
    file_info.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_valid_md5_hash() {
        // Test valid hashes through Md5Hash::from_hex
        assert!(Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").is_ok());
        assert!(Md5Hash::from_hex("9734faa872681f96b144f60d29d52011").is_ok());
        assert!(Md5Hash::from_hex("abcdef1234567890abcdef1234567890").is_ok());

        // Test invalid hashes
        assert!(Md5Hash::from_hex("").is_err());
        assert!(Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4").is_err()); // too short
        assert!(Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e5").is_err()); // too long
        assert!(Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4g").is_err()); // invalid char
        assert!(Md5Hash::from_hex("50C9D1C465F3CBFF652BE1509C2E2A4E").is_ok()); // uppercase (should be valid)
    }

    #[test]
    fn test_manifest_line_parsing() {
        // Valid cases
        let (hash, path) =
            parse_manifest_line("50c9d1c465f3cbff652be1509c2e2a4e  ./data/test.csv").unwrap();
        assert_eq!(hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(path, "./data/test.csv");

        // Test with multiple spaces
        let (hash, path) =
            parse_manifest_line("50c9d1c465f3cbff652be1509c2e2a4e    ./data/test.csv").unwrap();
        assert_eq!(hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(path, "./data/test.csv");

        // Invalid cases
        assert!(parse_manifest_line("").is_err()); // empty
        assert!(parse_manifest_line("50c9d1c465f3cbff652be1509c2e2a4e").is_err()); // missing path
        assert!(parse_manifest_line("invalid_hash ./data/test.csv").is_err()); // invalid hash
        assert!(parse_manifest_line("50c9d1c465f3cbff652be1509c2e2a4e data/test.csv").is_err()); // path doesn't start with ./
        assert!(
            parse_manifest_line("50c9d1c465f3cbff652be1509c2e2a4e ./data/test.csv extra").is_err()
        ); // too many fields
    }

    #[test]
    fn test_dataset_info_parsing() {
        // Test example from real manifest
        let path = "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1980.csv";
        let dataset_info = DatasetInfo::from_path(path).unwrap();

        assert_eq!(dataset_info.dataset_name, "uk-daily-temperature-obs");
        assert_eq!(dataset_info.version, "202407");
        assert_eq!(dataset_info.county, Some("devon".to_string()));
        assert_eq!(dataset_info.station_id, Some("01381".to_string()));
        assert_eq!(dataset_info.station_name, Some("twist".to_string()));
        assert_eq!(
            dataset_info.quality_version,
            Some(QualityControlVersion::V1)
        );
        assert_eq!(dataset_info.year, Some("1980".to_string()));
        assert_eq!(dataset_info.file_type, Some("data".to_string()));
    }

    #[test]
    fn test_dataset_info_capability_file() {
        let path = "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv";
        let dataset_info = DatasetInfo::from_path(path).unwrap();

        assert_eq!(dataset_info.dataset_name, "uk-daily-temperature-obs");
        assert_eq!(dataset_info.version, "202407");
        assert_eq!(dataset_info.county, Some("devon".to_string()));
        assert_eq!(dataset_info.station_id, Some("01381".to_string()));
        assert_eq!(dataset_info.station_name, Some("twist".to_string()));
        assert_eq!(dataset_info.quality_version, None);
        assert_eq!(dataset_info.year, None);
        assert_eq!(dataset_info.file_type, Some("capability".to_string()));
    }

    #[test]
    fn test_invalid_dataset_paths() {
        // Too short
        assert!(DatasetInfo::from_path("./data").is_err());
        assert!(DatasetInfo::from_path("./invalid").is_err());

        // Doesn't start with data
        assert!(DatasetInfo::from_path("./other/dataset/file.csv").is_err());
    }

    #[test]
    fn test_file_info_creation() {
        let temp_dir = tempdir().unwrap();
        let destination_root = temp_dir.path();

        let hash = "50c9d1c465f3cbff652be1509c2e2a4e".to_string();
        let path = "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/test_file.csv".to_string();

        let hash_obj = Md5Hash::from_hex(&hash).unwrap();
        let file_info = FileInfo::new(hash_obj, path.clone(), destination_root).unwrap();

        assert_eq!(file_info.hash.to_hex(), hash);
        assert_eq!(file_info.relative_path, path);
        assert_eq!(file_info.file_name, "test_file.csv");
        assert_eq!(file_info.retry_count, 0);
        assert!(file_info.last_attempt.is_none());

        // Check destination path
        let expected_dest = destination_root.join("data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/test_file.csv");
        assert_eq!(file_info.destination_path, expected_dest);
    }

    #[test]
    fn test_file_info_invalid_hash() {
        let temp_dir = tempdir().unwrap();
        let _destination_root = temp_dir.path();

        // This should fail during Md5Hash::from_hex, so test the hash validation directly
        let result = Md5Hash::from_hex("invalid_hash");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ManifestError::InvalidHash { .. }
        ));
    }

    #[test]
    fn test_file_info_retry_logic() {
        let temp_dir = tempdir().unwrap();
        let destination_root = temp_dir.path();

        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let mut file_info = FileInfo::new(
            hash,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv"
                .to_string(),
            destination_root,
        )
        .unwrap();

        // Should be able to retry initially
        assert!(file_info.should_retry(3, std::time::Duration::from_millis(100)));

        // Mark retry attempt
        file_info.mark_retry_attempt();
        assert_eq!(file_info.retry_count, 1);
        assert!(file_info.last_attempt.is_some());

        // Should not retry immediately (within delay)
        assert!(!file_info.should_retry(3, std::time::Duration::from_secs(60)));

        // Should retry after delay
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(file_info.should_retry(3, std::time::Duration::from_millis(1)));

        // Exceed max retries
        file_info.retry_count = 3;
        assert!(!file_info.should_retry(3, std::time::Duration::from_millis(1)));

        // Reset state
        file_info.reset_retry_state();
        assert_eq!(file_info.retry_count, 0);
        assert!(file_info.last_attempt.is_none());
    }

    #[test]
    fn test_file_info_download_url() {
        let temp_dir = tempdir().unwrap();
        let destination_root = temp_dir.path();

        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let file_info = FileInfo::new(
            hash,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv"
                .to_string(),
            destination_root,
        )
        .unwrap();

        let url = file_info.download_url("https://data.ceda.ac.uk");
        assert_eq!(
            url,
            "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv"
        );

        // Test with trailing slash
        let url = file_info.download_url("https://data.ceda.ac.uk/");
        assert_eq!(
            url,
            "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv"
        );
    }

    #[test]
    fn test_file_info_equality_and_hashing() {
        let temp_dir = tempdir().unwrap();
        let destination_root = temp_dir.path();

        let hash1 = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let file1 = FileInfo::new(
            hash1,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test1.csv"
                .to_string(),
            destination_root,
        )
        .unwrap();

        let hash2 = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let file2 = FileInfo::new(
            hash2,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test2.csv"
                .to_string(),
            destination_root,
        )
        .unwrap();

        let hash3 = Md5Hash::from_hex("9734faa872681f96b144f60d29d52011").unwrap();
        let file3 = FileInfo::new(
            hash3,
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test3.csv"
                .to_string(),
            destination_root,
        )
        .unwrap();

        // Files with same hash should be equal
        assert_eq!(file1, file2);
        assert_ne!(file1, file3);

        // Hash should be based on content hash
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(file1);
        set.insert(file2); // Should not be inserted (duplicate)
        set.insert(file3);

        assert_eq!(set.len(), 2); // Only unique hashes
    }

    #[test]
    fn test_dataset_info_display_name() {
        let dataset_info = DatasetInfo {
            dataset_name: "uk-daily-temperature-obs".to_string(),
            version: "202407".to_string(),
            county: Some("devon".to_string()),
            station_id: Some("01381".to_string()),
            station_name: Some("twist".to_string()),
            quality_version: Some(QualityControlVersion::V1),
            year: Some("1980".to_string()),
            file_type: Some("data".to_string()),
        };

        let display_name = dataset_info.display_name();
        assert_eq!(
            display_name,
            "uk-daily-temperature-obs-v202407-devon-twist-1980"
        );
    }

    #[test]
    fn test_quality_control_version_enum() {
        // Test parsing from directory names
        assert_eq!(
            QualityControlVersion::from_directory_name("qc-version-0"),
            Some(QualityControlVersion::V0)
        );
        assert_eq!(
            QualityControlVersion::from_directory_name("qc-version-1"),
            Some(QualityControlVersion::V1)
        );
        assert_eq!(
            QualityControlVersion::from_directory_name("qc-version-2"),
            None
        );

        // Test parsing from filename format
        assert_eq!(
            QualityControlVersion::from_filename_format("qcv-0"),
            Some(QualityControlVersion::V0)
        );
        assert_eq!(
            QualityControlVersion::from_filename_format("qcv-1"),
            Some(QualityControlVersion::V1)
        );
        assert_eq!(QualityControlVersion::from_filename_format("qcv-2"), None);

        // Test conversion to directory names
        assert_eq!(
            QualityControlVersion::V0.to_directory_name(),
            "qc-version-0"
        );
        assert_eq!(
            QualityControlVersion::V1.to_directory_name(),
            "qc-version-1"
        );

        // Test conversion to filename format
        assert_eq!(QualityControlVersion::V0.to_filename_format(), "qcv-0");
        assert_eq!(QualityControlVersion::V1.to_filename_format(), "qcv-1");

        // Test Display trait
        assert_eq!(format!("{}", QualityControlVersion::V0), "qcv-0");
        assert_eq!(format!("{}", QualityControlVersion::V1), "qcv-1");
    }

    #[test]
    fn test_dataset_info_with_qcv0_parsing() {
        // Test with QC version 0 (from real manifest)
        let path = "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01373_clyst-honiton/qc-version-0/midas-open_uk-daily-temperature-obs_dv-202407_devon_01373_clyst-honiton_qcv-0_1997.csv";
        let dataset_info = DatasetInfo::from_path(path).unwrap();

        assert_eq!(dataset_info.dataset_name, "uk-daily-temperature-obs");
        assert_eq!(dataset_info.version, "202407");
        assert_eq!(dataset_info.county, Some("devon".to_string()));
        assert_eq!(dataset_info.station_id, Some("01373".to_string()));
        assert_eq!(dataset_info.station_name, Some("clyst-honiton".to_string()));
        assert_eq!(
            dataset_info.quality_version,
            Some(QualityControlVersion::V0)
        );
        assert_eq!(dataset_info.year, Some("1997".to_string()));
        assert_eq!(dataset_info.file_type, Some("data".to_string()));
    }
}
