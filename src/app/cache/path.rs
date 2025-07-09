//! File path generation and organization
//!
//! This module handles the generation of cache file paths based on dataset
//! information, organizing files by dataset, quality version, county, and station.

use std::path::{Path, PathBuf};

use crate::app::models::FileInfo;

/// Path generation utility for cache files
pub struct PathGenerator;

impl PathGenerator {
    /// Get the cache path for a file based on its dataset information
    ///
    /// Structure:
    /// - Capability/metadata files: {cache_root}/{dataset}/capability/{county}/{station}/{filename}
    /// - Data files: {cache_root}/{dataset}/{quality_version}/{county}/{station}/{filename}
    /// - Station metadata files: {cache_root}/{dataset}/station-metadata/{filename}
    /// - Change log files: {cache_root}/{dataset}/{filename}
    /// - Station log files: {cache_root}/{dataset}/station-log-files/{filename}
    pub fn get_file_path(cache_root: &Path, file_info: &FileInfo) -> PathBuf {
        let dataset = &file_info.dataset_info;
        let mut path = cache_root.to_path_buf();

        // Add dataset name (with manifest version if available)
        if let Some(version) = file_info.manifest_version {
            let versioned_name = format!("{}-{}", dataset.dataset_name, version);
            path.push(versioned_name);
        } else {
            path.push(&dataset.dataset_name);
        }

        // Determine path structure based on file type
        if let Some(ref file_type) = dataset.file_type {
            match file_type.as_str() {
                "capability" | "metadata" => {
                    // Capability and metadata files go in separate capability folder
                    path.push("capability");
                    Self::add_county_and_station_path(&mut path, dataset);
                }
                "station-metadata" => {
                    // Station metadata files go in station-metadata folder
                    path.push("station-metadata");
                    // Add filename directly and return early (no county/station structure)
                    path.push(&file_info.file_name);
                    return path;
                }
                "change-log" => {
                    // Change log files go directly in dataset root
                    path.push(&file_info.file_name);
                    return path;
                }
                "station-log" => {
                    // Station log files go in station-log-files folder
                    path.push("station-log-files");
                    // Add filename directly and return early (no county/station structure)
                    path.push(&file_info.file_name);
                    return path;
                }
                _ => {
                    // Data files use quality version structure
                    Self::add_quality_version_path(&mut path, dataset);
                    Self::add_county_and_station_path(&mut path, dataset);
                }
            }
        } else {
            // Fallback for files without file_type - use quality version
            Self::add_quality_version_path(&mut path, dataset);
            Self::add_county_and_station_path(&mut path, dataset);
        }

        // Add filename
        path.push(&file_info.file_name);

        path
    }

    /// Add quality version to path for data files
    fn add_quality_version_path(path: &mut PathBuf, dataset: &crate::app::models::DatasetFileInfo) {
        if let Some(qv) = &dataset.quality_version {
            path.push(qv.to_filename_format());
        } else {
            path.push("no-quality");
        }
    }

    /// Add county and station directories to path
    fn add_county_and_station_path(
        path: &mut PathBuf,
        dataset: &crate::app::models::DatasetFileInfo,
    ) {
        // Add county if available
        if let Some(county) = &dataset.county {
            path.push(county);
        } else {
            path.push("no-county");
        }

        // Add station if available
        if let Some(station_id) = &dataset.station_id {
            if let Some(station_name) = &dataset.station_name {
                path.push(format!("{}_{}", station_id, station_name));
            } else {
                path.push(station_id);
            }
        } else {
            path.push("no-station");
        }
    }

    /// Generate a path for a specific file type (used for testing)
    pub fn get_path_for_file_type(
        cache_root: &Path,
        dataset_name: &str,
        manifest_version: Option<u32>,
        file_type: &str,
        file_name: &str,
    ) -> PathBuf {
        let mut path = cache_root.to_path_buf();

        // Add dataset name (with manifest version if available)
        if let Some(version) = manifest_version {
            let versioned_name = format!("{}-{}", dataset_name, version);
            path.push(versioned_name);
        } else {
            path.push(dataset_name);
        }

        // Add file type specific path
        match file_type {
            "station-metadata" => {
                path.push("station-metadata");
            }
            "change-log" => {
                // Change log files go directly in dataset root
                path.push(file_name);
                return path;
            }
            "station-log" => {
                path.push("station-log-files");
            }
            _ => {
                // For other types, we would need more parameters
                // This is a simplified version for testing
                path.push(file_type);
            }
        }

        path.push(file_name);
        path
    }

    /// Check if a path represents a data file (has quality version structure)
    pub fn is_data_file_path(path: &Path) -> bool {
        // Look for quality version directory in the path components
        for component in path.components() {
            if let Some(dir_name) = component.as_os_str().to_str() {
                if dir_name.starts_with("qcv-") || dir_name == "no-quality" {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a path represents a capability file
    pub fn is_capability_file_path(path: &Path) -> bool {
        // Look for capability directory in the path components
        for component in path.components() {
            if let Some(dir_name) = component.as_os_str().to_str() {
                if dir_name == "capability" {
                    return true;
                }
            }
        }
        false
    }

    /// Extract dataset name from a cache file path
    pub fn extract_dataset_name(cache_root: &Path, file_path: &Path) -> Option<String> {
        if let Ok(relative_path) = file_path.strip_prefix(cache_root) {
            if let Some(first_component) = relative_path.components().next() {
                let dataset_with_version = first_component.as_os_str().to_string_lossy();

                // Handle versioned dataset names (e.g., "uk-daily-temperature-obs-202507")
                if let Some(last_dash) = dataset_with_version.rfind('-') {
                    let potential_version = &dataset_with_version[last_dash + 1..];
                    if potential_version.chars().all(|c| c.is_ascii_digit()) {
                        return Some(dataset_with_version[..last_dash].to_string());
                    }
                }

                return Some(dataset_with_version.to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::hash::Md5Hash;
    use crate::app::models::{DatasetFileInfo, QualityControlVersion};

    fn create_test_file_info() -> FileInfo {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let dataset_info = DatasetFileInfo {
            dataset_name: "uk-daily-temperature-obs".to_string(),
            version: "202407".to_string(),
            county: Some("devon".to_string()),
            station_id: Some("01381".to_string()),
            station_name: Some("twist".to_string()),
            quality_version: Some(QualityControlVersion::V1),
            year: Some("1980".to_string()),
            file_type: Some("data".to_string()),
        };

        FileInfo {
            hash,
            relative_path: "./data/test.csv".to_string(),
            file_name: "test.csv".to_string(),
            dataset_info,
            manifest_version: Some(202507),
            retry_count: 0,
            last_attempt: None,
            estimated_size: None,
            destination_path: PathBuf::from("/tmp/test.csv"),
        }
    }

    #[test]
    fn test_data_file_path_generation() {
        let cache_root = PathBuf::from("/cache");
        let file_info = create_test_file_info();
        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/qcv-1/devon/01381_twist/test.csv",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_capability_file_path_generation() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.file_type = Some("capability".to_string());
        file_info.file_name = "capability.csv".to_string();

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/capability/devon/01381_twist/capability.csv",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_station_metadata_file_path_generation() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.file_type = Some("station-metadata".to_string());
        file_info.dataset_info.county = None;
        file_info.dataset_info.station_id = None;
        file_info.dataset_info.station_name = None;
        file_info.file_name = "station-metadata.csv".to_string();

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/station-metadata/station-metadata.csv",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_change_log_file_path_generation() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.file_type = Some("change-log".to_string());
        file_info.file_name = "change-log.txt".to_string();

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from("/cache/uk-daily-temperature-obs-202507/change-log.txt");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_station_log_file_path_generation() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.file_type = Some("station-log".to_string());
        file_info.file_name = "station-log.txt".to_string();

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/station-log-files/station-log.txt",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_file_without_manifest_version() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.manifest_version = None;

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path =
            PathBuf::from("/cache/uk-daily-temperature-obs/qcv-1/devon/01381_twist/test.csv");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_file_without_quality_version() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.quality_version = None;

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/no-quality/devon/01381_twist/test.csv",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_file_without_county() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.county = None;

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path = PathBuf::from(
            "/cache/uk-daily-temperature-obs-202507/qcv-1/no-county/01381_twist/test.csv",
        );
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_file_without_station() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.station_id = None;
        file_info.dataset_info.station_name = None;

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path =
            PathBuf::from("/cache/uk-daily-temperature-obs-202507/qcv-1/devon/no-station/test.csv");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_file_with_station_id_only() {
        let cache_root = PathBuf::from("/cache");
        let mut file_info = create_test_file_info();
        file_info.dataset_info.station_name = None;

        let path = PathGenerator::get_file_path(&cache_root, &file_info);

        let expected_path =
            PathBuf::from("/cache/uk-daily-temperature-obs-202507/qcv-1/devon/01381/test.csv");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_is_data_file_path() {
        let data_path = PathBuf::from("/cache/dataset/qcv-1/devon/station/file.csv");
        assert!(PathGenerator::is_data_file_path(&data_path));

        let capability_path = PathBuf::from("/cache/dataset/capability/devon/station/file.csv");
        assert!(!PathGenerator::is_data_file_path(&capability_path));

        let metadata_path = PathBuf::from("/cache/dataset/station-metadata/file.csv");
        assert!(!PathGenerator::is_data_file_path(&metadata_path));
    }

    #[test]
    fn test_is_capability_file_path() {
        let capability_path = PathBuf::from("/cache/dataset/capability/devon/station/file.csv");
        assert!(PathGenerator::is_capability_file_path(&capability_path));

        let data_path = PathBuf::from("/cache/dataset/qcv-1/devon/station/file.csv");
        assert!(!PathGenerator::is_capability_file_path(&data_path));
    }

    #[test]
    fn test_extract_dataset_name() {
        let cache_root = PathBuf::from("/cache");

        // Test with versioned dataset name
        let file_path =
            PathBuf::from("/cache/uk-daily-temperature-obs-202507/qcv-1/devon/station/file.csv");
        let dataset_name = PathGenerator::extract_dataset_name(&cache_root, &file_path);
        assert_eq!(dataset_name, Some("uk-daily-temperature-obs".to_string()));

        // Test with non-versioned dataset name
        let file_path = PathBuf::from("/cache/simple-dataset/qcv-1/devon/station/file.csv");
        let dataset_name = PathGenerator::extract_dataset_name(&cache_root, &file_path);
        assert_eq!(dataset_name, Some("simple-dataset".to_string()));

        // Test with path outside cache root
        let file_path = PathBuf::from("/other/path/file.csv");
        let dataset_name = PathGenerator::extract_dataset_name(&cache_root, &file_path);
        assert_eq!(dataset_name, None);
    }

    #[test]
    fn test_get_path_for_file_type() {
        let cache_root = PathBuf::from("/cache");

        // Test station metadata
        let path = PathGenerator::get_path_for_file_type(
            &cache_root,
            "test-dataset",
            Some(202507),
            "station-metadata",
            "metadata.csv",
        );
        assert_eq!(
            path,
            PathBuf::from("/cache/test-dataset-202507/station-metadata/metadata.csv")
        );

        // Test change log
        let path = PathGenerator::get_path_for_file_type(
            &cache_root,
            "test-dataset",
            Some(202507),
            "change-log",
            "change.txt",
        );
        assert_eq!(path, PathBuf::from("/cache/test-dataset-202507/change.txt"));

        // Test station log
        let path = PathGenerator::get_path_for_file_type(
            &cache_root,
            "test-dataset",
            Some(202507),
            "station-log",
            "log.txt",
        );
        assert_eq!(
            path,
            PathBuf::from("/cache/test-dataset-202507/station-log-files/log.txt")
        );
    }
}
