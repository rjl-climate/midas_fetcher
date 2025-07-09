//! Integration tests for manifest processing
//!
//! These tests verify the complete manifest processing workflow,
//! including complex parsing scenarios, real file handling, and
//! comprehensive FileInfo generation.

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    use crate::app::manifest::{streaming::ManifestStreamer, types::ManifestConfig, utils};
    use crate::app::models::QualityControlVersion;

    async fn create_test_manifest(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    /// Test error handling for malformed manifest lines.
    ///
    /// Purpose: Verifies that the streamer gracefully handles empty lines, invalid hashes,
    /// incorrect path formats, while continuing to process valid entries.
    /// Benefit: Ensures robust error handling doesn't stop manifest processing.
    #[tokio::test]
    async fn test_malformed_lines_handling() {
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

    /// Test parsing of real MIDAS manifest file format.
    ///
    /// Purpose: Verifies that the streamer correctly handles actual manifest entries
    /// from CEDA, including both capability and data files with complex paths.
    /// Benefit: Ensures compatibility with real-world MIDAS manifest formats.
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

        let files = utils::collect_all_files(manifest_file.path(), config)
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
            Some(QualityControlVersion::V1)
        );
        assert_eq!(data_file.dataset_info.file_type, Some("data".to_string()));
    }

    /// Test comprehensive FileInfo generation from manifest entries.
    ///
    /// Purpose: Verifies that complete FileInfo objects are created with all parsed
    /// dataset metadata, proper filenames, and destination paths.
    /// Benefit: Ensures complete data extraction and proper file organization.
    #[tokio::test]
    async fn test_comprehensive_fileinfo_generation() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_capability.csv"#;

        let manifest_file = create_test_manifest(content).await;
        let temp_dir = TempDir::new().unwrap();

        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let files = utils::collect_all_files(manifest_file.path(), config)
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
    }

    /// Test that manifest version is correctly extracted and used for folder renaming.
    ///
    /// Purpose: Verifies that dataset folders are renamed with the manifest version suffix
    /// when a versioned manifest filename is used.
    /// Benefit: Ensures proper dataset organization based on manifest versions.
    #[tokio::test]
    async fn test_manifest_version_folder_renaming() {
        let content = r#"50c9d1c465f3cbff652be1509c2e2a4e  ./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/test.csv
9734faa872681f96b144f60d29d52011  ./data/uk-daily-rain-obs/dataset-version-202407/devon/01382_another/test2.csv"#;

        // Create a manifest file with version in filename
        let mut manifest_file = NamedTempFile::new().unwrap();
        manifest_file.write_all(content.as_bytes()).unwrap();
        manifest_file.flush().unwrap();

        // Rename the file to have the version format
        let versioned_manifest_path = manifest_file
            .path()
            .with_file_name("midas-open-v202407-md5s.txt");
        std::fs::copy(manifest_file.path(), &versioned_manifest_path).unwrap();

        let temp_dir = TempDir::new().unwrap();
        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);

        let files = {
            let mut stream = streamer.stream(&versioned_manifest_path).await.unwrap();
            let mut files = Vec::new();
            while let Some(result) = stream.next().await {
                files.push(result.unwrap());
            }
            files
        };

        // Clean up the temporary versioned manifest file
        std::fs::remove_file(&versioned_manifest_path).ok();

        // Verify that the dataset folders have been renamed with the manifest version
        assert_eq!(files.len(), 2);

        // Check that the destination paths include the manifest version

        // File 1: uk-daily-temperature-obs should become uk-daily-temperature-obs-202407
        assert!(files[0]
            .destination_path
            .to_str()
            .unwrap()
            .contains("uk-daily-temperature-obs-202407"));
        assert!(!files[0]
            .destination_path
            .to_str()
            .unwrap()
            .contains("uk-daily-temperature-obs/dataset-version-202407"));

        // File 2: uk-daily-rain-obs should become uk-daily-rain-obs-202407
        assert!(files[1]
            .destination_path
            .to_str()
            .unwrap()
            .contains("uk-daily-rain-obs-202407"));
        assert!(!files[1]
            .destination_path
            .to_str()
            .unwrap()
            .contains("uk-daily-rain-obs/dataset-version-202407"));
    }
}
