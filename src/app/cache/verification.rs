//! Hash verification and cache integrity checking
//!
//! This module provides functionality for verifying cached files against their
//! expected MD5 hashes and generating detailed integrity reports.

use std::path::Path;
use std::time::Duration;

use tokio::fs;
use tracing::debug;

use crate::app::hash::Md5Hash;
use crate::app::models::FileInfo;
use crate::errors::{CacheError, CacheResult};

/// Cache verification report
#[derive(Debug, Clone, Default)]
pub struct VerificationReport {
    /// Total files checked
    pub files_checked: usize,
    /// Files that passed verification
    pub files_verified: usize,
    /// Files that failed verification
    pub files_failed: usize,
    /// Files that were missing
    pub files_missing: usize,
    /// Total verification time
    pub verification_time: Duration,
    /// Failed files with details
    pub failed_files: Vec<VerificationFailure>,
}

/// Details about a verification failure
#[derive(Debug, Clone)]
pub struct VerificationFailure {
    /// File that failed verification
    pub file_info: FileInfo,
    /// Reason for failure
    pub reason: String,
    /// Expected hash
    pub expected_hash: Md5Hash,
    /// Actual hash (if file exists)
    pub actual_hash: Option<Md5Hash>,
}

impl VerificationReport {
    /// Create a new empty verification report
    pub fn new() -> Self {
        Self::default()
    }

    /// Get verification success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.files_checked == 0 {
            0.0
        } else {
            (self.files_verified as f64 / self.files_checked as f64) * 100.0
        }
    }

    /// Check if verification passed (no failures)
    pub fn is_successful(&self) -> bool {
        self.files_failed == 0
    }

    /// Add a verified file to the report
    pub fn add_verified(&mut self) {
        self.files_checked += 1;
        self.files_verified += 1;
    }

    /// Add a missing file to the report
    pub fn add_missing(&mut self, file_info: FileInfo, expected_hash: Md5Hash) {
        self.files_checked += 1;
        self.files_missing += 1;
        self.files_failed += 1; // Missing files count as failures
        self.failed_files.push(VerificationFailure {
            file_info,
            reason: "File not found".to_string(),
            expected_hash,
            actual_hash: None,
        });
    }

    /// Add a failed file to the report
    pub fn add_failed(
        &mut self,
        file_info: FileInfo,
        reason: String,
        expected_hash: Md5Hash,
        actual_hash: Option<Md5Hash>,
    ) {
        self.files_checked += 1;
        self.files_failed += 1;
        self.failed_files.push(VerificationFailure {
            file_info,
            reason,
            expected_hash,
            actual_hash,
        });
    }

    /// Set the total verification time
    pub fn set_verification_time(&mut self, duration: Duration) {
        self.verification_time = duration;
    }
}

/// Hash verification functionality
pub struct HashVerifier;

impl HashVerifier {
    /// Verify that a file matches its expected hash
    pub async fn verify_file_hash(file_path: &Path, expected_hash: &Md5Hash) -> CacheResult<bool> {
        let actual_hash = Self::calculate_file_hash(file_path).await?;
        Ok(actual_hash == *expected_hash)
    }

    /// Calculate MD5 hash of a file
    pub async fn calculate_file_hash(file_path: &Path) -> CacheResult<Md5Hash> {
        let content = fs::read(file_path)
            .await
            .map_err(|e| CacheError::InvalidState {
                reason: format!("Failed to read file for hash calculation: {}", e),
            })?;

        let hash = md5::compute(&content);
        let hash_bytes: [u8; 16] = hash.0;
        Ok(Md5Hash::from_bytes(hash_bytes))
    }

    /// Verify a single file and return detailed result
    pub async fn verify_single_file(
        file_path: &Path,
        file_info: &FileInfo,
    ) -> (bool, Option<VerificationFailure>) {
        let expected_hash = file_info.hash;

        if !file_path.exists() {
            let failure = VerificationFailure {
                file_info: file_info.clone(),
                reason: "File not found".to_string(),
                expected_hash,
                actual_hash: None,
            };
            return (false, Some(failure));
        }

        match Self::verify_file_hash(file_path, &expected_hash).await {
            Ok(true) => (true, None),
            Ok(false) => {
                let actual_hash = Self::calculate_file_hash(file_path).await.ok();
                let failure = VerificationFailure {
                    file_info: file_info.clone(),
                    reason: "Hash mismatch".to_string(),
                    expected_hash,
                    actual_hash,
                };
                (false, Some(failure))
            }
            Err(e) => {
                let failure = VerificationFailure {
                    file_info: file_info.clone(),
                    reason: format!("Verification error: {}", e),
                    expected_hash,
                    actual_hash: None,
                };
                (false, Some(failure))
            }
        }
    }

    /// Log verification progress for large batches
    pub fn log_progress(files_checked: usize, files_failed: usize) {
        if files_checked % 1000 == 0 {
            debug!(
                "Verified {} files, {} failures",
                files_checked, files_failed
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::{DatasetFileInfo, QualityControlVersion};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_file_info() -> FileInfo {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let dataset_info = DatasetFileInfo {
            dataset_name: "test-dataset".to_string(),
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
    fn test_verification_report_creation() {
        let report = VerificationReport::new();
        assert_eq!(report.files_checked, 0);
        assert_eq!(report.files_verified, 0);
        assert_eq!(report.files_failed, 0);
        assert_eq!(report.files_missing, 0);
        assert!(report.failed_files.is_empty());
        assert!(report.is_successful());
        assert_eq!(report.success_rate(), 0.0);
    }

    #[test]
    fn test_verification_report_success_rate() {
        let mut report = VerificationReport::new();

        // Add some verified files
        report.add_verified();
        report.add_verified();
        report.add_verified();

        assert_eq!(report.success_rate(), 100.0);
        assert!(report.is_successful());

        // Add a failed file
        let file_info = create_test_file_info();
        report.add_failed(
            file_info,
            "Hash mismatch".to_string(),
            Md5Hash::from_hex("abcdef1234567890abcdef1234567890").unwrap(),
            None,
        );

        assert_eq!(report.success_rate(), 75.0);
        assert!(!report.is_successful());
        assert_eq!(report.files_checked, 4);
        assert_eq!(report.files_verified, 3);
        assert_eq!(report.files_failed, 1);
    }

    #[test]
    fn test_verification_report_missing_file() {
        let mut report = VerificationReport::new();
        let file_info = create_test_file_info();
        let expected_hash = file_info.hash;

        report.add_missing(file_info, expected_hash);

        assert_eq!(report.files_checked, 1);
        assert_eq!(report.files_missing, 1);
        assert!(!report.is_successful());
        assert_eq!(report.failed_files.len(), 1);
        assert_eq!(report.failed_files[0].reason, "File not found");
    }

    #[tokio::test]
    async fn test_hash_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_content = b"test content for hash verification";
        fs::write(&file_path, test_content).await.unwrap();

        let calculated_hash = HashVerifier::calculate_file_hash(&file_path).await.unwrap();

        // Calculate expected hash
        let expected_hash_digest = md5::compute(test_content);
        let expected_hash = Md5Hash::from_bytes(expected_hash_digest.0);

        assert_eq!(calculated_hash, expected_hash);
    }

    #[tokio::test]
    async fn test_file_verification_success() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_content = b"test content for verification";
        fs::write(&file_path, test_content).await.unwrap();

        let expected_hash_digest = md5::compute(test_content);
        let expected_hash = Md5Hash::from_bytes(expected_hash_digest.0);

        let is_valid = HashVerifier::verify_file_hash(&file_path, &expected_hash)
            .await
            .unwrap();
        assert!(is_valid);
    }

    #[tokio::test]
    async fn test_file_verification_failure() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_content = b"test content for verification";
        fs::write(&file_path, test_content).await.unwrap();

        let wrong_hash = Md5Hash::from_hex("00000000000000000000000000000000").unwrap();

        let is_valid = HashVerifier::verify_file_hash(&file_path, &wrong_hash)
            .await
            .unwrap();
        assert!(!is_valid);
    }

    #[tokio::test]
    async fn test_verify_single_file_success() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_content = b"test content";
        fs::write(&file_path, test_content).await.unwrap();

        let expected_hash_digest = md5::compute(test_content);
        let expected_hash = Md5Hash::from_bytes(expected_hash_digest.0);

        let mut file_info = create_test_file_info();
        file_info.hash = expected_hash;

        let (is_valid, failure) = HashVerifier::verify_single_file(&file_path, &file_info).await;
        assert!(is_valid);
        assert!(failure.is_none());
    }

    #[tokio::test]
    async fn test_verify_single_file_missing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        let file_info = create_test_file_info();

        let (is_valid, failure) = HashVerifier::verify_single_file(&file_path, &file_info).await;
        assert!(!is_valid);
        assert!(failure.is_some());

        let failure = failure.unwrap();
        assert_eq!(failure.reason, "File not found");
        assert_eq!(failure.expected_hash, file_info.hash);
        assert!(failure.actual_hash.is_none());
    }

    #[tokio::test]
    async fn test_verify_single_file_hash_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_content = b"test content";
        fs::write(&file_path, test_content).await.unwrap();

        let wrong_hash = Md5Hash::from_hex("00000000000000000000000000000000").unwrap();

        let mut file_info = create_test_file_info();
        file_info.hash = wrong_hash;

        let (is_valid, failure) = HashVerifier::verify_single_file(&file_path, &file_info).await;
        assert!(!is_valid);
        assert!(failure.is_some());

        let failure = failure.unwrap();
        assert_eq!(failure.reason, "Hash mismatch");
        assert_eq!(failure.expected_hash, wrong_hash);
        assert!(failure.actual_hash.is_some());
    }
}
