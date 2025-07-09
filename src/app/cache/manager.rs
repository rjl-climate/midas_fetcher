//! Core cache manager with atomic operations
//!
//! This module contains the main CacheManager implementation with atomic file
//! operations, reservation management, and cache verification functionality.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::app::hash::Md5Hash;
use crate::app::models::FileInfo;
use crate::constants::files;
use crate::errors::{CacheError, CacheResult};

use super::config::CacheConfig;
use super::path::PathGenerator;
use super::reservation::{ReservationInfo, ReservationState, ReservationStatus};
use super::stats::{CacheStats, DirectoryScanner};
use super::verification::{HashVerifier, VerificationReport};

/// Main cache management system
#[derive(Debug)]
pub struct CacheManager {
    /// Configuration
    config: CacheConfig,
    /// Cache root directory
    cache_root: PathBuf,
    /// File reservations by hash
    reservations: Arc<RwLock<HashMap<Md5Hash, ReservationInfo>>>,
    /// Worker ID generator
    worker_id_generator: Arc<RwLock<WorkerIdGenerator>>,
}

/// Worker ID generation system
#[derive(Debug)]
struct WorkerIdGenerator {
    next_id: u32,
}

impl WorkerIdGenerator {
    fn new() -> Self {
        Self { next_id: 1 }
    }

    fn generate_id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1; // Skip 0
        }
        id
    }
}

impl CacheManager {
    /// Create a new cache manager
    ///
    /// # Arguments
    ///
    /// * `config` - Cache configuration
    ///
    /// # Errors
    ///
    /// Returns `CacheError` if cache directory cannot be created or accessed
    pub async fn new(config: CacheConfig) -> CacheResult<Self> {
        let cache_root = match &config.cache_root {
            Some(path) => path.clone(),
            None => Self::get_default_cache_dir()?,
        };

        // Ensure cache directory exists
        Self::ensure_directory_exists(&cache_root).await?;

        info!(
            "Initialized cache manager with root: {}",
            cache_root.display()
        );

        Ok(Self {
            config,
            cache_root,
            reservations: Arc::new(RwLock::new(HashMap::new())),
            worker_id_generator: Arc::new(RwLock::new(WorkerIdGenerator::new())),
        })
    }

    /// Get the cache root directory
    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    /// Get the cache configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Get the default cache directory for the current OS
    ///
    /// Uses the Application Support directory to keep config and cache unified:
    /// - macOS: ~/Library/Application Support/midas-fetcher/cache
    /// - Linux: ~/.config/midas-fetcher/cache  
    /// - Windows: %APPDATA%/midas-fetcher/cache
    fn get_default_cache_dir() -> CacheResult<PathBuf> {
        let cache_dir = dirs::config_dir()
            .ok_or_else(|| CacheError::DirectoryNotAccessible {
                path: PathBuf::from("system config directory"),
            })?
            .join("midas-fetcher")
            .join("cache");

        Ok(cache_dir)
    }

    /// Ensure a directory exists, creating it if necessary
    async fn ensure_directory_exists(path: &Path) -> CacheResult<()> {
        if !path.exists() {
            fs::create_dir_all(path).await.map_err(|e| {
                error!("Failed to create cache directory: {}", e);
                CacheError::DirectoryNotAccessible {
                    path: path.to_path_buf(),
                }
            })?;
            debug!("Created cache directory: {}", path.display());
        }
        Ok(())
    }

    /// Get the cache path for a file
    pub fn get_file_path(&self, file_info: &FileInfo) -> PathBuf {
        PathGenerator::get_file_path(&self.cache_root, file_info)
    }

    /// Check if a file exists and attempt to reserve it for download
    ///
    /// # Arguments
    ///
    /// * `file_info` - Information about the file to check
    ///
    /// # Returns
    ///
    /// `ReservationStatus` indicating the current state of the file
    pub async fn check_and_reserve(&self, file_info: &FileInfo) -> CacheResult<ReservationStatus> {
        let file_path = self.get_file_path(file_info);

        // Check if file already exists and is valid
        if file_path.exists() {
            if HashVerifier::verify_file_hash(&file_path, &file_info.hash).await? {
                debug!("File already exists and verified: {}", file_path.display());
                return Ok(ReservationStatus::AlreadyExists);
            } else {
                warn!(
                    "File exists but hash verification failed, will re-download: {}",
                    file_path.display()
                );
                // Remove corrupted file
                if let Err(e) = fs::remove_file(&file_path).await {
                    error!("Failed to remove corrupted file: {}", e);
                }
            }
        }

        // Try to reserve the file
        self.try_reserve_file(file_info).await
    }

    /// Attempt to reserve a file for download
    async fn try_reserve_file(&self, file_info: &FileInfo) -> CacheResult<ReservationStatus> {
        let mut reservations = self.reservations.write().await;

        // Check if file is already reserved
        if let Some(existing) = reservations.get(&file_info.hash) {
            // Check for timeout
            if existing.is_timed_out(self.config.reservation_timeout) {
                warn!(
                    "Reservation timed out for worker {}, releasing",
                    existing.worker_id
                );
                reservations.remove(&file_info.hash);
            } else {
                return Ok(ReservationStatus::ReservedByOther {
                    worker_id: existing.worker_id,
                });
            }
        }

        // Create new reservation
        let worker_id = self.generate_worker_id().await;
        let reservation = ReservationInfo::new(worker_id, file_info.clone());
        reservations.insert(file_info.hash, reservation);

        debug!("Reserved file {} for worker {}", file_info.hash, worker_id);
        Ok(ReservationStatus::Reserved)
    }

    /// Generate a unique worker ID
    async fn generate_worker_id(&self) -> u32 {
        let mut generator = self.worker_id_generator.write().await;
        generator.generate_id()
    }

    /// Save file content atomically using temp file + rename pattern
    ///
    /// # Arguments
    ///
    /// * `content` - File content to save
    /// * `file_info` - File information including destination path
    ///
    /// # Errors
    ///
    /// Returns `CacheError` if file operation fails
    pub async fn save_file_atomic(&self, content: &[u8], file_info: &FileInfo) -> CacheResult<()> {
        let final_path = self.get_file_path(file_info);
        let temp_path = final_path.with_extension(format!(
            "{}{}",
            final_path.extension().unwrap_or_default().to_string_lossy(),
            files::TEMP_FILE_SUFFIX
        ));

        // Ensure parent directory exists
        if let Some(parent) = final_path.parent() {
            Self::ensure_directory_exists(parent).await?;
        }

        // Write to temporary file first
        fs::write(&temp_path, content).await.map_err(|e| {
            error!("Failed to write temporary file: {}", e);
            CacheError::InvalidState {
                reason: format!("Temporary file write failed: {}", e),
            }
        })?;

        // Verify hash before rename
        let actual_hash = HashVerifier::calculate_file_hash(&temp_path).await?;
        if actual_hash != file_info.hash {
            // Remove temp file and return error
            let _ = fs::remove_file(&temp_path).await;
            return Err(CacheError::VerificationFailed { files_failed: 1 });
        }

        // Atomic rename to final location
        fs::rename(&temp_path, &final_path).await.map_err(|e| {
            error!("Failed to rename temporary file: {}", e);
            CacheError::InvalidState {
                reason: format!("Atomic rename failed: {}", e),
            }
        })?;

        // Update reservation status
        self.mark_reservation_completed(&file_info.hash).await?;

        info!("Successfully saved file: {}", final_path.display());
        Ok(())
    }

    /// Mark a reservation as completed
    async fn mark_reservation_completed(&self, hash: &Md5Hash) -> CacheResult<()> {
        let mut reservations = self.reservations.write().await;
        if let Some(reservation) = reservations.get_mut(hash) {
            reservation.mark_completed();
            debug!("Marked reservation as completed: {}", hash);
        }
        Ok(())
    }

    /// Mark a reservation as failed
    pub async fn mark_reservation_failed(&self, hash: &Md5Hash, error: String) -> CacheResult<()> {
        let mut reservations = self.reservations.write().await;
        if let Some(reservation) = reservations.get_mut(hash) {
            reservation.mark_failed(error);
            debug!("Marked reservation as failed: {}", hash);
        }
        Ok(())
    }

    /// Release a reservation (remove it from the map)
    pub async fn release_reservation(&self, hash: &Md5Hash) -> CacheResult<()> {
        let mut reservations = self.reservations.write().await;
        if reservations.remove(hash).is_some() {
            debug!("Released reservation: {}", hash);
        }
        Ok(())
    }

    /// Verify cache integrity against a manifest
    ///
    /// # Arguments
    ///
    /// * `files` - Iterator of files to verify
    ///
    /// # Returns
    ///
    /// `VerificationReport` with detailed results
    pub async fn verify_cache_integrity<I>(&self, files: I) -> CacheResult<VerificationReport>
    where
        I: IntoIterator<Item = FileInfo>,
    {
        let start_time = Instant::now();
        let mut report = VerificationReport::new();

        for file_info in files {
            let file_path = self.get_file_path(&file_info);
            let expected_hash = file_info.hash;

            if !file_path.exists() {
                report.add_missing(file_info, expected_hash);
                continue;
            }

            let (is_valid, failure) =
                HashVerifier::verify_single_file(&file_path, &file_info).await;
            if is_valid {
                report.add_verified();
            } else if let Some(failure) = failure {
                report.add_failed(
                    failure.file_info,
                    failure.reason,
                    failure.expected_hash,
                    failure.actual_hash,
                );
            }

            // Log progress periodically
            HashVerifier::log_progress(report.files_checked, report.files_failed);
        }

        report.set_verification_time(start_time.elapsed());

        info!(
            "Cache verification completed: {}/{} files verified ({:.1}% success rate) in {:.2}s",
            report.files_verified,
            report.files_checked,
            report.success_rate(),
            report.verification_time.as_secs_f64()
        );

        Ok(report)
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> CacheStats {
        let reservations = self.reservations.read().await;
        let active_reservations = reservations.len();
        let downloading = reservations
            .values()
            .filter(|r| matches!(r.status, ReservationState::Downloading))
            .count();

        // Scan cache directory for actual files and sizes
        let (cached_files_count, total_cache_size) =
            DirectoryScanner::scan_cache_directory(&self.cache_root).await;
        let available_space = DirectoryScanner::get_available_disk_space(&self.cache_root).await;

        let mut stats = CacheStats::new(self.cache_root.clone());
        stats.set_reservation_stats(active_reservations, downloading);
        stats.set_disk_stats(cached_files_count, total_cache_size, available_space);
        stats
    }

    /// Clean up stale reservations
    pub async fn cleanup_stale_reservations(&self) -> CacheResult<usize> {
        let mut reservations = self.reservations.write().await;
        let initial_count = reservations.len();

        reservations
            .retain(|_, reservation| !reservation.is_timed_out(self.config.reservation_timeout));

        let cleaned = initial_count - reservations.len();
        if cleaned > 0 {
            info!("Cleaned up {} stale reservations", cleaned);
        }

        Ok(cleaned)
    }

    /// Get reservation information for a specific file hash
    pub async fn get_reservation(&self, hash: &Md5Hash) -> Option<ReservationInfo> {
        let reservations = self.reservations.read().await;
        reservations.get(hash).cloned()
    }

    /// Get all active reservations
    pub async fn get_all_reservations(&self) -> Vec<ReservationInfo> {
        let reservations = self.reservations.read().await;
        reservations.values().cloned().collect()
    }

    /// Check if a file is currently reserved
    pub async fn is_reserved(&self, hash: &Md5Hash) -> bool {
        let reservations = self.reservations.read().await;
        reservations.contains_key(hash)
    }

    /// Update reservation status to downloading
    pub async fn mark_reservation_downloading(&self, hash: &Md5Hash) -> CacheResult<()> {
        let mut reservations = self.reservations.write().await;
        if let Some(reservation) = reservations.get_mut(hash) {
            reservation.mark_downloading();
            debug!("Marked reservation as downloading: {}", hash);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::{DatasetFileInfo, QualityControlVersion};
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn test_cache_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());

        let cache = CacheManager::new(config).await.unwrap();
        assert_eq!(cache.cache_root(), temp_dir.path());
    }

    #[tokio::test]
    async fn test_default_cache_directory() {
        let config = CacheConfig::default();
        let cache = CacheManager::new(config).await.unwrap();

        // Should create a cache directory under the OS config directory
        assert!(cache.cache_root().exists());
        assert!(cache
            .cache_root()
            .to_string_lossy()
            .contains("midas-fetcher"));
    }

    #[tokio::test]
    async fn test_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();
        let path = cache.get_file_path(&file_info);

        let expected_path = temp_dir
            .path()
            .join("uk-daily-temperature-obs-202507")
            .join("qcv-1")
            .join("devon")
            .join("01381_twist")
            .join("test.csv");

        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_reservation_system() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();

        // First reservation should succeed
        let status1 = cache.check_and_reserve(&file_info).await.unwrap();
        assert_eq!(status1, ReservationStatus::Reserved);

        // Second reservation should be blocked
        let status2 = cache.check_and_reserve(&file_info).await.unwrap();
        assert!(matches!(status2, ReservationStatus::ReservedByOther { .. }));

        // Release reservation
        cache.release_reservation(&file_info.hash).await.unwrap();

        // Should be able to reserve again
        let status3 = cache.check_and_reserve(&file_info).await.unwrap();
        assert_eq!(status3, ReservationStatus::Reserved);
    }

    #[tokio::test]
    async fn test_worker_id_generation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let id1 = cache.generate_worker_id().await;
        let id2 = cache.generate_worker_id().await;
        let id3 = cache.generate_worker_id().await;

        // IDs should be unique and sequential
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_eq!(id1 + 1, id2);
        assert_eq!(id2 + 1, id3);
    }

    #[tokio::test]
    async fn test_atomic_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();

        // Reserve the file
        let status = cache.check_and_reserve(&file_info).await.unwrap();
        assert_eq!(status, ReservationStatus::Reserved);

        // Create test content with matching hash
        let test_content = b"test content for hash verification";
        let actual_hash = md5::compute(test_content);
        let file_info_with_correct_hash = FileInfo {
            hash: Md5Hash::from_bytes(actual_hash.0),
            ..file_info
        };

        // Save file atomically
        cache
            .save_file_atomic(test_content, &file_info_with_correct_hash)
            .await
            .unwrap();

        // Verify file exists and has correct content
        let file_path = cache.get_file_path(&file_info_with_correct_hash);
        assert!(file_path.exists());

        let saved_content = fs::read(&file_path).await.unwrap();
        assert_eq!(saved_content, test_content);

        // Verify file should pass verification
        let verified =
            HashVerifier::verify_file_hash(&file_path, &file_info_with_correct_hash.hash)
                .await
                .unwrap();
        assert!(verified);
    }

    #[tokio::test]
    async fn test_reservation_state_management() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();

        // Reserve file
        cache.check_and_reserve(&file_info).await.unwrap();
        assert!(cache.is_reserved(&file_info.hash).await);

        // Mark as downloading
        cache
            .mark_reservation_downloading(&file_info.hash)
            .await
            .unwrap();

        let reservation = cache.get_reservation(&file_info.hash).await.unwrap();
        assert!(reservation.is_downloading());

        // Mark as failed
        cache
            .mark_reservation_failed(&file_info.hash, "Test error".to_string())
            .await
            .unwrap();

        let reservation = cache.get_reservation(&file_info.hash).await.unwrap();
        assert!(reservation.is_failed());
        assert_eq!(reservation.get_error(), Some("Test error"));
    }

    #[tokio::test]
    async fn test_cache_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        // Create some test files
        fs::write(temp_dir.path().join("test1.csv"), b"test content 1")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("test2.csv"), b"test content 2")
            .await
            .unwrap();

        let stats = cache.get_cache_stats().await;
        assert_eq!(stats.cache_root, temp_dir.path());
        // Cache stats should be valid values
        assert!(stats.cached_files_count < usize::MAX);
        assert!(stats.total_cache_size < u64::MAX);
    }

    #[tokio::test]
    async fn test_cleanup_stale_reservations() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf())
            .with_reservation_timeout(std::time::Duration::from_millis(10));
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();

        // Create reservation
        cache.check_and_reserve(&file_info).await.unwrap();
        assert_eq!(cache.get_all_reservations().await.len(), 1);

        // Wait for timeout
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Cleanup should remove stale reservation
        let cleaned = cache.cleanup_stale_reservations().await.unwrap();
        assert_eq!(cleaned, 1);
        assert_eq!(cache.get_all_reservations().await.len(), 0);
    }

    #[tokio::test]
    async fn test_cache_verification() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_cache_root(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(config).await.unwrap();

        let file_info = create_test_file_info();

        // Create test content
        let test_content = b"verification test content";
        let actual_hash = md5::compute(test_content);
        let file_info_with_correct_hash = FileInfo {
            hash: Md5Hash::from_bytes(actual_hash.0),
            ..file_info
        };

        // Save file
        cache
            .check_and_reserve(&file_info_with_correct_hash)
            .await
            .unwrap();
        cache
            .save_file_atomic(test_content, &file_info_with_correct_hash)
            .await
            .unwrap();

        // Verify cache
        let files = vec![file_info_with_correct_hash];
        let report = cache.verify_cache_integrity(files).await.unwrap();

        assert_eq!(report.files_checked, 1);
        assert_eq!(report.files_verified, 1);
        assert_eq!(report.files_failed, 0);
        assert!(report.is_successful());
        assert_eq!(report.success_rate(), 100.0);
    }
}
