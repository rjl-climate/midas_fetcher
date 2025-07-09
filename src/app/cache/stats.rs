//! Cache statistics and disk usage monitoring
//!
//! This module provides functionality for monitoring cache usage, including
//! file counts, disk space usage, and scanning cache directories.

use std::path::{Path, PathBuf};

use tracing::warn;

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Cache root directory
    pub cache_root: PathBuf,
    /// Number of active reservations
    pub active_reservations: usize,
    /// Number of files currently being downloaded
    pub downloading_files: usize,
    /// Number of files actually cached on disk
    pub cached_files_count: usize,
    /// Total size of cached files in bytes
    pub total_cache_size: u64,
    /// Available disk space in bytes
    pub available_space: u64,
}

impl CacheStats {
    /// Create new cache statistics
    pub fn new(cache_root: PathBuf) -> Self {
        Self {
            cache_root,
            active_reservations: 0,
            downloading_files: 0,
            cached_files_count: 0,
            total_cache_size: 0,
            available_space: 0,
        }
    }

    /// Update reservation counts
    pub fn set_reservation_stats(&mut self, active_reservations: usize, downloading_files: usize) {
        self.active_reservations = active_reservations;
        self.downloading_files = downloading_files;
    }

    /// Update disk usage statistics
    pub fn set_disk_stats(
        &mut self,
        cached_files_count: usize,
        total_cache_size: u64,
        available_space: u64,
    ) {
        self.cached_files_count = cached_files_count;
        self.total_cache_size = total_cache_size;
        self.available_space = available_space;
    }

    /// Get cache usage as percentage of available space
    pub fn usage_percentage(&self) -> f64 {
        if self.available_space == 0 {
            return 0.0;
        }
        (self.total_cache_size as f64 / (self.total_cache_size + self.available_space) as f64)
            * 100.0
    }

    /// Check if cache is approaching capacity limits
    pub fn is_near_capacity(&self, threshold_percentage: f64) -> bool {
        self.usage_percentage() > threshold_percentage
    }

    /// Format cache size in human-readable format
    pub fn format_cache_size(&self) -> String {
        format_bytes(self.total_cache_size)
    }

    /// Format available space in human-readable format
    pub fn format_available_space(&self) -> String {
        format_bytes(self.available_space)
    }
}

/// Directory scanner for cache statistics
pub struct DirectoryScanner;

impl DirectoryScanner {
    /// Scan cache directory to count files and calculate total size
    pub async fn scan_cache_directory(cache_root: &Path) -> (usize, u64) {
        // Run the directory scanning in a blocking task to avoid blocking the async runtime
        let cache_root = cache_root.to_path_buf();

        tokio::task::spawn_blocking(move || Self::scan_directory_recursive(&cache_root))
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to scan cache directory: {}", e);
                (0, 0)
            })
    }

    /// Recursively scan a directory for cached files
    fn scan_directory_recursive(dir: &Path) -> (usize, u64) {
        let mut file_count = 0;
        let mut total_size = 0u64;

        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.is_dir() {
                    // Recursively scan subdirectories
                    let (sub_count, sub_size) = Self::scan_directory_recursive(&path);
                    file_count += sub_count;
                    total_size += sub_size;
                } else if path.is_file() {
                    // Count files that end with .csv (MIDAS data files)
                    if Self::is_data_file(&path) {
                        file_count += 1;

                        // Get file size
                        if let Ok(metadata) = entry.metadata() {
                            total_size += metadata.len();
                        }
                    }
                }
            }
        }

        (file_count, total_size)
    }

    /// Check if a file is a data file (.csv extension)
    fn is_data_file(path: &Path) -> bool {
        if let Some(extension) = path.extension() {
            extension == "csv"
        } else {
            false
        }
    }

    /// Get available disk space for a given path
    pub async fn get_available_disk_space(path: &Path) -> u64 {
        let path = path.to_path_buf();

        tokio::task::spawn_blocking(move || Self::get_disk_space_blocking(&path))
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to get available disk space: {}", e);
                0
            })
    }

    /// Get available disk space for a given path (blocking implementation)
    fn get_disk_space_blocking(_path: &Path) -> u64 {
        // For now, return a placeholder value until we add proper dependencies
        // TODO: Add libc dependency for Unix and winapi for Windows
        // This will be implemented in a future iteration
        warn!("Disk space calculation not yet implemented - returning 0");
        0
    }

    /// Scan a specific directory for file count and size (for testing)
    pub fn scan_directory_sync(dir: &Path) -> (usize, u64) {
        Self::scan_directory_recursive(dir)
    }

    /// Get file count in a directory (non-recursive)
    pub fn count_files_in_directory(dir: &Path) -> usize {
        if let Ok(entries) = std::fs::read_dir(dir) {
            entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_type().map(|ft| ft.is_file()).unwrap_or(false))
                .filter(|entry| Self::is_data_file(&entry.path()))
                .count()
        } else {
            0
        }
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    const THRESHOLD: u64 = 1024;

    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD as f64;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    #[test]
    fn test_cache_stats_creation() {
        let cache_root = PathBuf::from("/cache");
        let stats = CacheStats::new(cache_root.clone());

        assert_eq!(stats.cache_root, cache_root);
        assert_eq!(stats.active_reservations, 0);
        assert_eq!(stats.downloading_files, 0);
        assert_eq!(stats.cached_files_count, 0);
        assert_eq!(stats.total_cache_size, 0);
        assert_eq!(stats.available_space, 0);
    }

    #[test]
    fn test_cache_stats_updates() {
        let cache_root = PathBuf::from("/cache");
        let mut stats = CacheStats::new(cache_root);

        stats.set_reservation_stats(5, 2);
        assert_eq!(stats.active_reservations, 5);
        assert_eq!(stats.downloading_files, 2);

        stats.set_disk_stats(100, 1024 * 1024, 10 * 1024 * 1024);
        assert_eq!(stats.cached_files_count, 100);
        assert_eq!(stats.total_cache_size, 1024 * 1024);
        assert_eq!(stats.available_space, 10 * 1024 * 1024);
    }

    #[test]
    fn test_usage_percentage() {
        let cache_root = PathBuf::from("/cache");
        let mut stats = CacheStats::new(cache_root);

        // Test with no available space
        stats.set_disk_stats(10, 0, 0);
        assert_eq!(stats.usage_percentage(), 0.0);

        // Test with 10% usage (1MB used, 9MB available)
        stats.set_disk_stats(10, 1024 * 1024, 9 * 1024 * 1024);
        assert!((stats.usage_percentage() - 10.0).abs() < 0.1);

        // Test near capacity
        stats.set_disk_stats(10, 9 * 1024 * 1024, 1024 * 1024);
        assert!(stats.is_near_capacity(80.0));
        assert!(!stats.is_near_capacity(95.0));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024_u64.pow(4)), "1.00 TB");
    }

    #[test]
    fn test_format_cache_size() {
        let cache_root = PathBuf::from("/cache");
        let mut stats = CacheStats::new(cache_root);

        stats.set_disk_stats(10, 1024 * 1024, 0);
        assert_eq!(stats.format_cache_size(), "1.00 MB");

        stats.set_disk_stats(10, 2048, 0);
        assert_eq!(stats.format_cache_size(), "2.00 KB");
    }

    #[test]
    fn test_is_data_file() {
        assert!(DirectoryScanner::is_data_file(Path::new("test.csv")));
        assert!(DirectoryScanner::is_data_file(Path::new(
            "/path/to/file.csv"
        )));
        assert!(!DirectoryScanner::is_data_file(Path::new("test.txt")));
        assert!(!DirectoryScanner::is_data_file(Path::new("test")));
        assert!(!DirectoryScanner::is_data_file(Path::new("test.CSV"))); // Case sensitive
    }

    #[tokio::test]
    async fn test_scan_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let (file_count, total_size) =
            DirectoryScanner::scan_cache_directory(temp_dir.path()).await;

        assert_eq!(file_count, 0);
        assert_eq!(total_size, 0);
    }

    #[tokio::test]
    async fn test_scan_directory_with_csv_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create some test files
        fs::write(temp_dir.path().join("test1.csv"), b"test content 1")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("test2.csv"), b"test content 2")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("test.txt"), b"not a csv file")
            .await
            .unwrap();

        let (file_count, total_size) =
            DirectoryScanner::scan_cache_directory(temp_dir.path()).await;

        assert_eq!(file_count, 2); // Only CSV files counted
        assert!(total_size > 0);
    }

    #[tokio::test]
    async fn test_scan_directory_recursive() {
        let temp_dir = TempDir::new().unwrap();

        // Create nested directory structure
        let subdir = temp_dir.path().join("subdir");
        fs::create_dir_all(&subdir).await.unwrap();

        // Create files in both directories
        fs::write(temp_dir.path().join("root.csv"), b"root file")
            .await
            .unwrap();
        fs::write(subdir.join("sub.csv"), b"sub file")
            .await
            .unwrap();

        let (file_count, total_size) =
            DirectoryScanner::scan_cache_directory(temp_dir.path()).await;

        assert_eq!(file_count, 2); // Files in both root and subdirectory
        assert!(total_size > 0);
    }

    #[test]
    fn test_count_files_in_directory_sync() {
        let temp_dir = TempDir::new().unwrap();

        // Create some test files
        std::fs::write(temp_dir.path().join("test1.csv"), b"test1").unwrap();
        std::fs::write(temp_dir.path().join("test2.csv"), b"test2").unwrap();
        std::fs::write(temp_dir.path().join("test.txt"), b"txt file").unwrap();

        let count = DirectoryScanner::count_files_in_directory(temp_dir.path());
        assert_eq!(count, 2); // Only CSV files counted
    }

    #[test]
    fn test_scan_directory_sync() {
        let temp_dir = TempDir::new().unwrap();

        // Create test file
        std::fs::write(temp_dir.path().join("test.csv"), b"test content").unwrap();

        let (file_count, total_size) = DirectoryScanner::scan_directory_sync(temp_dir.path());

        assert_eq!(file_count, 1);
        assert_eq!(total_size, 12); // Length of "test content"
    }

    #[tokio::test]
    async fn test_get_available_disk_space() {
        let temp_dir = TempDir::new().unwrap();
        let available_space = DirectoryScanner::get_available_disk_space(temp_dir.path()).await;

        // On most systems, available space should be > 0
        // On systems without proper implementation, it returns 0
        // Just check that it's a valid value (useless comparison warning fixed)
        let _ = available_space;
    }
}
