//! Manifest version management functionality
//!
//! This module handles discovery, download, and management of different
//! manifest versions from CEDA. It provides compatibility checking and
//! version selection capabilities.

use std::path::PathBuf;
use tracing::info;

use super::types::ManifestVersion;
use crate::constants::{ceda, selectors};
use crate::errors::{ManifestError, ManifestResult};

/// Manager for multiple manifest versions
#[derive(Debug, Clone)]
pub struct ManifestVersionManager {
    /// Cache directory for manifest files
    cache_dir: PathBuf,
    /// Available manifest versions
    available_versions: Vec<ManifestVersion>,
    /// Current selected version
    selected_version: Option<ManifestVersion>,
}

impl ManifestVersionManager {
    /// Create a new manifest version manager
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            available_versions: Vec::new(),
            selected_version: None,
        }
    }

    /// Discover available manifest versions from remote
    pub async fn discover_available_versions(
        &mut self,
        client: &crate::app::CedaClient,
    ) -> ManifestResult<()> {
        use scraper::{Html, Selector};

        // Download the directory page HTML
        let html_content = client
            .download_file_content(ceda::MIDAS_DIRECTORY_URL)
            .await
            .map_err(|e| {
                ManifestError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let html_text = String::from_utf8(html_content).map_err(|e| {
            ManifestError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Parse HTML
        let document = Html::parse_document(&html_text);
        let selector = Selector::parse(selectors::MANIFEST_MD5_SELECTOR).map_err(|e| {
            ManifestError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Find all manifest links and extract versions
        let mut versions = Vec::new();

        for element in document.select(&selector) {
            if let Some(href) = element.value().attr("href") {
                let filename = href.split('/').last().unwrap_or(href);
                if let Some(version) = parse_manifest_version(filename) {
                    let full_url = if href.starts_with("http") {
                        href.to_string()
                    } else {
                        format!(
                            "{}{}",
                            ceda::MIDAS_DIRECTORY_URL.trim_end_matches('/'),
                            href
                        )
                    };
                    versions.push(ManifestVersion::new(
                        version,
                        filename.to_string(),
                        full_url,
                    ));
                }
            }
        }

        // Sort by version number
        versions.sort_by_key(|v| v.version);
        self.available_versions = versions;

        Ok(())
    }

    /// Discover locally cached manifest versions
    pub async fn discover_local_versions(&mut self) -> ManifestResult<()> {
        let mut versions = Vec::new();

        // Look for versioned manifest files in cache directory
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                        if let Some(version) = parse_manifest_version(filename) {
                            let mut manifest_version = ManifestVersion::new(
                                version,
                                filename.to_string(),
                                format!("{}/{}", ceda::MIDAS_OPEN_BASE_URL, filename),
                            );
                            manifest_version.local_path = Some(entry.path());

                            // Set file modification time
                            if let Ok(metadata) = entry.metadata() {
                                if let Ok(modified) = metadata.modified() {
                                    if let Ok(datetime) =
                                        modified.duration_since(std::time::UNIX_EPOCH)
                                    {
                                        manifest_version.last_updated = Some(
                                            chrono::DateTime::from_timestamp(
                                                datetime.as_secs() as i64,
                                                0,
                                            )
                                            .unwrap_or_else(chrono::Utc::now),
                                        );
                                    }
                                }
                            }

                            versions.push(manifest_version);
                        }
                    }
                }
            }
        }

        // Sort by version number
        versions.sort_by_key(|v| v.version);
        self.available_versions = versions;

        Ok(())
    }

    /// Get all available versions
    pub fn get_available_versions(&self) -> &[ManifestVersion] {
        &self.available_versions
    }

    /// Get the latest compatible version from cached versions
    pub fn get_latest_compatible_version(&self) -> Option<&ManifestVersion> {
        self.available_versions
            .iter()
            .rev()
            .find(|v| v.is_compatible == Some(true))
    }

    /// Select a specific version
    pub fn select_version(&mut self, version: u32) -> Result<(), ManifestError> {
        if let Some(manifest) = self
            .available_versions
            .iter()
            .find(|v| v.version == version)
        {
            self.selected_version = Some(manifest.clone());
            Ok(())
        } else {
            Err(ManifestError::NotFound {
                path: PathBuf::from(format!("manifest version {}", version)),
            })
        }
    }

    /// Get the selected version
    pub fn get_selected_version(&self) -> Option<&ManifestVersion> {
        self.selected_version.as_ref()
    }

    /// Auto-select the latest compatible version
    pub async fn auto_select_compatible_version(
        &mut self,
        client: &crate::app::CedaClient,
    ) -> ManifestResult<Option<ManifestVersion>> {
        // Check compatibility for all versions, starting from the latest
        let mut compatible_versions = Vec::new();

        for version in self.available_versions.iter().rev() {
            if let Ok(is_compatible) = self.check_version_compatibility(version, client).await {
                if is_compatible {
                    compatible_versions.push(version.clone());
                }
            }
        }

        if let Some(latest_compatible) = compatible_versions.first() {
            self.selected_version = Some(latest_compatible.clone());
            Ok(Some(latest_compatible.clone()))
        } else {
            Ok(None)
        }
    }

    /// Check if a manifest version is compatible with current data
    pub async fn check_version_compatibility(
        &self,
        version: &ManifestVersion,
        client: &crate::app::CedaClient,
    ) -> ManifestResult<bool> {
        // Extract dataset version from manifest version
        let dataset_version = format!("dataset-version-{}", version.version);

        // Check if this dataset version exists on the server
        // We'll check the uk-daily-rain-obs dataset as a representative sample
        let test_url = format!(
            "{}/data/uk-daily-rain-obs/{}/",
            ceda::MIDAS_OPEN_BASE_URL,
            dataset_version
        );

        match client.download_file_content(&test_url).await {
            Ok(_) => Ok(true),
            Err(crate::errors::DownloadError::NotFound { .. }) => Ok(false),
            Err(crate::errors::DownloadError::Forbidden { .. }) => Ok(false),
            Err(_) => Ok(false), // Assume incompatible on other errors
        }
    }

    /// Get the local path for a manifest version
    pub fn get_local_path(&self, version: &ManifestVersion) -> PathBuf {
        self.cache_dir.join(&version.filename)
    }

    /// Check if a manifest version is downloaded locally
    pub fn is_downloaded(&self, version: &ManifestVersion) -> bool {
        self.get_local_path(version).exists()
    }

    /// Download a specific manifest version
    pub async fn download_version(
        &mut self,
        version: &ManifestVersion,
        client: &crate::app::CedaClient,
    ) -> ManifestResult<()> {
        use std::fs::File;
        use std::io::Write;

        let content = client
            .download_file_content(&version.remote_url)
            .await
            .map_err(|e| {
                ManifestError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let local_path = self.get_local_path(version);

        // Create parent directory if it doesn't exist
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).map_err(ManifestError::Io)?;
        }

        let mut file = File::create(&local_path).map_err(ManifestError::Io)?;
        file.write_all(&content).map_err(ManifestError::Io)?;

        // Update the version info
        if let Some(mut_version) = self
            .available_versions
            .iter_mut()
            .find(|v| v.version == version.version)
        {
            mut_version.local_path = Some(local_path.clone());
            mut_version.last_updated = Some(chrono::Utc::now());
        }

        info!(
            "Downloaded manifest version {} to: {}",
            version.version,
            local_path.display()
        );
        Ok(())
    }
}

/// Parse version number from manifest filename
///
/// Consolidates the duplicate version parsing logic found throughout the codebase.
/// Looks for pattern "midas-open-v" followed by digits followed by "-md5s.txt"
pub fn parse_manifest_version(filename: &str) -> Option<u32> {
    if let Some(start) = filename.find("midas-open-v") {
        let version_start = start + "midas-open-v".len();
        if let Some(end) = filename[version_start..].find("-md5s.txt") {
            let version_str = &filename[version_start..version_start + end];
            version_str.parse::<u32>().ok()
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test manifest version extraction from filename
    ///
    /// Purpose: Verify that version parsing correctly extracts numeric versions
    /// from standard manifest filenames while rejecting invalid formats.
    /// Benefit: Ensures consistent version parsing across the application.
    #[test]
    fn test_manifest_version_parsing() {
        assert_eq!(
            parse_manifest_version("midas-open-v202407-md5s.txt"),
            Some(202407)
        );
        assert_eq!(
            parse_manifest_version("midas-open-v202501-md5s.txt"),
            Some(202501)
        );
        assert_eq!(
            parse_manifest_version("midas-open-v202312-md5s.txt"),
            Some(202312)
        );
        assert_eq!(parse_manifest_version("invalid-filename.txt"), None);
        assert_eq!(parse_manifest_version("midas-open-v-md5s.txt"), None);
        assert_eq!(parse_manifest_version("midas-open-vABC-md5s.txt"), None);
    }

    /// Test ManifestVersionManager creation and basic functionality
    ///
    /// Purpose: Verify that the version manager initializes correctly with empty state
    /// Benefit: Ensures the manager starts in a known, consistent state
    #[test]
    fn test_version_manager_creation() {
        let cache_dir = PathBuf::from("/tmp/test");
        let manager = ManifestVersionManager::new(cache_dir.clone());

        assert_eq!(manager.cache_dir, cache_dir);
        assert_eq!(manager.available_versions.len(), 0);
        assert!(manager.selected_version.is_none());
    }

    /// Test version selection functionality
    ///
    /// Purpose: Verify that version selection works correctly for existing and non-existing versions
    /// Benefit: Ensures proper error handling and state management for version selection
    #[test]
    fn test_version_selection() {
        let cache_dir = PathBuf::from("/tmp/test");
        let mut manager = ManifestVersionManager::new(cache_dir);

        // Add a test version
        let version = ManifestVersion::new(
            202407,
            "midas-open-v202407-md5s.txt".to_string(),
            "https://example.com/manifest.txt".to_string(),
        );
        manager.available_versions.push(version.clone());

        // Test successful selection
        assert!(manager.select_version(202407).is_ok());
        assert_eq!(manager.get_selected_version().unwrap().version, 202407);

        // Test failed selection
        assert!(manager.select_version(999999).is_err());
    }
}
