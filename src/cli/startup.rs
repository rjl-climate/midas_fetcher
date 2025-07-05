//! Startup validation and checks for MIDAS Fetcher
//!
//! This module provides comprehensive startup validation including authentication
//! checks, manifest file detection, and user prompts for initial setup.

use std::io::{self, Write};
use std::path::Path;

use tracing::{debug, info, warn};

use crate::auth::{ensure_authenticated, get_auth_status};
use crate::errors::{AppError, Result};

/// Results of startup validation checks
#[derive(Debug, Clone)]
pub struct StartupStatus {
    /// Whether authentication is properly configured
    pub auth_configured: bool,
    /// Whether manifest files are available
    pub manifest_available: bool,
    /// List of missing manifest files
    pub missing_manifests: Vec<String>,
    /// Whether the system is ready for operations
    pub ready_for_operations: bool,
}

impl StartupStatus {
    /// Create a new startup status with default values
    pub fn new() -> Self {
        Self {
            auth_configured: false,
            manifest_available: false,
            missing_manifests: Vec::new(),
            ready_for_operations: false,
        }
    }

    /// Check if startup validation passed
    pub fn is_ready(&self) -> bool {
        self.auth_configured && self.manifest_available
    }

    /// Get a summary message for display
    pub fn summary(&self) -> String {
        if self.is_ready() {
            "✅ System ready for operations".to_string()
        } else {
            let mut issues = Vec::new();
            if !self.auth_configured {
                issues.push("authentication not configured");
            }
            if !self.manifest_available {
                issues.push("manifest files missing");
            }
            format!("⚠️  Setup required: {}", issues.join(", "))
        }
    }
}

impl Default for StartupStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive startup validation
///
/// Performs all necessary checks to ensure the system is ready for operation,
/// including authentication, manifest files, and configuration validation.
///
/// # Arguments
///
/// * `require_auth` - Whether authentication is required for the operation
/// * `require_manifest` - Whether manifest files are required
///
/// # Returns
///
/// `StartupStatus` indicating what checks passed or failed
pub async fn validate_startup(require_auth: bool, require_manifest: bool) -> Result<StartupStatus> {
    let mut status = StartupStatus::new();

    info!("Performing startup validation...");

    // Check authentication if required
    if require_auth {
        status.auth_configured = check_authentication().await?;
    } else {
        status.auth_configured = true; // Not required, so consider it "ok"
    }

    // Check manifest files if required
    if require_manifest {
        status.manifest_available = check_manifest_files(&mut status.missing_manifests).await?;
    } else {
        status.manifest_available = true; // Not required, so consider it "ok"
    }

    // Overall readiness
    status.ready_for_operations = status.is_ready();

    debug!("Startup validation completed: {}", status.summary());

    Ok(status)
}

/// Check authentication configuration and prompt for setup if needed
async fn check_authentication() -> Result<bool> {
    debug!("Checking authentication configuration...");

    let auth_status = get_auth_status();

    if auth_status.has_credentials() {
        info!("✅ Authentication credentials found");
        return Ok(true);
    }

    warn!("⚠️  Authentication credentials not found");

    // Prompt user for authentication setup
    match ensure_authenticated().await {
        Ok(()) => {
            info!("✅ Authentication setup completed");
            Ok(true)
        }
        Err(e) => {
            warn!("❌ Authentication setup failed: {}", e);
            Ok(false)
        }
    }
}

/// Check for manifest files and detect missing ones
async fn check_manifest_files(missing_manifests: &mut Vec<String>) -> Result<bool> {
    use crate::app::CacheManager;

    debug!("Checking for manifest files...");

    // Get cache directory
    let cache = CacheManager::new(Default::default())
        .await
        .map_err(AppError::Cache)?;
    let cache_root = cache.cache_root();

    // Check for versioned manifest files (midas-open-v*-md5s.txt)
    let mut found_any = false;

    // Look for any files matching the manifest pattern in cache directory
    if let Ok(entries) = std::fs::read_dir(cache_root) {
        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                    debug!("✅ Found manifest file: {}", entry.path().display());
                    found_any = true;
                } else if filename == "manifest.txt" {
                    debug!("✅ Found legacy manifest file: {}", entry.path().display());
                    found_any = true;
                }
            }
        }
    }

    if !found_any {
        missing_manifests.push("midas-open-v*-md5s.txt".to_string());
    }

    if !found_any {
        println!("⚠️  No manifest files found");
        prompt_manifest_update().await?;

        // Check again after potential download
        found_any = false;
        missing_manifests.clear();

        // Recheck for versioned manifest files in cache directory
        if let Ok(entries) = std::fs::read_dir(cache_root) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                        debug!(
                            "✅ Found manifest file after download: {}",
                            entry.path().display()
                        );
                        found_any = true;
                    } else if filename == "manifest.txt" {
                        debug!(
                            "✅ Found legacy manifest file after download: {}",
                            entry.path().display()
                        );
                        found_any = true;
                    }
                }
            }
        }

        if !found_any {
            missing_manifests.push("midas-open-v*-md5s.txt".to_string());
        }

        if found_any {
            info!("✅ Manifest files now available");
        }
    } else {
        info!("✅ Manifest files available");
    }

    Ok(found_any)
}

/// Prompt user to update manifest files
async fn prompt_manifest_update() -> Result<()> {
    println!();
    println!("📋 Manifest files are required to discover available datasets.");
    println!("   Manifest files contain the list of all available files and their checksums.");
    println!();

    print!("Would you like to download the latest manifest now? [Y/n]: ");
    io::stdout().flush().map_err(AppError::Io)?;

    let mut response = String::new();
    io::stdin().read_line(&mut response).map_err(AppError::Io)?;

    if response.trim().to_lowercase().starts_with('n') {
        println!("📝 To download manifests manually later, run:");
        println!("   midas_fetcher manifest update");
        return Ok(());
    }

    println!();

    // Create progress spinner for manifest download
    use indicatif::{ProgressBar, ProgressStyle};
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    spinner.set_message("Downloading latest manifest files...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));

    // Actually download the manifest
    match download_manifest_files().await {
        Ok(()) => {
            spinner.finish_with_message("✅ Manifest files downloaded successfully!");
        }
        Err(e) => {
            spinner.finish_with_message("❌ Failed to download manifest files");
            warn!("Failed to download manifest files: {}", e);
            println!("📝 You can try downloading manually with:");
            println!("   midas_fetcher manifest update");
            println!();
            println!("💡 Or download manually from:");
            println!("   https://data.ceda.ac.uk/badc/ukmo-midas-open/");
        }
    }

    Ok(())
}

/// Parse version number from manifest filename
///
/// Extracts version like "202507" from "midas-open-v202507-md5s.txt"
pub fn parse_manifest_version(filename: &str) -> Option<u32> {
    // Look for pattern "midas-open-v" followed by digits followed by "-md5s.txt"
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

/// Find the latest manifest URL by parsing the directory page
async fn find_latest_manifest_url(client: &crate::app::CedaClient) -> Result<String> {
    use crate::constants::selectors;
    use scraper::{Html, Selector};

    let directory_url = "https://data.ceda.ac.uk/badc/ukmo-midas-open/";

    info!("Fetching directory listing from: {}", directory_url);

    // Download the directory page HTML
    let html_content = client
        .download_file_content(directory_url)
        .await
        .map_err(AppError::Download)?;

    let html_text = String::from_utf8(html_content)
        .map_err(|e| AppError::generic(format!("Invalid UTF-8 in directory page: {}", e)))?;

    // Parse HTML
    let document = Html::parse_document(&html_text);
    let selector = Selector::parse(selectors::MANIFEST_MD5_SELECTOR)
        .map_err(|e| AppError::generic(format!("Invalid CSS selector: {}", e)))?;

    // Find all manifest links and extract versions
    let mut manifest_files: Vec<(u32, String)> = Vec::new();

    for element in document.select(&selector) {
        if let Some(href) = element.value().attr("href") {
            let filename = href.split('/').last().unwrap_or(href);
            if let Some(version) = parse_manifest_version(filename) {
                let full_url = if href.starts_with("http") {
                    href.to_string()
                } else {
                    format!("{}{}", directory_url.trim_end_matches('/'), href)
                };
                manifest_files.push((version, full_url));
                debug!("Found manifest: {} (version: {})", filename, version);
            }
        }
    }

    if manifest_files.is_empty() {
        return Err(AppError::generic("No manifest files found in directory"));
    }

    // Sort by version number and get the latest
    manifest_files.sort_by_key(|&(version, _)| version);
    let (latest_version, latest_url) = manifest_files.into_iter().last().unwrap();

    info!("Latest manifest version: {}", latest_version);
    Ok(latest_url)
}

/// Status information about manifest updates
#[derive(Debug, Clone)]
pub struct ManifestUpdateStatus {
    /// Local manifest version (None if no local manifest)
    pub local_version: Option<u32>,
    /// Latest remote version available
    pub remote_version: u32,
    /// Whether an update is needed
    pub needs_update: bool,
    /// Age of local manifest in days (None if no local manifest)
    pub local_age_days: Option<u64>,
    /// Local manifest filename (None if no local manifest)
    pub local_filename: Option<String>,
    /// Remote manifest filename
    pub remote_filename: String,
}

impl ManifestUpdateStatus {
    /// Get a user-friendly status message
    pub fn status_message(&self) -> String {
        if self.needs_update {
            if self.local_version.is_some() {
                "⚠️  Update available"
            } else {
                "📥 No local manifest - download recommended"
            }
        } else {
            "✅ Up to date"
        }
        .to_string()
    }

    /// Get age description
    pub fn age_description(&self) -> String {
        match self.local_age_days {
            None => "Not found".to_string(),
            Some(0) => "Downloaded today".to_string(),
            Some(1) => "Downloaded yesterday".to_string(),
            Some(days) => format!("Downloaded {} days ago", days),
        }
    }
}

/// Get the latest remote manifest version from CEDA
pub async fn get_latest_remote_version(client: &crate::app::CedaClient) -> Result<(u32, String)> {
    let latest_url = find_latest_manifest_url(client).await?;
    let filename = latest_url
        .split('/')
        .last()
        .unwrap_or("unknown")
        .to_string();

    let version = parse_manifest_version(&filename).ok_or_else(|| {
        AppError::generic("Could not parse version from remote manifest filename")
    })?;

    Ok((version, filename))
}

/// Get the local manifest version from cache directory
pub async fn get_local_manifest_version(
    cache_manager: &crate::app::CacheManager,
) -> Result<Option<(u32, String, u64)>> {
    let cache_root = cache_manager.cache_root();

    // Look for versioned manifest files in cache directory
    if let Ok(entries) = std::fs::read_dir(cache_root) {
        let mut versioned_manifests: Vec<(u32, String, u64)> = Vec::new();

        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                    if let Some(version) = parse_manifest_version(filename) {
                        // Get file age in days
                        if let Ok(metadata) = entry.metadata() {
                            if let Ok(modified) = metadata.modified() {
                                let age = modified.elapsed().unwrap_or_default().as_secs()
                                    / (24 * 60 * 60); // Convert to days
                                versioned_manifests.push((version, filename.to_string(), age));
                            }
                        }
                    }
                }
            }
        }

        if !versioned_manifests.is_empty() {
            // Sort by version and return the latest
            versioned_manifests.sort_by_key(|&(version, _, _)| version);
            let (version, filename, age) = versioned_manifests.into_iter().last().unwrap();
            return Ok(Some((version, filename, age)));
        }
    }

    Ok(None)
}

/// Check if manifest needs updating
pub async fn check_manifest_update_needed() -> Result<ManifestUpdateStatus> {
    use crate::app::{CacheManager, CedaClient};

    // Create clients
    let client = CedaClient::new().await.map_err(AppError::Auth)?;
    let cache = CacheManager::new(Default::default())
        .await
        .map_err(AppError::Cache)?;

    // Get remote version
    let (remote_version, remote_filename) = get_latest_remote_version(&client).await?;

    // Get local version
    let local_info = get_local_manifest_version(&cache).await?;

    let (local_version, local_filename, local_age_days) = match local_info {
        Some((version, filename, age)) => (Some(version), Some(filename), Some(age)),
        None => (None, None, None),
    };

    let needs_update = match local_version {
        None => true,                          // No local manifest
        Some(local) => local < remote_version, // Local is older
    };

    Ok(ManifestUpdateStatus {
        local_version,
        remote_version,
        needs_update,
        local_age_days,
        local_filename,
        remote_filename,
    })
}

/// Download manifest files from CEDA
pub async fn download_manifest_files() -> Result<()> {
    use crate::app::{CacheManager, CedaClient};
    use std::fs::File;
    use std::io::Write;

    // Create authenticated client
    let client = CedaClient::new().await.map_err(AppError::Auth)?;

    // Get cache directory
    let cache = CacheManager::new(Default::default())
        .await
        .map_err(AppError::Cache)?;
    let cache_root = cache.cache_root();

    // Dynamically find the latest manifest URL
    let manifest_url = find_latest_manifest_url(&client).await?;

    info!("Downloading manifest from: {}", manifest_url);

    match client.download_file_content(&manifest_url).await {
        Ok(content) => {
            // Extract filename from URL for local saving
            let filename = manifest_url
                .split('/')
                .last()
                .unwrap_or("midas-open-latest-md5s.txt");

            // Save to cache root directory
            let manifest_path = cache_root.join(filename);
            let mut file = File::create(&manifest_path).map_err(AppError::Io)?;
            file.write_all(&content).map_err(AppError::Io)?;

            info!("Manifest saved to: {}", manifest_path.display());

            Ok(())
        }
        Err(e) => {
            warn!("Failed to download manifest: {}", e);
            Err(AppError::Download(e))
        }
    }
}

/// Interactive dataset selection when not specified
///
/// # Arguments
///
/// * `manifest_path` - Path to the manifest file to analyze
/// * `specified_dataset` - Dataset name if already specified
///
/// # Returns
///
/// Selected dataset name
pub async fn interactive_selection(
    manifest_path: &Path,
    specified_dataset: Option<&str>,
) -> Result<String> {
    use crate::app::get_selection_options;

    // If dataset is specified, no interaction needed
    if let Some(dataset) = specified_dataset {
        return Ok(dataset.to_string());
    }

    debug!("Starting interactive selection...");

    // Get available datasets
    let (available_datasets, _) = get_selection_options(manifest_path)
        .await
        .map_err(AppError::Manifest)?;

    if available_datasets.is_empty() {
        return Err(AppError::generic("No datasets found in manifest"));
    }

    // Select dataset interactively
    let selected_dataset = select_dataset(&available_datasets)?;

    Ok(selected_dataset)
}

/// Interactive dataset selection
fn select_dataset(available_datasets: &[String]) -> Result<String> {
    println!();
    println!("📊 Available Datasets:");
    for (i, dataset) in available_datasets.iter().enumerate() {
        println!("  {}. {}", i + 1, dataset);
    }
    println!();

    loop {
        print!("Select dataset (1-{}): ", available_datasets.len());
        io::stdout().flush().map_err(AppError::Io)?;

        let mut input = String::new();
        io::stdin().read_line(&mut input).map_err(AppError::Io)?;

        if let Ok(choice) = input.trim().parse::<usize>() {
            if choice > 0 && choice <= available_datasets.len() {
                let selected = available_datasets[choice - 1].clone();
                println!("✅ Selected dataset: {}", selected);
                return Ok(selected);
            }
        }

        println!(
            "❌ Invalid choice. Please enter a number between 1 and {}",
            available_datasets.len()
        );
    }
}

/// Show startup status and recommendations
pub fn show_startup_status(status: &StartupStatus) {
    println!();
    println!("🚀 MIDAS Fetcher Startup Status");
    println!("================================");
    println!();

    // Authentication status
    if status.auth_configured {
        println!("🔐 Authentication: ✅ Configured");
    } else {
        println!("🔐 Authentication: ❌ Not configured");
        println!("   Run: midas_fetcher auth setup");
    }

    // Manifest status
    if status.manifest_available {
        println!("📋 Manifest files: ✅ Available");
    } else {
        println!("📋 Manifest files: ❌ Missing");
        if !status.missing_manifests.is_empty() {
            println!("   Missing: {}", status.missing_manifests.join(", "));
        }
        println!("   Run: midas_fetcher manifest update");
    }

    println!();
    println!("Overall: {}", status.summary());
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_status_creation() {
        let status = StartupStatus::new();
        assert!(!status.auth_configured);
        assert!(!status.manifest_available);
        assert!(!status.is_ready());
    }

    #[test]
    fn test_startup_status_ready() {
        let mut status = StartupStatus::new();
        assert!(!status.is_ready());

        status.auth_configured = true;
        assert!(!status.is_ready());

        status.manifest_available = true;
        assert!(status.is_ready());
    }

    #[test]
    fn test_startup_status_summary() {
        let mut status = StartupStatus::new();
        assert!(status.summary().contains("Setup required"));

        status.auth_configured = true;
        status.manifest_available = true;
        assert!(status.summary().contains("ready for operations"));
    }
}
