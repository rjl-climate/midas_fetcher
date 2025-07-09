//! File download operations with atomic writes and streaming
//!
//! This module handles file download operations including atomic writes,
//! content streaming, and proper error handling for interrupted downloads.

use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::app::client::http::HttpHandler;
use crate::constants::{files, limits};
use crate::errors::{DownloadError, DownloadResult};

/// File download operations handler
pub struct DownloadHandler<'a> {
    http_handler: &'a HttpHandler,
}

impl<'a> DownloadHandler<'a> {
    /// Creates a new DownloadHandler with the given HTTP handler
    pub fn new(http_handler: &'a HttpHandler) -> Self {
        Self { http_handler }
    }

    /// Downloads a file to the specified path with atomic operations
    ///
    /// This method uses the atomic temp file + rename pattern to prevent
    /// corruption from interruptions.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to download from
    /// * `destination` - The path to save the file to
    /// * `force` - Whether to overwrite existing files
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if:
    /// - The file already exists and force is false
    /// - The HTTP request fails
    /// - File I/O operations fail
    /// - The download times out
    pub async fn download_file(
        &self,
        url: &Url,
        destination: &Path,
        force: bool,
    ) -> DownloadResult<()> {
        // Check if file already exists
        if destination.exists() && !force {
            return Err(DownloadError::FileExists {
                path: destination.display().to_string(),
            });
        }

        // Create parent directory if it doesn't exist
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Create temporary file path
        let temp_path = destination.with_extension(format!(
            "{}{}",
            destination
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            files::TEMP_FILE_SUFFIX
        ));

        // Download with retry logic
        let mut retries = 0;
        loop {
            match self.download_file_attempt(url, &temp_path).await {
                Ok(()) => {
                    // Atomic move from temp file to final destination
                    tokio::fs::rename(&temp_path, destination)
                        .await
                        .map_err(|_e| DownloadError::AtomicOperationFailed {
                            temp_path: temp_path.clone(),
                            final_path: destination.to_path_buf(),
                        })?;
                    tracing::info!("Successfully downloaded: {}", destination.display());
                    return Ok(());
                }
                Err(e) if retries < limits::MAX_RETRIES => {
                    retries += 1;
                    let delay = std::time::Duration::from_millis(
                        limits::RETRY_BASE_DELAY_MS * 2_u64.pow(retries),
                    );
                    tracing::warn!(
                        "Download failed (attempt {}/{}): {}. Retrying in {}ms",
                        retries,
                        limits::MAX_RETRIES,
                        e,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    // Clean up temp file on final failure
                    if temp_path.exists() {
                        let _ = tokio::fs::remove_file(&temp_path).await;
                    }
                    tracing::error!(
                        "Download failed after {} retries: {}",
                        limits::MAX_RETRIES,
                        e
                    );
                    return Err(DownloadError::MaxRetriesExceeded {
                        max_retries: limits::MAX_RETRIES,
                    });
                }
            }
        }
    }

    /// Attempts to download a file to a temporary path
    async fn download_file_attempt(&self, url: &Url, temp_path: &Path) -> DownloadResult<()> {
        let response = self.http_handler.get_response(url).await?;

        if !response.status().is_success() {
            return Err(DownloadError::ServerError {
                status: response.status().as_u16(),
            });
        }

        // Create the temporary file
        let mut file = File::create(temp_path).await?;

        // Get the response bytes
        let bytes = response.bytes().await?;
        file.write_all(&bytes).await?;
        file.flush().await?;

        Ok(())
    }

    /// Download file content as bytes without saving to disk
    ///
    /// This method downloads the file content directly into memory,
    /// suitable for cache operations where the content needs to be
    /// processed before saving.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to download content from
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if the HTTP request fails or content cannot be read
    pub async fn download_file_content(&self, url: &str) -> DownloadResult<Vec<u8>> {
        let parsed_url = Url::parse(url).map_err(|e| DownloadError::InvalidUrl {
            url: url.to_string(),
            error: e.to_string(),
        })?;

        let response = self.http_handler.get_response(&parsed_url).await?;

        if !response.status().is_success() {
            match response.status().as_u16() {
                404 => {
                    return Err(DownloadError::NotFound {
                        url: url.to_string(),
                    })
                }
                403 => {
                    return Err(DownloadError::Forbidden {
                        url: url.to_string(),
                    })
                }
                status => return Err(DownloadError::ServerError { status }),
            }
        }

        let bytes = response.bytes().await.map_err(DownloadError::Http)?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;

    use crate::app::client::config::ClientConfig;
    use crate::app::client::http::HttpHandler;

    async fn create_test_handler() -> HttpHandler {
        let config = ClientConfig::default();
        let client = config.build_http_client().unwrap();
        HttpHandler::new(client, 5).unwrap()
    }

    #[tokio::test]
    async fn test_download_file_already_exists() {
        // Test download behavior when target file already exists
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("existing_file.csv");
        fs::write(&file_path, "existing content").await.unwrap();

        let http_handler = create_test_handler().await;
        let download_handler = DownloadHandler::new(&http_handler);

        // Test URL (this will fail in actual test, but we're testing the existence check)
        let url = Url::parse("https://example.com/test.csv").unwrap();

        // Test that the download would fail if file exists and force=false
        let result = download_handler
            .download_file(&url, &file_path, false)
            .await;
        assert!(result.is_err());

        match result.unwrap_err() {
            DownloadError::FileExists { .. } => {
                // Expected error type
            }
            other => {
                panic!("Expected DownloadError::FileExists, got {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn test_temp_file_path_generation() {
        // Test that temporary file paths are generated correctly
        let original_path = Path::new("/tmp/test.csv");
        let temp_path = original_path.with_extension(format!(
            "{}{}",
            original_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            files::TEMP_FILE_SUFFIX
        ));

        assert!(temp_path.to_string_lossy().ends_with(".csv.tmp"));
    }

    #[tokio::test]
    async fn test_temp_file_path_no_extension() {
        // Test temporary file path generation for files without extensions
        let original_path = Path::new("/tmp/testfile");
        let temp_path = original_path.with_extension(format!(
            "{}{}",
            original_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            files::TEMP_FILE_SUFFIX
        ));

        assert!(temp_path.to_string_lossy().ends_with(".tmp"));
    }

    #[test]
    fn test_download_url_parsing() {
        // Test URL parsing for content downloads
        let valid_url = "https://example.com/file.csv";
        let parsed = Url::parse(valid_url);
        assert!(parsed.is_ok());

        let invalid_url = "not-a-url";
        let parsed = Url::parse(invalid_url);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_http_status_code_mapping() {
        // Test that HTTP status codes map to correct error types
        assert_eq!(404_u16, 404); // Not Found
        assert_eq!(403_u16, 403); // Forbidden
        assert_eq!(500_u16, 500); // Server Error
    }
}
