//! HTTP client implementation for CEDA archive interaction
//!
//! This module provides a comprehensive HTTP client with authentication, rate limiting,
//! exponential backoff, and circuit breaker patterns for reliable CEDA archive access.
//!
//! The module is organized into specialized components:
//! - `config`: HTTP client configuration and building
//! - `auth`: CEDA authentication flow with CSRF handling
//! - `http`: Core HTTP operations with resilience patterns
//! - `download`: File download operations with atomic writes

use std::env;
use std::path::Path;

use url::Url;

use crate::constants::{ceda, env as env_constants};
use crate::errors::{AuthResult, DownloadResult};

// Module declarations
pub mod auth;
pub mod config;
pub mod download;
pub mod http;

// Re-export public types for backward compatibility
pub use config::ClientConfig;

use auth::AuthHandler;
use download::DownloadHandler;
use http::HttpHandler;

/// HTTP client for interacting with the CEDA archive
///
/// Handles authentication, rate limiting, and file downloads
/// with proper error handling and retry logic.
#[derive(Debug)]
pub struct CedaClient {
    http_handler: HttpHandler,
    base_url: Url,
}

impl CedaClient {
    /// Creates a new CedaClient without authentication (for testing)
    ///
    /// This creates a client that can make unauthenticated requests to CEDA.
    /// Useful for testing or accessing public resources.
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if HTTP client creation fails
    pub async fn new_simple() -> AuthResult<Self> {
        Self::new_simple_with_config(ClientConfig::default()).await
    }

    /// Creates a new CedaClient with custom configuration (for testing)
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration settings
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if HTTP client creation fails
    pub async fn new_simple_with_config(config: ClientConfig) -> AuthResult<Self> {
        let client = config.build_http_client()?;
        let http_handler = HttpHandler::new(client, config.rate_limit_rps)?;
        let base_url = Url::parse(ceda::BASE_URL).expect("Base URL should be valid");

        tracing::info!("Created simple CEDA client without authentication");

        Ok(Self {
            http_handler,
            base_url,
        })
    }

    /// Creates a new CedaClient and authenticates with the CEDA service
    ///
    /// This follows the working authentication pattern with CSRF token extraction.
    /// It fetches the login page, extracts the CSRF token, and submits the form.
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if:
    /// - Environment variables are missing
    /// - HTTP client creation fails
    /// - Authentication request fails
    /// - Login credentials are invalid
    pub async fn new() -> AuthResult<Self> {
        Self::new_with_config(ClientConfig::default()).await
    }

    /// Creates a new CedaClient with custom configuration and authenticates with the CEDA service
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration settings
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if authentication fails
    pub async fn new_with_config(config: ClientConfig) -> AuthResult<Self> {
        // Load credentials from environment variables
        let username = env::var(env_constants::USERNAME)?;
        let password = env::var(env_constants::PASSWORD)?;

        let client = config.build_http_client()?;
        let http_handler = HttpHandler::new(client, config.rate_limit_rps)?;
        let base_url = Url::parse(ceda::BASE_URL).expect("Base URL should be valid");

        // Perform authentication
        AuthHandler::authenticate(http_handler.client(), &username, &password).await?;

        tracing::info!("Successfully authenticated with CEDA");

        Ok(Self {
            http_handler,
            base_url,
        })
    }

    /// Fetches the HTTP response with rate limiting and retry logic
    ///
    /// This method returns the raw reqwest::Response for streaming downloads.
    /// Use `get_page()` if you need the response body as text.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to fetch
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if the HTTP request fails after retries
    pub async fn get_response(&self, url: &Url) -> DownloadResult<reqwest::Response> {
        self.http_handler.get_response(url).await
    }

    /// Fetches the HTML content of a web page with rate limiting
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to fetch
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if the HTTP request fails after retries
    pub async fn get_page(&self, url: &Url) -> DownloadResult<String> {
        self.http_handler.get_page(url).await
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
        let download_handler = DownloadHandler::new(&self.http_handler);
        download_handler
            .download_file(url, destination, force)
            .await
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
        let download_handler = DownloadHandler::new(&self.http_handler);
        download_handler.download_file_content(url).await
    }

    /// Get the base URL for the CEDA archive
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::AuthError;

    #[tokio::test]
    async fn test_simple_client_creation() {
        // Test that simple client can be created without authentication
        let result = CedaClient::new_simple().await;
        assert!(result.is_ok());

        let client = result.unwrap();
        // URL might be normalized with trailing slash
        assert!(
            client.base_url.as_str() == ceda::BASE_URL
                || client.base_url.as_str() == format!("{}/", ceda::BASE_URL)
        );
    }

    #[test]
    fn test_client_creation_without_env() {
        // Test that authenticated client creation fails without environment variables
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Clear environment variables for this test
        unsafe {
            env::remove_var(env_constants::USERNAME);
            env::remove_var(env_constants::PASSWORD);
        }

        let result = rt.block_on(CedaClient::new());
        assert!(result.is_err());

        match result.unwrap_err() {
            AuthError::EnvVar(_) => {
                // Expected error type
            }
            other => {
                panic!("Expected AuthError::EnvVar, got {:?}", other);
            }
        }
    }

    #[test]
    fn test_base_url_access() {
        // Test that base URL can be accessed and is correctly parsed
        let base_url = Url::parse(ceda::BASE_URL).unwrap();
        assert_eq!(base_url.scheme(), "https");
        assert_eq!(base_url.host_str(), Some("data.ceda.ac.uk"));
    }
}
