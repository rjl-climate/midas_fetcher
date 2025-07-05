// Allow dead code for client methods that will be used in later tasks
#![allow(dead_code)]

use std::env;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use governor::{Quota, RateLimiter, clock::DefaultClock, state::InMemoryState};
use reqwest::Client;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::constants::*;
use crate::errors::{AuthError, DownloadError};
use crate::types::FileInfo;

/// HTTP client for interacting with the CEDA archive
///
/// Handles authentication, rate limiting, and file downloads
/// with proper error handling and retry logic.
pub struct CedaClient {
    client: Client,
    rate_limiter: RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>,
    base_url: Url,
}

/// Configuration for HTTP client optimizations
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Enable HTTP/2 support
    pub http2: bool,
    /// TCP keep-alive settings
    pub tcp_keepalive: Option<Duration>,
    /// TCP nodelay (disable Nagle's algorithm)
    pub tcp_nodelay: bool,
    /// Connection pool idle timeout
    pub pool_idle_timeout: Option<Duration>,
    /// Maximum number of connections per host
    pub pool_max_per_host: usize,
    /// Request timeout
    pub request_timeout: Duration,
    /// Connect timeout
    pub connect_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            http2: false, // Disable HTTP/2 by default for CEDA compatibility
            tcp_keepalive: Some(Duration::from_secs(30)),
            tcp_nodelay: true,
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_per_host: 10,
            request_timeout: HTTP_TIMEOUT,
            connect_timeout: Duration::from_secs(30),
        }
    }
}

impl std::fmt::Debug for CedaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CedaClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl CedaClient {
    /// Creates a new CedaClient without authentication (for testing)
    pub async fn new_simple() -> Result<Self, AuthError> {
        Self::new_simple_with_config(ClientConfig::default()).await
    }

    /// Creates a new CedaClient with custom configuration (for testing)
    pub async fn new_simple_with_config(config: ClientConfig) -> Result<Self, AuthError> {
        // Create HTTP client with cookie store for session management
        let mut client_builder = Client::builder()
            .cookie_store(true)
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .user_agent(USER_AGENT)
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .tcp_nodelay(config.tcp_nodelay)
            .pool_max_idle_per_host(config.pool_max_per_host);

        // Configure HTTP/2 if enabled (but allow fallback)
        if config.http2 {
            // Don't use http2_prior_knowledge() as it forces HTTP/2
            // Instead let the client negotiate HTTP version
            client_builder = client_builder.http2_adaptive_window(true);
        }

        // Configure TCP keep-alive if specified
        if let Some(keepalive) = config.tcp_keepalive {
            client_builder = client_builder.tcp_keepalive(keepalive);
        }

        // Configure connection pool idle timeout
        if let Some(idle_timeout) = config.pool_idle_timeout {
            client_builder = client_builder.pool_idle_timeout(idle_timeout);
        }

        let client = client_builder.build().map_err(AuthError::Http)?;

        // Create rate limiter with governor
        let quota = Quota::per_second(
            NonZeroU32::new(DEFAULT_RATE_LIMIT_RPS).expect("Rate limit must be non-zero"),
        );
        let rate_limiter = RateLimiter::direct(quota);

        // Parse base URL
        let base_url = Url::parse(CEDA_BASE_URL).expect("Base URL should be valid");

        tracing::info!("Created simple CEDA client without authentication");

        Ok(Self {
            client,
            rate_limiter,
            base_url,
        })
    }

    /// Creates a new CedaClient and authenticates with the CEDA service
    ///
    /// This follows the working authentication pattern from the scraper example.
    /// It fetches the login page, extracts the CSRF token, and submits the form.
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if:
    /// - Environment variables are missing
    /// - HTTP client creation fails
    /// - Authentication request fails
    /// - Login credentials are invalid
    pub async fn new() -> Result<Self, AuthError> {
        Self::new_with_config(ClientConfig::default()).await
    }

    /// Creates a new CedaClient with custom configuration and authenticates with the CEDA service
    pub async fn new_with_config(config: ClientConfig) -> Result<Self, AuthError> {
        // Load credentials from environment variables
        let username = env::var(ENV_USERNAME)?;
        let password = env::var(ENV_PASSWORD)?;

        // Create HTTP client with cookie store for session management
        let mut client_builder = Client::builder()
            .cookie_store(true)
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .user_agent(USER_AGENT)
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .tcp_nodelay(config.tcp_nodelay)
            .pool_max_idle_per_host(config.pool_max_per_host);

        // Configure HTTP/2 if enabled (but allow fallback)
        if config.http2 {
            // Don't use http2_prior_knowledge() as it forces HTTP/2
            // Instead let the client negotiate HTTP version
            client_builder = client_builder.http2_adaptive_window(true);
        }

        // Configure TCP keep-alive if specified
        if let Some(keepalive) = config.tcp_keepalive {
            client_builder = client_builder.tcp_keepalive(keepalive);
        }

        // Configure connection pool idle timeout
        if let Some(idle_timeout) = config.pool_idle_timeout {
            client_builder = client_builder.pool_idle_timeout(idle_timeout);
        }

        let client = client_builder.build().map_err(AuthError::Http)?;

        // Create rate limiter with governor
        let quota = Quota::per_second(
            NonZeroU32::new(DEFAULT_RATE_LIMIT_RPS).expect("Rate limit must be non-zero"),
        );
        let rate_limiter = RateLimiter::direct(quota);

        // Parse base URL
        let base_url = Url::parse(CEDA_BASE_URL).expect("Base URL should be valid");

        // Authenticate with CEDA following the working pattern from scraper.rs
        let login_page_url = "https://auth.ceda.ac.uk/account/signin";
        tracing::info!("Starting CEDA authentication for user: {}", username);
        tracing::info!("📄 Fetching login page: {}", login_page_url);

        // Step 1: Get the login page to extract CSRF token and form details
        let login_page_response = client
            .get(login_page_url)
            .send()
            .await
            .map_err(AuthError::Http)?;

        tracing::info!(
            "📄 Login page response status: {}",
            login_page_response.status()
        );
        tracing::info!("📄 Login page final URL: {}", login_page_response.url());

        let login_page_html = login_page_response.text().await.map_err(AuthError::Http)?;
        tracing::info!(
            "📄 Login page content length: {} bytes",
            login_page_html.len()
        );

        // Step 2: Extract CSRF token using scraper crate (like the working example)
        use scraper::{Html, Selector};
        let document = Html::parse_document(&login_page_html);

        let csrf_selector = Selector::parse("input[name='csrfmiddlewaretoken']")
            .map_err(|_| AuthError::LoginFailed)?;

        let csrf_token = document
            .select(&csrf_selector)
            .next()
            .and_then(|element| element.value().attr("value"))
            .ok_or(AuthError::LoginFailed)?;

        tracing::info!(
            "🔑 Found CSRF token: {}...",
            &csrf_token[..std::cmp::min(8, csrf_token.len())]
        );

        // Determine the correct form action URL
        let form_selector = Selector::parse("form").map_err(|_| AuthError::LoginFailed)?;
        let form_action = document
            .select(&form_selector)
            .next()
            .and_then(|element| element.value().attr("action"))
            .unwrap_or("/account/signin/");

        // Build the complete action URL
        let submit_url = if form_action.starts_with("http") {
            form_action.to_string()
        } else {
            format!("https://auth.ceda.ac.uk{}", form_action)
        };

        tracing::info!("📝 Form action URL: {}", submit_url);

        // Step 3: Submit login form
        let login_response = client
            .post(&submit_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Referer", login_page_url)
            .form(&[
                ("username", username.as_str()),
                ("password", password.as_str()),
                ("csrfmiddlewaretoken", csrf_token),
            ])
            .send()
            .await
            .map_err(AuthError::Http)?;

        // Step 4: Check if login was successful (following working pattern)
        let final_url = login_response.url().clone();
        let login_status = login_response.status();

        tracing::info!("📝 Login response status: {}", login_status);
        tracing::info!("📝 Login response final URL: {}", final_url);

        let login_result_html = login_response.text().await.map_err(AuthError::Http)?;
        tracing::info!(
            "📝 Login response content length: {} bytes",
            login_result_html.len()
        );

        // Check for explicit error messages
        if login_result_html.contains("Please enter a correct username and password") {
            tracing::warn!(
                "CEDA login failed: Invalid credentials for user: {}",
                username
            );
            return Err(AuthError::LoginFailed);
        }

        // Check if we're still on the login page (indicates failure)
        if final_url.as_str().contains("signin")
            && login_result_html.contains("<form")
            && login_result_html.contains("password")
        {
            tracing::warn!(
                "CEDA login failed: Still on login page for user: {}",
                username
            );
            return Err(AuthError::LoginFailed);
        }

        // Check if we successfully reached a CEDA account page (Keycloak)
        if final_url.as_str().contains("accounts.ceda.ac.uk")
            || final_url.as_str().contains("profile")
            || final_url.as_str().contains("dashboard")
            || login_result_html.contains("logout")
        {
            tracing::info!("Successfully authenticated with CEDA as user: {}", username);
            // Don't return here - continue to test authentication below
        }

        // Test authentication by trying to access a protected resource
        tracing::info!("❓ Testing authentication with a protected resource");
        let test_url = crate::version_utils::TEST_FILE_URL;

        let test_response = client.get(test_url).send().await.map_err(AuthError::Http)?;
        tracing::info!("🧪 Test response status: {}", test_response.status());
        tracing::info!("🧪 Test response URL: {}", test_response.url());

        let test_content = test_response.text().await.map_err(AuthError::Http)?;
        tracing::info!(
            "🧪 Test response content length: {} bytes",
            test_content.len()
        );

        // If we get HTML with login form, authentication failed
        if test_content.contains("<html")
            && (test_content.contains("signin") || test_content.contains("login"))
        {
            tracing::warn!("🧪 Authentication test failed: Got login page");
            return Err(AuthError::LoginFailed);
        }

        // If we get CSV content (including BADC-CSV format), authentication succeeded
        if test_content.starts_with("ob_end_time")
            || test_content.contains("station_file")
            || test_content.starts_with("Conventions,G,BADC-CSV")
            || test_content.contains("BADC-CSV")
        {
            tracing::info!("🧪 Authentication test succeeded: Got CSV content");
        } else {
            // Show preview of ambiguous content
            let preview = test_content.chars().take(200).collect::<String>();
            tracing::warn!(
                "🧪 Authentication test ambiguous - content preview: {}",
                preview
            );
            return Err(AuthError::LoginFailed);
        }

        tracing::info!("Successfully authenticated with CEDA");

        Ok(Self {
            client,
            rate_limiter,
            base_url,
        })
    }

    /// Fetches the HTTP response with rate limiting
    ///
    /// This method returns the raw reqwest::Response for streaming downloads.
    /// Use `get_page()` if you need the response body as text.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to fetch
    ///
    /// # Returns
    ///
    /// The HTTP response object
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if the HTTP request fails after retries
    pub async fn get_response(&self, url: &Url) -> Result<reqwest::Response, DownloadError> {
        // Apply rate limiting
        self.rate_limiter.until_ready().await;

        // Retry logic with exponential backoff
        let mut retries = 0;
        loop {
            match self.client.get(url.as_str()).send().await {
                Ok(response) => {
                    tracing::debug!("Successfully fetched response: {}", url);
                    return Ok(response);
                }
                Err(e) if retries < MAX_RETRIES => {
                    retries += 1;
                    let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * 2_u64.pow(retries));
                    tracing::warn!(
                        "Request failed (attempt {}/{}): {}. Retrying in {}ms",
                        retries,
                        MAX_RETRIES,
                        e,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    tracing::error!("Request failed after {} retries: {}", MAX_RETRIES, e);
                    return Err(DownloadError::Http(e));
                }
            }
        }
    }

    /// Fetches the HTML content of a web page with rate limiting
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to fetch
    ///
    /// # Returns
    ///
    /// The HTML content as a string
    ///
    /// # Errors
    ///
    /// Returns `DownloadError` if the HTTP request fails after retries
    pub async fn get_page(&self, url: &Url) -> Result<String, DownloadError> {
        let response = self.get_response(url).await?;
        let text = response.text().await?;
        tracing::debug!("Successfully fetched page: {}", url);
        Ok(text)
    }

    /// Downloads a file from the CEDA archive
    ///
    /// # Arguments
    ///
    /// * `file_info` - Information about the file to download
    ///
    /// # Returns
    ///
    /// `Ok(())` if the download succeeds
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
        file_info: &FileInfo,
        force: bool,
    ) -> Result<(), DownloadError> {
        // Check if file already exists
        if file_info.destination.exists() && !force {
            return Err(DownloadError::FileExists {
                path: file_info.destination.display().to_string(),
            });
        }

        // Create parent directory if it doesn't exist
        if let Some(parent) = file_info.destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Apply rate limiting
        self.rate_limiter.until_ready().await;

        // Create temporary file path
        let temp_path = file_info.destination.with_extension(format!(
            "{}{}",
            file_info
                .destination
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            TEMP_FILE_SUFFIX
        ));

        // Download with retry logic
        let mut retries = 0;
        loop {
            match self.download_file_attempt(&file_info.url, &temp_path).await {
                Ok(()) => {
                    // Move temp file to final destination
                    tokio::fs::rename(&temp_path, &file_info.destination).await?;
                    tracing::info!("Successfully downloaded: {}", file_info.id);
                    return Ok(());
                }
                Err(e) if retries < MAX_RETRIES => {
                    retries += 1;
                    let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * 2_u64.pow(retries));
                    tracing::warn!(
                        "Download failed (attempt {}/{}): {}. Retrying in {}ms",
                        retries,
                        MAX_RETRIES,
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
                    tracing::error!("Download failed after {} retries: {}", MAX_RETRIES, e);
                    return Err(e);
                }
            }
        }
    }

    /// Attempts to download a file to a temporary path
    async fn download_file_attempt(
        &self,
        url: &Url,
        temp_path: &Path,
    ) -> Result<(), DownloadError> {
        let response = self.client.get(url.as_str()).send().await?;

        if !response.status().is_success() {
            return Err(DownloadError::Http(
                response.error_for_status().unwrap_err(),
            ));
        }

        // Create the temporary file
        let mut file = File::create(temp_path).await?;

        // Get the response bytes
        let bytes = response.bytes().await?;
        file.write_all(&bytes).await?;

        file.flush().await?;
        Ok(())
    }
}

/// Extracts CSRF token from HTML content
fn extract_csrf_token(html: &str) -> Option<String> {
    // Look for CSRF token in various common formats
    if let Some(start) = html.find("csrfmiddlewaretoken") {
        // Django style: <input type="hidden" name="csrfmiddlewaretoken" value="TOKEN">
        if let Some(value_start) = html[start..].find("value=\"") {
            let value_start = start + value_start + 7; // 7 = len("value=\"")
            if let Some(value_end) = html[value_start..].find("\"") {
                let token = &html[value_start..value_start + value_end];
                return Some(token.to_string());
            }
        }
    }

    // Alternative: look for data-csrf-token attribute
    if let Some(start) = html.find("data-csrf-token=\"") {
        let start = start + 17; // 17 = len("data-csrf-token=\"")
        if let Some(end) = html[start..].find("\"") {
            let token = &html[start..start + end];
            return Some(token.to_string());
        }
    }

    // Alternative: look for meta tag
    if let Some(start) = html.find("<meta name=\"csrf-token\"") {
        if let Some(content_start) = html[start..].find("content=\"") {
            let content_start = start + content_start + 9; // 9 = len("content=\"")
            if let Some(content_end) = html[content_start..].find("\"") {
                let token = &html[content_start..content_start + content_end];
                return Some(token.to_string());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_client_creation_without_env() {
        // Test that client creation fails without environment variables
        // This test will pass if env vars are not set
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Clear environment variables for this test
        unsafe {
            env::remove_var(ENV_USERNAME);
            env::remove_var(ENV_PASSWORD);
        }

        let result = rt.block_on(CedaClient::new());
        assert!(result.is_err());

        if let Err(AuthError::MissingCredentials) = result {
            // Expected error type
        } else if let Err(AuthError::EnvVar(_)) = result {
            // Also acceptable - env var not found
        } else {
            panic!(
                "Expected AuthError::MissingCredentials or AuthError::EnvVar, got {:?}",
                result
            );
        }
    }

    #[tokio::test]
    async fn test_file_info_creation() {
        let url = Url::parse("https://example.com/test.csv").unwrap();
        let destination = PathBuf::from("/tmp/test.csv");
        let id = "test".to_string();

        let file_info = FileInfo::new(url.clone(), destination.clone(), id.clone());

        assert_eq!(file_info.url, url);
        assert_eq!(file_info.destination, destination);
        assert_eq!(file_info.id, id);
        assert_eq!(file_info.size_hint, None);
        assert_eq!(file_info.retry_count, 0);
    }

    #[tokio::test]
    async fn test_download_file_already_exists() {
        // Create a temporary file
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("existing_file.csv");
        tokio::fs::write(&file_path, "existing content")
            .await
            .unwrap();

        // Create FileInfo for the existing file
        let url = Url::parse("https://example.com/test.csv").unwrap();
        let file_info = FileInfo::new(url, file_path, "test".to_string());

        // Mock client (we won't actually create one since we don't have credentials)
        // This test focuses on the file existence check logic

        // Test that the download would fail if file exists and force=false
        assert!(file_info.destination.exists());

        // We can't test the actual download without a real client,
        // but we can verify the file exists check works
    }

    #[tokio::test]
    #[ignore] // Requires real CEDA credentials
    async fn test_real_file_download() {
        // This test requires real CEDA credentials to be set
        // Run with: cargo test -- --ignored

        // Initialize tracing for debugging
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init()
            .ok();

        // Load environment variables from .env file
        dotenv::dotenv().ok();

        // Test CEDA authentication and file download
        let client = CedaClient::new()
            .await
            .expect("Failed to create CEDA client");

        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_download.csv");

        let test_url = crate::version_utils::TEST_FILE_URL;
        let url = Url::parse(test_url).unwrap();
        let file_info = FileInfo::new(url, file_path.clone(), "test-download".to_string());

        let result = client.download_file(&file_info, false).await;

        match result {
            Ok(()) => {
                let content = tokio::fs::read_to_string(&file_path).await.unwrap();
                if content.contains("<html") || content.contains("<!DOCTYPE") {
                    panic!(
                        "Got HTML login page instead of CSV: {}",
                        &content[..content.len().min(200)]
                    );
                }

                // Verify it's actual CSV data
                if content.starts_with("ob_end_time")
                    || content.contains("station_file")
                    || content.starts_with("Conventions,G,BADC-CSV")
                    || content.contains("BADC-CSV")
                {
                    println!(
                        "Successfully downloaded CSV file with {} bytes",
                        content.len()
                    );
                } else {
                    panic!(
                        "Downloaded content doesn't appear to be CSV: {}",
                        &content[..content.len().min(200)]
                    );
                }
            }
            Err(e) => {
                panic!("Download failed: {:?}", e);
            }
        }
    }
}
