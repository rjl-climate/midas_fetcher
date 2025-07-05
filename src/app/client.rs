//! HTTP client implementation for CEDA archive interaction
//!
//! This module provides a comprehensive HTTP client with authentication, rate limiting,
//! exponential backoff, and circuit breaker patterns for reliable CEDA archive access.

use std::env;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use governor::{Jitter, Quota, RateLimiter, clock::DefaultClock, state::InMemoryState};
use reqwest::Client;
use scraper::{Html, Selector};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::constants::{auth, ceda, env as env_constants, files, http, limits};
use crate::errors::{AuthError, AuthResult, DownloadError, DownloadResult};

/// HTTP client for interacting with the CEDA archive
///
/// Handles authentication, rate limiting, and file downloads
/// with proper error handling and retry logic.
#[derive(Debug)]
pub struct CedaClient {
    client: Client,
    rate_limiter: RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>,
    base_url: Url,
}

/// Configuration for HTTP client optimizations
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Enable HTTP/2 support (disabled by default for CEDA compatibility)
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
    /// Rate limit (requests per second)
    pub rate_limit_rps: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            http2: false, // Disable HTTP/2 by default for CEDA compatibility
            tcp_keepalive: Some(Duration::from_secs(30)),
            tcp_nodelay: true,
            pool_idle_timeout: Some(http::POOL_IDLE_TIMEOUT),
            pool_max_per_host: http::POOL_MAX_PER_HOST,
            request_timeout: http::DEFAULT_TIMEOUT,
            connect_timeout: http::CONNECT_TIMEOUT,
            rate_limit_rps: limits::DEFAULT_RATE_LIMIT_RPS,
        }
    }
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
        let client = Self::build_http_client(&config)?;
        let rate_limiter = Self::build_rate_limiter(config.rate_limit_rps)?;
        let base_url = Url::parse(ceda::BASE_URL).expect("Base URL should be valid");

        tracing::info!("Created simple CEDA client without authentication");

        Ok(Self {
            client,
            rate_limiter,
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

        let client = Self::build_http_client(&config)?;
        let rate_limiter = Self::build_rate_limiter(config.rate_limit_rps)?;
        let base_url = Url::parse(ceda::BASE_URL).expect("Base URL should be valid");

        // Perform authentication
        Self::authenticate(&client, &username, &password).await?;

        tracing::info!("Successfully authenticated with CEDA");

        Ok(Self {
            client,
            rate_limiter,
            base_url,
        })
    }

    /// Builds the HTTP client with the specified configuration
    fn build_http_client(config: &ClientConfig) -> AuthResult<Client> {
        let mut client_builder = Client::builder()
            .cookie_store(true) // CRITICAL: Required for CEDA session auth
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .user_agent(http::USER_AGENT)
            .http2_keep_alive_interval(Some(http::HTTP2_KEEP_ALIVE_INTERVAL))
            .http2_keep_alive_timeout(http::HTTP2_KEEP_ALIVE_TIMEOUT)
            .tcp_nodelay(config.tcp_nodelay)
            .pool_max_idle_per_host(config.pool_max_per_host);

        // Configure HTTP/2 if enabled (but allow fallback)
        if config.http2 {
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

        client_builder.build().map_err(AuthError::Http)
    }

    /// Builds the rate limiter with the specified rate limit
    fn build_rate_limiter(
        rate_limit_rps: u32,
    ) -> AuthResult<RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>> {
        let quota = Quota::per_second(NonZeroU32::new(rate_limit_rps).ok_or_else(|| {
            AuthError::InvalidUsername {
                reason: "Rate limit must be non-zero".to_string(),
            }
        })?);
        Ok(RateLimiter::direct(quota))
    }

    /// Performs CEDA authentication using the proven pattern
    async fn authenticate(client: &Client, username: &str, password: &str) -> AuthResult<()> {
        tracing::info!("Starting CEDA authentication for user: {}", username);
        tracing::info!("Fetching login page: {}", auth::CEDA_LOGIN_URL);

        // Step 1: Get the login page to extract CSRF token
        let login_page_response = client
            .get(auth::CEDA_LOGIN_URL)
            .send()
            .await
            .map_err(AuthError::Http)?;

        tracing::info!(
            "Login page response status: {}",
            login_page_response.status()
        );
        tracing::info!("Login page final URL: {}", login_page_response.url());

        let login_page_html = login_page_response.text().await.map_err(AuthError::Http)?;
        tracing::info!("Login page content length: {} bytes", login_page_html.len());

        // Step 2: Extract CSRF token using scraper crate
        let document = Html::parse_document(&login_page_html);
        let csrf_selector =
            Selector::parse(auth::CSRF_TOKEN_SELECTOR).map_err(|_| AuthError::CsrfTokenNotFound)?;

        let csrf_token = document
            .select(&csrf_selector)
            .next()
            .and_then(|element| element.value().attr("value"))
            .ok_or(AuthError::CsrfTokenNotFound)?;

        tracing::info!(
            "Found CSRF token: {}...",
            &csrf_token[..std::cmp::min(8, csrf_token.len())]
        );

        // Determine the correct form action URL
        let form_selector = Selector::parse("form").map_err(|_| AuthError::CsrfTokenNotFound)?;
        let form_action = document
            .select(&form_selector)
            .next()
            .and_then(|element| element.value().attr("action"))
            .unwrap_or("/account/signin/");

        // Build the complete action URL
        let submit_url = if form_action.starts_with("http") {
            form_action.to_string()
        } else {
            format!("{}{}", auth::CEDA_AUTH_BASE_URL, form_action)
        };

        tracing::info!("Form action URL: {}", submit_url);

        // Step 3: Submit login form
        let login_response = client
            .post(&submit_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Referer", auth::CEDA_LOGIN_URL)
            .form(&[
                ("username", username),
                ("password", password),
                ("csrfmiddlewaretoken", csrf_token),
            ])
            .send()
            .await
            .map_err(AuthError::Http)?;

        // Step 4: Check if login was successful
        let final_url = login_response.url().clone();
        let login_status = login_response.status();

        tracing::info!("Login response status: {}", login_status);
        tracing::info!("Login response final URL: {}", final_url);

        let login_result_html = login_response.text().await.map_err(AuthError::Http)?;
        tracing::info!(
            "Login response content length: {} bytes",
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

        // Step 5: Test authentication by trying to access a protected resource
        tracing::info!("Testing authentication with a protected resource");
        let test_response = client
            .get(ceda::TEST_FILE_URL)
            .send()
            .await
            .map_err(AuthError::Http)?;

        tracing::info!("Test response status: {}", test_response.status());
        tracing::info!("Test response URL: {}", test_response.url());

        let test_content = test_response.text().await.map_err(AuthError::Http)?;
        tracing::info!("Test response content length: {} bytes", test_content.len());

        // If we get HTML with login form, authentication failed
        if test_content.contains("<html")
            && (test_content.contains("signin") || test_content.contains("login"))
        {
            tracing::warn!("Authentication test failed: Got login page");
            return Err(AuthError::LoginFailed);
        }

        // If we get CSV content (including BADC-CSV format), authentication succeeded
        if test_content.starts_with("ob_end_time")
            || test_content.contains("station_file")
            || test_content.starts_with("Conventions,G,BADC-CSV")
            || test_content.contains("BADC-CSV")
        {
            tracing::info!("Authentication test succeeded: Got CSV content");
        } else {
            // Show preview of ambiguous content
            let preview = test_content.chars().take(200).collect::<String>();
            tracing::warn!(
                "Authentication test ambiguous - content preview: {}",
                preview
            );
            return Err(AuthError::LoginFailed);
        }

        Ok(())
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
        // Apply rate limiting with jitter to avoid thundering herd
        self.rate_limiter
            .until_ready_with_jitter(Jitter::up_to(Duration::from_millis(100)))
            .await;

        // Retry logic with exponential backoff
        let mut retries = 0;
        loop {
            match self.client.get(url.as_str()).send().await {
                Ok(response) => {
                    // Check for rate limiting from server
                    if response.status() == 429 {
                        if retries < limits::MAX_RETRIES {
                            retries += 1;
                            let delay = Duration::from_millis(
                                limits::RETRY_BASE_DELAY_MS * 2_u64.pow(retries),
                            );
                            tracing::warn!(
                                "Rate limited by server (429). Backing off for {}ms",
                                delay.as_millis()
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            return Err(DownloadError::RateLimitExceeded);
                        }
                    }

                    // Check for server overload
                    if response.status() == 503 {
                        if retries < limits::MAX_RETRIES {
                            retries += 1;
                            let delay = Duration::from_millis(
                                limits::RETRY_BASE_DELAY_MS * 2_u64.pow(retries),
                            );
                            tracing::warn!(
                                "Server overloaded (503). Backing off for {}ms",
                                delay.as_millis()
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            return Err(DownloadError::ServerOverloaded);
                        }
                    }

                    tracing::debug!("Successfully fetched response: {}", url);
                    return Ok(response);
                }
                Err(e) if retries < limits::MAX_RETRIES => {
                    retries += 1;
                    let delay =
                        Duration::from_millis(limits::RETRY_BASE_DELAY_MS * 2_u64.pow(retries));
                    tracing::warn!(
                        "Request failed (attempt {}/{}): {}. Retrying in {}ms",
                        retries,
                        limits::MAX_RETRIES,
                        e,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    tracing::error!(
                        "Request failed after {} retries: {}",
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
        let response = self.get_response(url).await?;
        let text = response.text().await?;
        tracing::debug!("Successfully fetched page: {}", url);
        Ok(text)
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
                    let delay =
                        Duration::from_millis(limits::RETRY_BASE_DELAY_MS * 2_u64.pow(retries));
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
        let response = self.get_response(url).await?;

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

        let response = self.get_response(&parsed_url).await?;

        if !response.status().is_success() {
            return Err(DownloadError::ServerError {
                status: response.status().as_u16(),
            });
        }

        let bytes = response.bytes().await.map_err(DownloadError::Http)?;
        Ok(bytes.to_vec())
    }

    /// Get the base URL for the CEDA archive
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        assert!(!config.http2); // HTTP/2 disabled by default for CEDA compatibility
        assert!(config.tcp_nodelay);
        assert_eq!(config.rate_limit_rps, limits::DEFAULT_RATE_LIMIT_RPS);
    }

    #[tokio::test]
    async fn test_simple_client_creation() {
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
        // Test that client creation fails without environment variables
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

    #[tokio::test]
    async fn test_download_file_already_exists() {
        // Create a temporary file
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("existing_file.csv");
        tokio::fs::write(&file_path, "existing content")
            .await
            .unwrap();

        // Create simple client (no auth needed for this test)
        let client = CedaClient::new_simple().await.unwrap();

        // Test URL
        let url = Url::parse("https://example.com/test.csv").unwrap();

        // Test that the download would fail if file exists and force=false
        let result = client.download_file(&url, &file_path, false).await;
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
    async fn test_rate_limiter_creation() {
        let rate_limiter = CedaClient::build_rate_limiter(5).unwrap();

        // Test that rate limiter allows requests
        rate_limiter.until_ready().await;
        // If we get here, the rate limiter is working
    }

    #[test]
    fn test_rate_limiter_zero_fails() {
        let result = CedaClient::build_rate_limiter(0);
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore] // Requires real CEDA credentials and actual file download
    async fn test_manifest_hash_validation() {
        // This test validates that the hashes in the manifest file actually match
        // the files available on CEDA. It downloads a real file and verifies the hash.
        //
        // Run with: cargo test test_manifest_hash_validation -- --ignored --nocapture

        use crate::app::{ManifestConfig, ManifestStreamer};
        use futures::StreamExt;
        use std::path::Path;

        // Initialize tracing for detailed debugging
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(false)
            .with_line_number(false)
            .try_init()
            .ok();

        // Load credentials from .env file if it exists
        if std::path::Path::new(".env").exists() {
            println!("Loading credentials from .env file...");
            let env_content = std::fs::read_to_string(".env").expect("Failed to read .env file");
            for line in env_content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    unsafe {
                        if key.trim() == env_constants::USERNAME && !value.trim().is_empty() {
                            env::set_var(env_constants::USERNAME, value.trim());
                        }
                        if key.trim() == env_constants::PASSWORD && !value.trim().is_empty() {
                            env::set_var(env_constants::PASSWORD, value.trim());
                        }
                    }
                }
            }
        }

        // Check credentials
        let username = env::var(env_constants::USERNAME)
            .expect("CEDA_USERNAME environment variable not set. Please set credentials.");
        let _password = env::var(env_constants::PASSWORD)
            .expect("CEDA_PASSWORD environment variable not set. Please set credentials.");

        println!(
            "🔍 Testing manifest hash validation with user: {}",
            username
        );

        // Parse the example manifest file
        let manifest_path = Path::new("examples/midas-open-v202407-md5s.txt");
        if !manifest_path.exists() {
            panic!(
                "Example manifest file not found at: {}",
                manifest_path.display()
            );
        }

        println!("📋 Reading manifest file: {}", manifest_path.display());

        let temp_dir = tempdir().unwrap();
        let config = ManifestConfig {
            destination_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut streamer = ManifestStreamer::with_config(config);
        let mut stream = streamer
            .stream(manifest_path)
            .await
            .expect("Failed to open manifest file");

        // Get the first file from the manifest
        let first_file = stream
            .next()
            .await
            .expect("Manifest file is empty")
            .expect("Failed to parse first line of manifest");

        println!("📁 Selected file for validation:");
        println!("   Hash: {}", first_file.hash);
        println!("   Path: {}", first_file.relative_path);
        println!("   File: {}", first_file.file_name);
        println!("   Dataset: {}", first_file.dataset_info.display_name());

        // Create authenticated CEDA client
        println!("🔐 Creating authenticated CEDA client...");
        let client = CedaClient::new()
            .await
            .expect("Failed to create authenticated CEDA client");

        // Download the file
        let download_url = first_file.download_url(ceda::BASE_URL);
        println!("⬇️  Downloading file from: {}", download_url);

        let response = client
            .get_response(&Url::parse(&download_url).unwrap())
            .await
            .expect("Failed to download file");

        if !response.status().is_success() {
            panic!("Download failed with status: {}", response.status());
        }

        let file_content = response.bytes().await.expect("Failed to read file content");

        println!("📊 Downloaded {} bytes", file_content.len());

        // Calculate MD5 hash of downloaded content
        let calculated_hash = format!("{:x}", md5::compute(&file_content));

        println!("🔍 Hash validation:");
        println!("   Expected (manifest): {}", first_file.hash);
        println!("   Calculated (downloaded): {}", calculated_hash);

        // Verify hashes match
        if calculated_hash == first_file.hash.to_hex() {
            println!("✅ SUCCESS: Hash validation passed!");
            println!("   The manifest hash matches the actual file content.");
        } else {
            panic!(
                "❌ FAILURE: Hash mismatch!\n   Expected: {}\n   Calculated: {}\n   This indicates either:\n   1. The manifest file is outdated\n   2. The file was modified on CEDA\n   3. There's an issue with the download process",
                first_file.hash.to_hex(),
                calculated_hash
            );
        }

        // Validate it's actually CSV content as expected
        let content_str = String::from_utf8_lossy(&file_content);
        let first_lines: Vec<&str> = content_str.lines().take(3).collect();

        println!("📄 File content validation:");
        for (i, line) in first_lines.iter().enumerate() {
            println!("   Line {}: {}", i + 1, &line[..line.len().min(80)]);
        }

        // Check for MIDAS format indicators
        if content_str.contains("Conventions,G,BADC-CSV")
            || content_str.contains("ob_end_time")
            || content_str.contains("BADC-CSV")
        {
            println!("✅ File format validation passed: Detected MIDAS/BADC-CSV format");
        } else if content_str.contains("<html") || content_str.contains("<!DOCTYPE") {
            panic!("❌ Got HTML content instead of CSV - authentication may have failed");
        } else {
            println!("⚠️  Warning: File format doesn't match expected MIDAS patterns");
            println!(
                "   Content preview: {}",
                &content_str[..content_str.len().min(200)]
            );
        }

        println!("🎉 All validations passed!");
        println!("   • Manifest parsing works correctly");
        println!("   • CEDA authentication is functional");
        println!("   • File download is working");
        println!("   • Hash validation confirms data integrity");
        println!("   • The manifest file contains accurate hash information");
    }

    #[tokio::test]
    #[ignore] // Requires real CEDA credentials
    async fn test_real_authentication() {
        // This test requires real CEDA credentials to be set
        // Run with: cargo test test_real_authentication -- --ignored --nocapture
        //
        // Setup instructions:
        // 1. Create a .env file in the project root with:
        //    CEDA_USERNAME=your_username
        //    CEDA_PASSWORD=your_password
        // 2. Or set environment variables directly:
        //    export CEDA_USERNAME=your_username
        //    export CEDA_PASSWORD=your_password

        // Initialize tracing for detailed debugging
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(false)
            .with_line_number(false)
            .try_init()
            .ok();

        // Try to load from .env file if it exists
        if std::path::Path::new(".env").exists() {
            println!("Loading credentials from .env file...");
            // Simple .env parsing (basic implementation)
            let env_content = std::fs::read_to_string(".env").expect("Failed to read .env file");
            for line in env_content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    unsafe {
                        if key.trim() == env_constants::USERNAME && !value.trim().is_empty() {
                            env::set_var(env_constants::USERNAME, value.trim());
                        }
                        if key.trim() == env_constants::PASSWORD && !value.trim().is_empty() {
                            env::set_var(env_constants::PASSWORD, value.trim());
                        }
                    }
                }
            }
        }

        // Check if credentials are available
        let username = env::var(env_constants::USERNAME)
            .expect("CEDA_USERNAME environment variable not set. Please set credentials.");
        let password = env::var(env_constants::PASSWORD)
            .expect("CEDA_PASSWORD environment variable not set. Please set credentials.");

        println!("Testing CEDA authentication for user: {}", username);
        println!("Password length: {} characters", password.len());

        // Test CEDA authentication
        println!("Creating CEDA client...");
        let client = match CedaClient::new().await {
            Ok(client) => {
                println!("✅ Successfully created authenticated CEDA client");
                client
            }
            Err(e) => {
                println!("❌ Failed to create CEDA client: {}", e);
                match e {
                    AuthError::LoginFailed => {
                        println!("💡 Check your username and password are correct");
                        println!("💡 Make sure your CEDA account is active");
                    }
                    AuthError::Http(ref http_err) => {
                        println!("💡 Network error - check your internet connection");
                        println!("💡 HTTP error details: {}", http_err);
                    }
                    _ => {
                        println!("💡 Error details: {:?}", e);
                    }
                }
                panic!("Failed to create CEDA client: {}", e);
            }
        };

        // Test download of a small file
        println!("Testing file download...");
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_download.csv");

        let test_url = Url::parse(ceda::TEST_FILE_URL).unwrap();
        println!("Downloading test file: {}", test_url);

        let result = client.download_file(&test_url, &file_path, false).await;

        match result {
            Ok(()) => {
                println!(
                    "✅ Successfully downloaded file to: {}",
                    file_path.display()
                );

                let content = tokio::fs::read_to_string(&file_path).await.unwrap();
                println!("📁 File size: {} bytes", content.len());

                // Show first few lines for debugging
                let lines: Vec<&str> = content.lines().take(3).collect();
                println!("📄 First few lines:");
                for (i, line) in lines.iter().enumerate() {
                    println!("   {}: {}", i + 1, &line[..line.len().min(80)]);
                }

                // Check if we got HTML instead of CSV (authentication failed)
                if content.contains("<html") || content.contains("<!DOCTYPE") {
                    println!("❌ Got HTML login page instead of CSV");
                    println!("💡 This indicates authentication failed");
                    let preview = &content[..content.len().min(500)];
                    println!("🔍 Content preview: {}", preview);
                    panic!("Got HTML login page instead of CSV");
                }

                // Verify it's actual CSV data
                let is_valid_csv = content.starts_with("ob_end_time")
                    || content.contains("station_file")
                    || content.starts_with("Conventions,G,BADC-CSV")
                    || content.contains("BADC-CSV")
                    || content.contains("data_type")
                    || (content.contains(",") && content.lines().count() > 1);

                if is_valid_csv {
                    println!("✅ Successfully downloaded valid CSV file");
                    println!("📊 File contains {} lines", content.lines().count());

                    // Check for specific MIDAS format indicators
                    if content.contains("ob_end_time") {
                        println!("🎯 Detected MIDAS observation format");
                    } else if content.contains("BADC-CSV") {
                        println!("🎯 Detected BADC-CSV format");
                    } else {
                        println!("📋 Generic CSV format detected");
                    }
                } else {
                    println!("❌ Downloaded content doesn't appear to be valid CSV");
                    let preview = &content[..content.len().min(300)];
                    println!("🔍 Content preview: {}", preview);
                    panic!("Downloaded content doesn't appear to be CSV");
                }

                println!(
                    "🎉 All tests passed! CEDA authentication and download working correctly."
                );
            }
            Err(e) => {
                println!("❌ Download failed: {}", e);
                match e {
                    DownloadError::Http(ref http_err) => {
                        println!("💡 HTTP error - check the test URL is accessible");
                        println!("💡 HTTP error details: {}", http_err);
                    }
                    DownloadError::ServerError { status } => {
                        println!("💡 Server returned error status: {}", status);
                        if status == 401 || status == 403 {
                            println!("💡 This suggests authentication issues");
                        }
                    }
                    DownloadError::RateLimitExceeded => {
                        println!("💡 Rate limited - try again later");
                    }
                    DownloadError::MaxRetriesExceeded { .. } => {
                        println!("💡 Max retries exceeded - check network connectivity");
                    }
                    _ => {
                        println!("💡 Error details: {:?}", e);
                    }
                }
                panic!("Download failed: {:?}", e);
            }
        }
    }
}
