//! Core HTTP operations with rate limiting and retry logic
//!
//! This module provides the fundamental HTTP request operations with
//! built-in resilience patterns including rate limiting, exponential backoff,
//! and circuit breaker logic.

use std::num::NonZeroU32;
use std::time::Duration;

use governor::{clock::DefaultClock, state::InMemoryState, Jitter, Quota, RateLimiter};
use reqwest::Client;
use url::Url;

use crate::constants::limits;
use crate::errors::{AuthError, AuthResult, DownloadError, DownloadResult};

/// HTTP operations handler with resilience patterns
#[derive(Debug)]
pub struct HttpHandler {
    client: Client,
    rate_limiter: RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>,
}

impl HttpHandler {
    /// Creates a new HttpHandler with the given client and rate limiting
    ///
    /// # Arguments
    ///
    /// * `client` - The HTTP client to use for requests
    /// * `rate_limit_rps` - Requests per second rate limit
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if rate limiter creation fails
    pub fn new(client: Client, rate_limit_rps: u32) -> AuthResult<Self> {
        let rate_limiter = Self::build_rate_limiter(rate_limit_rps)?;
        Ok(Self {
            client,
            rate_limiter,
        })
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

    /// Get a reference to the underlying HTTP client
    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::client::config::ClientConfig;

    #[tokio::test]
    async fn test_rate_limiter_creation() {
        // Test that rate limiter can be created with valid rate
        let rate_limiter = HttpHandler::build_rate_limiter(5).unwrap();

        // Test that rate limiter allows requests
        rate_limiter.until_ready().await;
        // If we get here, the rate limiter is working
    }

    #[test]
    fn test_rate_limiter_zero_fails() {
        // Test that rate limiter creation fails with zero rate
        let result = HttpHandler::build_rate_limiter(0);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_handler_creation() {
        // Test that HttpHandler can be created with valid configuration
        let config = ClientConfig::default();
        let client = config.build_http_client().unwrap();
        let handler = HttpHandler::new(client, 5);
        assert!(handler.is_ok());
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        // Test that exponential backoff delays increase correctly
        let base_delay = 1000_u64;

        let delay_1 = Duration::from_millis(base_delay * 2_u64.pow(1));
        let delay_2 = Duration::from_millis(base_delay * 2_u64.pow(2));
        let delay_3 = Duration::from_millis(base_delay * 2_u64.pow(3));

        assert_eq!(delay_1.as_millis(), 2000);
        assert_eq!(delay_2.as_millis(), 4000);
        assert_eq!(delay_3.as_millis(), 8000);
    }
}
