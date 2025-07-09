//! HTTP client configuration and building logic
//!
//! This module handles the configuration and construction of HTTP clients
//! optimized for CEDA archive interaction.

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::constants::{http, limits};
use crate::errors::{AuthError, AuthResult};

/// Configuration for HTTP client optimizations
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl ClientConfig {
    /// Builds the HTTP client with the specified configuration
    pub fn build_http_client(&self) -> AuthResult<Client> {
        let mut client_builder = Client::builder()
            .cookie_store(true) // CRITICAL: Required for CEDA session auth
            .timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout)
            .user_agent(http::USER_AGENT)
            .http2_keep_alive_interval(Some(http::HTTP2_KEEP_ALIVE_INTERVAL))
            .http2_keep_alive_timeout(http::HTTP2_KEEP_ALIVE_TIMEOUT)
            .tcp_nodelay(self.tcp_nodelay)
            .pool_max_idle_per_host(self.pool_max_per_host);

        // Configure HTTP/2 if enabled (but allow fallback)
        if self.http2 {
            client_builder = client_builder.http2_adaptive_window(true);
        }

        // Configure TCP keep-alive if specified
        if let Some(keepalive) = self.tcp_keepalive {
            client_builder = client_builder.tcp_keepalive(keepalive);
        }

        // Configure connection pool idle timeout
        if let Some(idle_timeout) = self.pool_idle_timeout {
            client_builder = client_builder.pool_idle_timeout(idle_timeout);
        }

        client_builder.build().map_err(AuthError::Http)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_default() {
        // Test that default configuration has expected CEDA-optimized settings
        let config = ClientConfig::default();
        assert!(!config.http2); // HTTP/2 disabled by default for CEDA compatibility
        assert!(config.tcp_nodelay);
        assert_eq!(config.rate_limit_rps, limits::DEFAULT_RATE_LIMIT_RPS);
    }

    #[test]
    fn test_client_config_custom() {
        // Test custom configuration creation and validation
        let config = ClientConfig {
            http2: true,
            rate_limit_rps: 10,
            ..Default::default()
        };

        assert!(config.http2);
        assert_eq!(config.rate_limit_rps, 10);
        assert!(config.tcp_nodelay); // Should inherit default values
    }

    #[test]
    fn test_http_client_creation() {
        // Test that HTTP client can be created with default config
        let config = ClientConfig::default();
        let result = config.build_http_client();
        assert!(result.is_ok());
    }

    #[test]
    fn test_http_client_with_custom_config() {
        // Test HTTP client creation with custom timeouts
        let config = ClientConfig {
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            ..Default::default()
        };

        let result = config.build_http_client();
        assert!(result.is_ok());
    }
}
