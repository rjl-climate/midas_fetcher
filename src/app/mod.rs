//! Core application logic for MIDAS Fetcher
//!
//! This module contains the main application components including the HTTP client,
//! data models, caching system, and orchestration logic.
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::app::CedaClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create authenticated CEDA client
//! let client = CedaClient::new().await?;
//!
//! // The client can now be used for authenticated downloads
//! # Ok(())
//! # }
//! ```

pub mod client;

// Re-export main public API
pub use client::{CedaClient, ClientConfig};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_structure() {
        // Ensure public API is accessible
        let config = ClientConfig::default();
        assert!(config.tcp_nodelay);
    }
}
