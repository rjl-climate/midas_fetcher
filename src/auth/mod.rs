//! Authentication management for CEDA credentials
//!
//! This module provides functions for managing CEDA authentication credentials,
//! including interactive setup, verification, and secure storage in .env files.
//!
//! # Examples
//!
//! ```rust,no_run
//! use midas_fetcher::auth::{check_credentials, setup_credentials};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Check if credentials are available
//! if !check_credentials() {
//!     println!("Setting up credentials...");
//!     setup_credentials().await?;
//! }
//! # Ok(())
//! # }
//! ```

pub mod credentials;

// Re-export main public API
pub use credentials::{
    AuthStatus, check_credentials, ensure_authenticated, get_auth_status, prompt_credentials,
    save_credentials, setup_credentials, show_auth_status, verify_credentials,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_structure() {
        // Ensure public API is accessible
        let _ = check_credentials();
        let _ = get_auth_status();
    }
}
