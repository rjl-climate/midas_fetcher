//! MIDAS Fetcher Library
//!
//! A Rust library for downloading weather data from the UK Met Office MIDAS Open dataset.
//! Provides efficient, concurrent downloading with proper rate limiting and error handling.

pub mod app;
pub mod auth;
pub mod cli;
pub mod constants;
pub mod errors;

// Re-export commonly used types for convenience
pub use errors::{AppError, Result};

#[cfg(test)]
mod tests {
    use super::*;
    use constants::*;

    #[test]
    fn test_constants_accessible() {
        // Test that our constants are accessible
        assert_eq!(DEFAULT_WORKER_COUNT, 4);
        assert_eq!(ENV_USERNAME, "CEDA_USERNAME");
        assert!(USER_AGENT.contains("MIDAS-Fetcher"));
    }

    #[test]
    fn test_error_types() {
        // Test that our error types work correctly
        let auth_error = errors::AuthError::LoginFailed;
        let app_error = AppError::Auth(auth_error);

        assert_eq!(app_error.category(), "authentication");
        assert!(!app_error.is_recoverable());
    }
}
