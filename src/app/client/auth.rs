//! CEDA authentication logic
//!
//! This module handles the complete CEDA authentication flow including
//! CSRF token extraction, form submission, and authentication verification.

use reqwest::Client;
use scraper::{Html, Selector};

use crate::constants::{auth, ceda};
use crate::errors::{AuthError, AuthResult};

/// Handles CEDA authentication operations
pub struct AuthHandler;

impl AuthHandler {
    /// Performs CEDA authentication using the proven pattern
    ///
    /// This follows the working authentication pattern with CSRF token extraction.
    /// It fetches the login page, extracts the CSRF token, and submits the form.
    ///
    /// # Arguments
    ///
    /// * `client` - The HTTP client to use for authentication
    /// * `username` - CEDA username
    /// * `password` - CEDA password
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if:
    /// - Login page cannot be fetched
    /// - CSRF token cannot be extracted
    /// - Form submission fails
    /// - Login credentials are invalid
    /// - Authentication verification fails
    pub async fn authenticate(client: &Client, username: &str, password: &str) -> AuthResult<()> {
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
        let csrf_token = Self::extract_csrf_token(&login_page_html)?;
        tracing::info!(
            "Found CSRF token: {}...",
            &csrf_token[..std::cmp::min(8, csrf_token.len())]
        );

        // Step 3: Determine form action URL
        let submit_url = Self::extract_form_action(&login_page_html)?;
        tracing::info!("Form action URL: {}", submit_url);

        // Step 4: Submit login form
        let login_response = client
            .post(&submit_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Referer", auth::CEDA_LOGIN_URL)
            .form(&[
                ("username", username),
                ("password", password),
                ("csrfmiddlewaretoken", &csrf_token),
            ])
            .send()
            .await
            .map_err(AuthError::Http)?;

        // Step 5: Check if login was successful
        Self::verify_login_response(login_response, username).await?;

        // Step 6: Test authentication by trying to access a protected resource
        Self::verify_authentication(client).await?;

        Ok(())
    }

    /// Extracts CSRF token from login page HTML
    fn extract_csrf_token(html: &str) -> AuthResult<String> {
        let document = Html::parse_document(html);
        let csrf_selector =
            Selector::parse(auth::CSRF_TOKEN_SELECTOR).map_err(|_| AuthError::CsrfTokenNotFound)?;

        document
            .select(&csrf_selector)
            .next()
            .and_then(|element| element.value().attr("value"))
            .map(|token| token.to_string())
            .ok_or(AuthError::CsrfTokenNotFound)
    }

    /// Extracts form action URL from login page HTML
    fn extract_form_action(html: &str) -> AuthResult<String> {
        let document = Html::parse_document(html);
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

        Ok(submit_url)
    }

    /// Verifies the login response for success indicators
    async fn verify_login_response(
        login_response: reqwest::Response,
        username: &str,
    ) -> AuthResult<()> {
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

        Ok(())
    }

    /// Verifies authentication by accessing a protected resource
    async fn verify_authentication(client: &Client) -> AuthResult<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csrf_token_extraction() {
        // Test CSRF token extraction from sample HTML
        let sample_html = r#"
            <html>
                <form>
                    <input type="hidden" name="csrfmiddlewaretoken" value="test-token-123">
                    <input type="text" name="username">
                </form>
            </html>
        "#;

        let result = AuthHandler::extract_csrf_token(sample_html);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test-token-123");
    }

    #[test]
    fn test_csrf_token_extraction_missing() {
        // Test CSRF token extraction failure when token is missing
        let sample_html = r#"
            <html>
                <form>
                    <input type="text" name="username">
                </form>
            </html>
        "#;

        let result = AuthHandler::extract_csrf_token(sample_html);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::CsrfTokenNotFound));
    }

    #[test]
    fn test_form_action_extraction() {
        // Test form action URL extraction from sample HTML
        let sample_html = r#"
            <html>
                <form action="/custom/signin/">
                    <input type="text" name="username">
                </form>
            </html>
        "#;

        let result = AuthHandler::extract_form_action(sample_html);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("/custom/signin/"));
    }

    #[test]
    fn test_form_action_extraction_default() {
        // Test form action URL extraction with default when action is missing
        let sample_html = r#"
            <html>
                <form>
                    <input type="text" name="username">
                </form>
            </html>
        "#;

        let result = AuthHandler::extract_form_action(sample_html);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("/account/signin/"));
    }

    #[test]
    fn test_form_action_absolute_url() {
        // Test form action URL extraction with absolute URL
        let sample_html = r#"
            <html>
                <form action="https://example.com/signin">
                    <input type="text" name="username">
                </form>
            </html>
        "#;

        let result = AuthHandler::extract_form_action(sample_html);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "https://example.com/signin");
    }
}
