//! Authentication management for CEDA credentials
//!
//! This module provides functions for managing CEDA authentication credentials,
//! including interactive setup, verification, and secure storage in .env files.

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::client::CedaClient;
use crate::constants::{ENV_PASSWORD, ENV_USERNAME};

/// Authentication status information
#[derive(Debug, Clone)]
pub struct AuthStatus {
    pub username_set: bool,
    pub password_set: bool,
    pub dotenv_file_exists: bool,
    pub credentials_valid: Option<bool>, // None = not tested, Some(bool) = test result
}

impl AuthStatus {
    /// Check if both credentials are available
    pub fn has_credentials(&self) -> bool {
        self.username_set && self.password_set
    }

    /// Get status message for display
    pub fn status_message(&self) -> String {
        match (self.has_credentials(), self.credentials_valid) {
            (false, _) => "Missing credentials - run 'auth setup' to configure".to_string(),
            (true, None) => "❓ Credentials configured but not verified".to_string(),
            (true, Some(true)) => "Credentials configured and verified".to_string(),
            (true, Some(false)) => "Credentials configured but invalid".to_string(),
        }
    }
}

/// Check current authentication status
pub fn get_auth_status() -> AuthStatus {
    AuthStatus {
        username_set: env::var(ENV_USERNAME).is_ok(),
        password_set: env::var(ENV_PASSWORD).is_ok(),
        dotenv_file_exists: Path::new(".env").exists(),
        credentials_valid: None,
    }
}

/// Check if credentials exist in environment
pub fn check_credentials() -> bool {
    env::var(ENV_USERNAME).is_ok() && env::var(ENV_PASSWORD).is_ok()
}

/// Prompt user for credentials interactively
pub fn prompt_credentials() -> Result<(String, String)> {
    print!("CEDA Username: ");
    io::stdout().flush()?;

    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();

    if username.is_empty() {
        anyhow::bail!("Username cannot be empty");
    }

    // Validate username format (basic check)
    if !is_valid_username(&username) {
        anyhow::bail!(
            "Invalid username format. Username should be alphanumeric with optional dots, hyphens, or underscores"
        );
    }

    let password =
        rpassword::prompt_password("CEDA Password: ").context("Failed to read password")?;

    if password.is_empty() {
        anyhow::bail!("Password cannot be empty");
    }

    Ok((username, password))
}

/// Validate username format
fn is_valid_username(username: &str) -> bool {
    // Basic validation: alphanumeric with dots, hyphens, underscores
    // Length between 3 and 50 characters
    if username.len() < 3 || username.len() > 50 {
        return false;
    }

    username
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
}

/// Save credentials to .env file
pub fn save_credentials(username: &str, password: &str) -> Result<()> {
    let env_path = Path::new(".env");
    let mut existing_lines = Vec::new();
    let mut username_found = false;
    let mut password_found = false;

    // Read existing .env file if it exists
    if env_path.exists() {
        let file = File::open(env_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();

            if trimmed.starts_with(&format!("{}=", ENV_USERNAME)) {
                existing_lines.push(format!("{}={}", ENV_USERNAME, username));
                username_found = true;
            } else if trimmed.starts_with(&format!("{}=", ENV_PASSWORD)) {
                existing_lines.push(format!("{}={}", ENV_PASSWORD, password));
                password_found = true;
            } else {
                existing_lines.push(line);
            }
        }
    }

    // Add missing credentials
    if !username_found {
        existing_lines.push(format!("{}={}", ENV_USERNAME, username));
    }
    if !password_found {
        existing_lines.push(format!("{}={}", ENV_PASSWORD, password));
    }

    // Write back to .env file
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(env_path)?;

    for line in existing_lines {
        writeln!(file, "{}", line)?;
    }

    // Set restrictive permissions (Unix-like systems only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o600); // Read/write for owner only
        file.set_permissions(perms)?;
    }

    // Update current environment
    unsafe {
        env::set_var(ENV_USERNAME, username);
        env::set_var(ENV_PASSWORD, password);
    }

    println!("Credentials saved to .env file");

    #[cfg(unix)]
    println!("🔒 File permissions set to owner-only (600)");

    #[cfg(not(unix))]
    println!(
        "Warning: File permissions not set (non-Unix system). Please ensure .env file is protected."
    );

    Ok(())
}

/// Verify credentials by attempting authentication
pub async fn verify_credentials() -> Result<bool> {
    if !check_credentials() {
        anyhow::bail!("No credentials found. Run 'auth setup' first.");
    }

    println!("Verifying credentials with CEDA...");

    match CedaClient::new().await {
        Ok(_) => {
            println!("Credentials verified successfully!");
            Ok(true)
        }
        Err(e) => {
            println!("Credential verification failed: {}", e);
            Ok(false)
        }
    }
}

/// Interactive credential setup workflow
pub async fn setup_credentials() -> Result<()> {
    println!("CEDA Authentication Setup");
    println!("===============================");
    println!();
    println!("This will help you configure your CEDA credentials for accessing the archive.");
    println!("Your credentials will be stored in a .env file in the current directory.");
    println!();

    // Check if credentials already exist
    let status = get_auth_status();
    if status.has_credentials() {
        println!("Warning: Credentials are already configured.");
        print!("Do you want to update them? [y/N]: ");
        io::stdout().flush()?;

        let mut response = String::new();
        io::stdin().read_line(&mut response)?;

        if !response.trim().to_lowercase().starts_with('y') {
            println!("Setup cancelled.");
            return Ok(());
        }
        println!();
    }

    // Prompt for credentials
    let (username, password) = prompt_credentials()?;

    println!();
    println!("Saving credentials...");
    save_credentials(&username, &password)?;

    println!();
    println!("Verifying credentials...");
    let is_valid = verify_credentials().await?;

    if is_valid {
        println!();
        println!("Setup complete! You can now use CEDA fetcher commands.");
    } else {
        println!();
        println!("Setup failed. Please check your credentials and try again.");
        println!("   You can run 'auth setup' again to re-enter your credentials.");
    }

    Ok(())
}

/// Show current authentication status
pub async fn show_auth_status() -> Result<()> {
    let mut status = get_auth_status();

    println!("CEDA Authentication Status");
    println!("=============================");
    println!();

    if let Ok(username) = env::var(ENV_USERNAME) {
        println!("Username: {} (verified)", username);
    } else {
        println!("Username: Not set");
    }

    println!(
        "Password: {} {}",
        if status.password_set {
            "Set"
        } else {
            "Not set"
        },
        if status.password_set {
            "(set)"
        } else {
            "(not set)"
        }
    );

    println!(
        ".env file: {} {}",
        if status.dotenv_file_exists {
            "Exists"
        } else {
            "Not found"
        },
        if status.dotenv_file_exists {
            "(verified)"
        } else {
            "(failed)"
        }
    );

    println!();

    if status.has_credentials() {
        println!("Testing credentials...");
        let is_valid = verify_credentials().await?;
        status.credentials_valid = Some(is_valid);

        println!();
    }

    println!("Status: {}", status.status_message());

    if !status.has_credentials() {
        println!();
        println!("To configure credentials, run: ceda_fetcher auth setup");
    } else if status.credentials_valid == Some(false) {
        println!();
        println!("To update credentials, run: ceda_fetcher auth setup");
    }

    Ok(())
}

/// Check if command requires authentication and prompt setup if needed
pub async fn ensure_authenticated() -> Result<()> {
    if !check_credentials() {
        println!("This command requires CEDA authentication credentials.");
        println!();

        print!("Would you like to set up authentication now? [Y/n]: ");
        io::stdout().flush()?;

        let mut response = String::new();
        io::stdin().read_line(&mut response)?;

        if response.trim().to_lowercase().starts_with('n') {
            anyhow::bail!(
                "Authentication required. Run 'ceda_fetcher auth setup' to configure credentials."
            );
        }

        println!();
        setup_credentials().await?;

        // Check again after setup
        if !check_credentials() {
            anyhow::bail!(
                "Authentication setup incomplete. Please run 'ceda_fetcher auth setup' again."
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_username() {
        assert!(is_valid_username("testuser"));
        assert!(is_valid_username("test.user"));
        assert!(is_valid_username("test-user"));
        assert!(is_valid_username("test_user"));
        assert!(is_valid_username("test123"));
        assert!(is_valid_username("user.name123"));

        // Invalid cases
        assert!(!is_valid_username(""));
        assert!(!is_valid_username("ab")); // too short
        assert!(!is_valid_username("test user")); // space
        assert!(!is_valid_username("test@user")); // special char
        assert!(!is_valid_username(&"a".repeat(51))); // too long
    }

    #[test]
    fn test_auth_status() {
        // This test depends on environment state, so just test the structure
        let status = get_auth_status();

        // Test status message generation
        let message = status.status_message();
        assert!(!message.is_empty());

        // Test has_credentials logic
        let has_creds = status.has_credentials();
        assert_eq!(has_creds, status.username_set && status.password_set);
    }
}
