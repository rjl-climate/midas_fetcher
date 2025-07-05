//! Credential management implementation for CEDA authentication
//!
//! This module handles secure storage, retrieval, and validation of CEDA credentials.
//! Credentials are stored in .env files with appropriate security permissions.

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use crate::constants::{auth, env as env_constants};
use crate::errors::{AuthError, AuthResult};

/// Authentication status information
#[derive(Debug, Clone)]
pub struct AuthStatus {
    /// Whether username environment variable is set
    pub username_set: bool,
    /// Whether password environment variable is set
    pub password_set: bool,
    /// Whether .env file exists in current directory
    pub dotenv_file_exists: bool,
    /// Whether credentials have been verified (None = not tested)
    pub credentials_valid: Option<bool>,
}

impl AuthStatus {
    /// Check if both credentials are available in environment
    pub fn has_credentials(&self) -> bool {
        self.username_set && self.password_set
    }

    /// Get descriptive status message for display
    pub fn status_message(&self) -> String {
        match (self.has_credentials(), self.credentials_valid) {
            (false, _) => "Missing credentials - run 'auth setup' to configure".to_string(),
            (true, None) => "Credentials configured but not verified".to_string(),
            (true, Some(true)) => "Credentials configured and verified".to_string(),
            (true, Some(false)) => "Credentials configured but invalid".to_string(),
        }
    }
}

/// Check current authentication status
pub fn get_auth_status() -> AuthStatus {
    AuthStatus {
        username_set: env::var(env_constants::USERNAME).is_ok(),
        password_set: env::var(env_constants::PASSWORD).is_ok(),
        dotenv_file_exists: Path::new(".env").exists(),
        credentials_valid: None,
    }
}

/// Check if credentials exist in environment variables
pub fn check_credentials() -> bool {
    env::var(env_constants::USERNAME).is_ok() && env::var(env_constants::PASSWORD).is_ok()
}

/// Prompt user for credentials interactively
pub fn prompt_credentials() -> AuthResult<(String, String)> {
    print!("CEDA Username: ");
    io::stdout().flush().map_err(AuthError::CredentialStorage)?;

    let mut username = String::new();
    io::stdin()
        .read_line(&mut username)
        .map_err(AuthError::CredentialStorage)?;
    let username = username.trim().to_string();

    if username.is_empty() {
        return Err(AuthError::InvalidUsername {
            reason: "Username cannot be empty".to_string(),
        });
    }

    // Validate username format
    if !is_valid_username(&username) {
        return Err(AuthError::InvalidUsername {
            reason: "Username should be alphanumeric with optional dots, hyphens, or underscores"
                .to_string(),
        });
    }

    let password = rpassword::prompt_password("CEDA Password: ")
        .map_err(|e| AuthError::CredentialStorage(io::Error::new(io::ErrorKind::Other, e)))?;

    if password.is_empty() {
        return Err(AuthError::InvalidUsername {
            reason: "Password cannot be empty".to_string(),
        });
    }

    Ok((username, password))
}

/// Validate username format according to CEDA requirements
fn is_valid_username(username: &str) -> bool {
    // Length validation using constants
    if username.len() < auth::MIN_USERNAME_LENGTH || username.len() > auth::MAX_USERNAME_LENGTH {
        return false;
    }

    // Character validation: alphanumeric with dots, hyphens, underscores
    username
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
}

/// Save credentials to .env file with secure permissions
pub fn save_credentials(username: &str, password: &str) -> AuthResult<()> {
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

            if trimmed.starts_with(&format!("{}=", env_constants::USERNAME)) {
                existing_lines.push(format!("{}={}", env_constants::USERNAME, username));
                username_found = true;
            } else if trimmed.starts_with(&format!("{}=", env_constants::PASSWORD)) {
                existing_lines.push(format!("{}={}", env_constants::PASSWORD, password));
                password_found = true;
            } else {
                existing_lines.push(line);
            }
        }
    }

    // Add missing credentials
    if !username_found {
        existing_lines.push(format!("{}={}", env_constants::USERNAME, username));
    }
    if !password_found {
        existing_lines.push(format!("{}={}", env_constants::PASSWORD, password));
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
        perms.set_mode(auth::ENV_FILE_PERMISSIONS);
        file.set_permissions(perms)?;
    }

    // Update current environment (unsafe but necessary for credential management)
    unsafe {
        env::set_var(env_constants::USERNAME, username);
        env::set_var(env_constants::PASSWORD, password);
    }

    println!("Credentials saved to .env file");

    #[cfg(unix)]
    println!("File permissions set to owner-only (600)");

    #[cfg(not(unix))]
    println!(
        "Warning: File permissions not set (non-Unix system). Please ensure .env file is protected."
    );

    Ok(())
}

/// Verify credentials by attempting authentication with CEDA
pub async fn verify_credentials() -> AuthResult<bool> {
    if !check_credentials() {
        return Err(AuthError::MissingCredentials);
    }

    println!("Verifying credentials with CEDA...");

    // Use CedaClient to test authentication
    match crate::app::CedaClient::new().await {
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
pub async fn setup_credentials() -> AuthResult<()> {
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
        io::stdout().flush().map_err(AuthError::CredentialStorage)?;

        let mut response = String::new();
        io::stdin()
            .read_line(&mut response)
            .map_err(AuthError::CredentialStorage)?;

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
pub async fn show_auth_status() -> AuthResult<()> {
    let mut status = get_auth_status();

    println!("CEDA Authentication Status");
    println!("=============================");
    println!();

    if let Ok(username) = env::var(env_constants::USERNAME) {
        println!("Username: {} (set)", username);
    } else {
        println!("Username: Not set");
    }

    println!(
        "Password: {}",
        if status.password_set {
            "Set"
        } else {
            "Not set"
        }
    );

    println!(
        ".env file: {}",
        if status.dotenv_file_exists {
            "Exists"
        } else {
            "Not found"
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
        println!("To configure credentials, run: midas_fetcher auth setup");
    } else if status.credentials_valid == Some(false) {
        println!();
        println!("To update credentials, run: midas_fetcher auth setup");
    }

    Ok(())
}

/// Check if command requires authentication and prompt setup if needed
pub async fn ensure_authenticated() -> AuthResult<()> {
    if !check_credentials() {
        println!("This command requires CEDA authentication credentials.");
        println!();

        print!("Would you like to set up authentication now? [Y/n]: ");
        io::stdout().flush().map_err(AuthError::CredentialStorage)?;

        let mut response = String::new();
        io::stdin()
            .read_line(&mut response)
            .map_err(AuthError::CredentialStorage)?;

        if response.trim().to_lowercase().starts_with('n') {
            return Err(AuthError::MissingCredentials);
        }

        println!();
        setup_credentials().await?;

        // Check again after setup
        if !check_credentials() {
            return Err(AuthError::MissingCredentials);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_valid_username() {
        // Valid cases
        assert!(is_valid_username("testuser"));
        assert!(is_valid_username("test.user"));
        assert!(is_valid_username("test-user"));
        assert!(is_valid_username("test_user"));
        assert!(is_valid_username("test123"));
        assert!(is_valid_username("user.name123"));

        // Invalid cases
        assert!(!is_valid_username("")); // empty
        assert!(!is_valid_username("ab")); // too short
        assert!(!is_valid_username("test user")); // space
        assert!(!is_valid_username("test@user")); // special char
        assert!(!is_valid_username(&"a".repeat(51))); // too long
    }

    #[test]
    fn test_auth_status_structure() {
        let status = get_auth_status();

        // Test status message generation
        let message = status.status_message();
        assert!(!message.is_empty());

        // Test has_credentials logic
        let has_creds = status.has_credentials();
        assert_eq!(has_creds, status.username_set && status.password_set);
    }

    #[test]
    fn test_auth_status_messages() {
        let mut status = AuthStatus {
            username_set: false,
            password_set: false,
            dotenv_file_exists: false,
            credentials_valid: None,
        };

        // No credentials
        assert!(status.status_message().contains("Missing credentials"));

        // Credentials set but not verified
        status.username_set = true;
        status.password_set = true;
        assert!(status.status_message().contains("not verified"));

        // Credentials verified
        status.credentials_valid = Some(true);
        assert!(status.status_message().contains("verified"));

        // Credentials invalid
        status.credentials_valid = Some(false);
        assert!(status.status_message().contains("invalid"));
    }

    #[tokio::test]
    async fn test_verify_credentials_missing() {
        // Ensure no credentials are set for this test
        unsafe {
            env::remove_var(env_constants::USERNAME);
            env::remove_var(env_constants::PASSWORD);
        }

        let result = verify_credentials().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::MissingCredentials));
    }

    #[test]
    fn test_check_credentials() {
        // Save current state
        let original_username = env::var(env_constants::USERNAME).ok();
        let original_password = env::var(env_constants::PASSWORD).ok();

        unsafe {
            // Test with no credentials
            env::remove_var(env_constants::USERNAME);
            env::remove_var(env_constants::PASSWORD);
            assert!(!check_credentials());

            // Test with only username
            env::set_var(env_constants::USERNAME, "testuser");
            assert!(!check_credentials());

            // Test with both credentials
            env::set_var(env_constants::PASSWORD, "testpass");
            assert!(check_credentials());

            // Restore original state
            if let Some(username) = original_username {
                env::set_var(env_constants::USERNAME, username);
            } else {
                env::remove_var(env_constants::USERNAME);
            }

            if let Some(password) = original_password {
                env::set_var(env_constants::PASSWORD, password);
            } else {
                env::remove_var(env_constants::PASSWORD);
            }
        }
    }

    #[test]
    fn test_save_credentials_new_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let env_path = temp_dir.path().join(".env");

        // Change to temp directory
        let original_dir = env::current_dir()?;
        env::set_current_dir(&temp_dir)?;

        // Save credentials
        let result = save_credentials("testuser", "testpass");
        assert!(result.is_ok());

        // Check file exists
        assert!(env_path.exists());

        // Check file contents
        let contents = std::fs::read_to_string(&env_path)?;
        assert!(contents.contains("CEDA_USERNAME=testuser"));
        assert!(contents.contains("CEDA_PASSWORD=testpass"));

        // Check permissions (Unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&env_path)?;
            let permissions = metadata.permissions();
            assert_eq!(permissions.mode() & 0o777, 0o600);
        }

        // Restore directory
        env::set_current_dir(original_dir)?;

        Ok(())
    }
}
