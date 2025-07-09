//! Real CEDA authentication integration tests
//!
//! These tests require actual CEDA credentials and verify the complete
//! authentication flow against the real CEDA service.

#[cfg(test)]
mod tests {
    use std::env;
    use tempfile::tempdir;
    use url::Url;

    use crate::app::client::CedaClient;
    use crate::constants::{ceda, env as env_constants};
    use crate::errors::AuthError;

    #[tokio::test]
    #[ignore] // Requires real CEDA credentials
    async fn test_real_authentication() {
        // Test complete CEDA authentication flow with real credentials
        // 
        // This test verifies:
        // - CSRF token extraction from login page
        // - Form submission with credentials
        // - Authentication verification with protected resource
        // - File download capability
        //
        // Setup instructions:
        // 1. Create a .env file in the project root with:
        //    CEDA_USERNAME=your_username
        //    CEDA_PASSWORD=your_password
        // 2. Or set environment variables directly:
        //    export CEDA_USERNAME=your_username
        //    export CEDA_PASSWORD=your_password
        //
        // Run with: cargo test test_real_authentication -- --ignored --nocapture

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
                panic!("Download failed: {:?}", e);
            }
        }
    }
}