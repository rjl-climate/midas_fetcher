//! Manifest hash validation integration tests
//!
//! These tests verify that manifest file hashes match actual file content
//! downloaded from CEDA, ensuring data integrity throughout the pipeline.

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::Path;
    use tempfile::tempdir;
    use url::Url;
    use futures::StreamExt;

    use crate::app::client::CedaClient;
    use crate::app::{ManifestConfig, ManifestStreamer};
    use crate::constants::{ceda, env as env_constants};

    #[tokio::test]
    #[ignore] // Requires real CEDA credentials and actual file download
    async fn test_manifest_hash_validation() {
        // Test that validates manifest file hashes against actual downloaded content
        //
        // This test verifies:
        // - Manifest file parsing works correctly
        // - CEDA authentication is functional
        // - File download is working properly
        // - Hash validation confirms data integrity
        // - The manifest file contains accurate hash information
        //
        // This validates that the hashes in the manifest file actually match
        // the files available on CEDA. It downloads a real file and verifies the hash.
        //
        // Run with: cargo test test_manifest_hash_validation -- --ignored --nocapture

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

        let manifest_version = ManifestStreamer::extract_manifest_version_from_path(manifest_path);
        let mut streamer = ManifestStreamer::with_config_and_version(config, manifest_version);
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
}