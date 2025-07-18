//! Command-line argument parsing for MIDAS Fetcher
//!
//! This module defines the CLI structure using clap derive macros,
//! providing a user-friendly interface for dataset discovery, downloading,
//! authentication management, and cache operations.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// MIDAS Fetcher - Download UK Met Office weather data
#[derive(Parser, Debug)]
#[command(
    name = "midas_fetcher",
    version,
    about = "Download UK Met Office MIDAS Open dataset files efficiently",
    long_about = "A high-performance tool for downloading weather data from the UK Met Office MIDAS Open dataset.
Features concurrent downloads, automatic retry logic, and comprehensive progress tracking."
)]
pub struct Cli {
    /// Global options
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Subcommands
    #[command(subcommand)]
    pub command: Commands,
}

/// Global arguments available to all subcommands
#[derive(Args, Debug)]
pub struct GlobalArgs {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Very verbose logging (debug level)
    #[arg(long, global = true)]
    pub very_verbose: bool,

    /// Quiet mode - suppress non-essential output
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Configuration file path
    #[arg(long, global = true, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Cache directory path
    #[arg(long, global = true, value_name = "DIR")]
    pub cache_dir: Option<PathBuf>,
}

/// Available CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Download MIDAS dataset files
    Download(DownloadArgs),

    /// Manage manifest files
    Manifest(ManifestArgs),

    /// Manage authentication credentials
    Auth(AuthArgs),

    /// Cache management and verification
    Cache(CacheArgs),
}

/// Arguments for the download command
#[derive(Args, Debug, Clone)]
pub struct DownloadArgs {
    /// Specific dataset to download (e.g., "uk-daily-temperature-obs")
    #[arg(short, long)]
    pub dataset: Option<String>,

    /// Specific county/region to download
    #[arg(short, long)]
    pub county: Option<String>,

    /// Use quality control version 0 instead of default version 1
    #[arg(long = "quality-0")]
    pub quality_zero: bool,

    /// Maximum number of files to download (for testing)
    #[arg(short, long)]
    pub limit: Option<usize>,

    /// Number of concurrent download workers
    #[arg(short = 'w', long, default_value = "8")]
    pub workers: usize,

    /// Force re-download of existing files
    #[arg(short, long)]
    pub force: bool,

    /// Dry run - show what would be downloaded without downloading
    #[arg(long)]
    pub dry_run: bool,

    /// Enable verbose logging and detailed progress information
    #[arg(short, long)]
    pub verbose: bool,

    /// Use specific manifest version (e.g., 202407)
    #[arg(long)]
    pub manifest_version: Option<u32>,

    /// Use the latest available manifest version (overrides compatibility checks)
    #[arg(long)]
    pub use_latest_manifest: bool,

    /// Allow incompatible manifest versions
    #[arg(long)]
    pub allow_incompatible_manifest: bool,
}

/// Arguments for manifest management
#[derive(Args, Debug)]
pub struct ManifestArgs {
    #[command(subcommand)]
    pub action: ManifestAction,
}

/// Manifest management actions
#[derive(Subcommand, Debug)]
pub enum ManifestAction {
    /// Download or update manifest files
    Update {
        /// Force update even if manifest is recent
        #[arg(short, long)]
        force: bool,

        /// Verify manifest integrity after download
        #[arg(long)]
        verify: bool,
    },

    /// Show manifest information and statistics
    Info {
        /// Path to manifest file
        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,
    },

    /// List available datasets and years from manifest
    List {
        /// Show only dataset names
        #[arg(long)]
        datasets_only: bool,

        /// Show years for specific dataset
        #[arg(long)]
        dataset: Option<String>,
    },

    /// Check if manifest needs updating
    Check {
        /// Show detailed comparison information
        #[arg(long)]
        detailed: bool,
    },
}

/// Arguments for authentication management
#[derive(Args, Debug)]
pub struct AuthArgs {
    #[command(subcommand)]
    pub action: AuthAction,
}

/// Authentication actions
#[derive(Subcommand, Debug)]
pub enum AuthAction {
    /// Set up CEDA authentication credentials
    Setup {
        /// Force setup even if credentials exist
        #[arg(short, long)]
        force: bool,
    },

    /// Verify current credentials
    Verify,

    /// Show authentication status
    Status,

    /// Clear stored credentials
    Clear,
}

/// Arguments for cache management
#[derive(Args, Debug)]
pub struct CacheArgs {
    #[command(subcommand)]
    pub action: CacheAction,
}

/// Cache management actions
#[derive(Subcommand, Debug)]
pub enum CacheAction {
    /// Verify cache integrity by checking file hashes against manifest
    Verify {
        /// Specific dataset to verify
        #[arg(short, long)]
        dataset: Option<String>,
    },

    /// Show cache statistics and information
    Info,

    /// Clean up incomplete or corrupted files
    Clean {
        /// Remove all cached files
        #[arg(long)]
        all: bool,

        /// Remove only failed/incomplete downloads
        #[arg(long)]
        failed_only: bool,
    },
}

impl Cli {
    /// Parse command line arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Get the logging level based on global arguments
    pub fn log_level(&self) -> tracing::Level {
        if self.global.quiet {
            tracing::Level::ERROR
        } else if self.global.very_verbose {
            tracing::Level::DEBUG
        } else if self.global.verbose {
            tracing::Level::INFO
        } else {
            tracing::Level::WARN
        }
    }
}

impl DownloadArgs {
    /// Validate download arguments
    pub fn validate(&self) -> Result<(), String> {
        if self.workers == 0 {
            return Err("Number of workers must be greater than 0".to_string());
        }

        Ok(())
    }

    /// Get the quality control version to use
    pub fn quality_version(&self) -> crate::app::models::QualityControlVersion {
        if self.quality_zero {
            crate::app::models::QualityControlVersion::V0
        } else {
            crate::app::models::QualityControlVersion::V1
        }
    }

    /// Check if this is a filtered download (specific criteria)
    pub fn is_filtered(&self) -> bool {
        self.dataset.is_some() || self.county.is_some() || self.quality_zero
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_args_validation() {
        let mut args = DownloadArgs {
            dataset: None,
            county: None,
            quality_zero: false,
            limit: None,
            workers: 8,
            force: false,
            dry_run: false,
            verbose: false,
            manifest_version: None,
            use_latest_manifest: false,
            allow_incompatible_manifest: false,
        };

        // Valid configuration
        assert!(args.validate().is_ok());

        // Invalid: zero workers
        args.workers = 0;
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_quality_version_selection() {
        let args_v1 = DownloadArgs {
            dataset: None,
            county: None,
            quality_zero: false,
            limit: None,
            workers: 8,
            force: false,
            dry_run: false,
            verbose: false,
            manifest_version: None,
            use_latest_manifest: false,
            allow_incompatible_manifest: false,
        };

        let args_v0 = DownloadArgs {
            quality_zero: true,
            ..args_v1.clone()
        };

        assert_eq!(
            args_v1.quality_version(),
            crate::app::models::QualityControlVersion::V1
        );
        assert_eq!(
            args_v0.quality_version(),
            crate::app::models::QualityControlVersion::V0
        );
    }

    #[test]
    fn test_filtering_detection() {
        let base_args = DownloadArgs {
            dataset: None,
            county: None,
            quality_zero: false,
            limit: None,
            workers: 8,
            force: false,
            dry_run: false,
            verbose: false,
            manifest_version: None,
            use_latest_manifest: false,
            allow_incompatible_manifest: false,
        };

        // No filtering
        assert!(!base_args.is_filtered());

        // With dataset filter
        let filtered_args = DownloadArgs {
            dataset: Some("uk-daily-temperature-obs".to_string()),
            ..base_args.clone()
        };
        assert!(filtered_args.is_filtered());

        // With quality filter
        let quality_args = DownloadArgs {
            quality_zero: true,
            ..base_args.clone()
        };
        assert!(quality_args.is_filtered());
    }

    #[test]
    fn test_log_level() {
        let cli_quiet = Cli {
            global: GlobalArgs {
                verbose: false,
                very_verbose: false,
                quiet: true,
                config: None,
                cache_dir: None,
            },
            command: Commands::Auth(AuthArgs {
                action: AuthAction::Status,
            }),
        };

        let cli_verbose = Cli {
            global: GlobalArgs {
                verbose: true,
                very_verbose: false,
                quiet: false,
                config: None,
                cache_dir: None,
            },
            command: Commands::Auth(AuthArgs {
                action: AuthAction::Status,
            }),
        };

        assert_eq!(cli_quiet.log_level(), tracing::Level::ERROR);
        assert_eq!(cli_verbose.log_level(), tracing::Level::INFO);
    }
}
