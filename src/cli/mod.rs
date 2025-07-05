//! Command-line interface components
//!
//! This module contains CLI-specific code for the MIDAS Fetcher application,
//! including argument parsing, progress display, and user interaction.

pub mod args;
pub mod commands;
pub mod progress;
pub mod startup;

pub use args::{
    AuthAction, AuthArgs, CacheAction, CacheArgs, Cli, Commands, DownloadArgs, GlobalArgs,
    ManifestAction, ManifestArgs,
};
pub use commands::{handle_auth, handle_cache, handle_download, handle_manifest};
pub use progress::{ProgressConfig, ProgressDisplay, ProgressEvent};
pub use startup::{StartupStatus, interactive_selection, show_startup_status, validate_startup};
