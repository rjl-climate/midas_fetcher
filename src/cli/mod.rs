//! Command-line interface components
//!
//! This module contains CLI-specific code for the MIDAS Fetcher application,
//! including argument parsing, progress display, and user interaction.

pub mod progress;

pub use progress::{ProgressConfig, ProgressDisplay, ProgressEvent};
