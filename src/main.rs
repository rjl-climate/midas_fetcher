//! MIDAS Fetcher CLI application
//!
//! Command-line interface for downloading UK Met Office MIDAS Open dataset files.
//! Features concurrent downloads, progress tracking, and comprehensive error handling.

use std::process;

use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

// Import CLI modules through the library (module is public but not re-exported)
use midas_fetcher::cli::{
    Cli, Commands, handle_auth, handle_cache, handle_download, handle_manifest,
};
use midas_fetcher::errors::Result;

#[tokio::main]
async fn main() {
    // Initialize program
    let result = run().await;

    // Handle any errors that occurred
    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

/// Main application logic
async fn run() -> Result<()> {
    // Load environment variables from .env file if it exists
    dotenv::dotenv().ok(); // Ignore errors if file doesn't exist

    // Parse command line arguments
    let cli = Cli::parse_args();

    // Initialize logging based on verbosity
    init_logging(&cli);

    info!("MIDAS Fetcher v{} starting", env!("CARGO_PKG_VERSION"));

    // Execute the appropriate command
    match cli.command {
        Commands::Download(args) => {
            info!("Executing download command");
            handle_download(args).await
        }
        Commands::Manifest(args) => {
            info!("Executing manifest command");
            handle_manifest(args).await
        }
        Commands::Auth(args) => {
            info!("Executing auth command");
            handle_auth(args).await
        }
        Commands::Cache(args) => {
            info!("Executing cache command");
            handle_cache(args).await
        }
    }
}

/// Initialize logging based on CLI verbosity settings
fn init_logging(cli: &Cli) {
    let log_level = cli.log_level();

    // Create environment filter
    let filter = EnvFilter::from_default_env()
        .add_directive(format!("midas_fetcher={}", log_level).parse().unwrap());

    // Initialize subscriber
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_level(cli.global.very_verbose) // Show levels only in very verbose mode
        .init();

    if cli.global.very_verbose {
        info!("Very verbose logging enabled");
    } else if cli.global.verbose {
        info!("Verbose logging enabled");
    }
}
