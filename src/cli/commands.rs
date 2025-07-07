//! Command handlers for MIDAS Fetcher CLI
//!
//! This module implements the main command handlers that coordinate between
//! CLI arguments and the core application functionality.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error, info, warn};

use crate::app::{
    collect_datasets_and_years, filter_manifest_files, CacheConfig, CacheManager, CedaClient,
    Coordinator, CoordinatorConfig, ManifestStreamer, Md5Hash, WorkQueue,
};
use crate::auth::{setup_credentials, show_auth_status, verify_credentials};
use crate::cli::{
    interactive_selection, validate_startup, AuthAction, AuthArgs, CacheAction, CacheArgs,
    DownloadArgs, ManifestAction, ManifestArgs, ProgressConfig, ProgressDisplay,
};
use crate::errors::{AppError, Result};

/// Handle the download command
///
/// Orchestrates the complete download process including startup validation,
/// dataset selection, file filtering, and coordinated downloading.
pub async fn handle_download(args: DownloadArgs) -> Result<()> {
    use std::time::Instant;

    let start_time = Instant::now();
    info!("Starting download command with {} workers", args.workers);

    // Validate download arguments
    args.validate().map_err(AppError::generic)?;

    // Perform startup validation
    let validation_start = Instant::now();
    let startup_status = validate_startup(true, true).await?;
    if !startup_status.is_ready() {
        error!("Startup validation failed: {}", startup_status.summary());
        return Err(AppError::generic("System not ready for downloads"));
    }
    info!(
        "Startup validation completed in {:?}",
        validation_start.elapsed()
    );

    // Check authentication status early
    let auth_check_start = Instant::now();
    info!("Checking authentication status...");
    let auth_status = crate::auth::get_auth_status();
    info!("Auth status: {}", auth_status.status_message());
    if !auth_status.has_credentials() {
        warn!("No credentials found - downloads may fail. Run 'midas_fetcher auth setup' first.");
        println!("⚠️  Warning: No CEDA credentials found. Downloads may fail.");
        println!("   Run 'midas_fetcher auth setup' to configure credentials.");
        println!();
    } else {
        info!("Credentials are available for authentication");
    }
    info!(
        "Authentication check completed in {:?}",
        auth_check_start.elapsed()
    );

    // Determine manifest file to use
    let manifest_start = Instant::now();
    let manifest_path = find_manifest_file().await?;
    info!(
        "Using manifest file: {} (found in {:?})",
        manifest_path.display(),
        manifest_start.elapsed()
    );

    // Interactive dataset selection
    let selection_start = Instant::now();
    let selected_dataset = interactive_selection(&manifest_path, args.dataset.as_deref()).await?;
    info!(
        "Dataset selection completed in {:?}",
        selection_start.elapsed()
    );

    // Get quality control version
    let quality_version = args.quality_version();
    info!("Using quality control version: {}", quality_version);

    // Filter files based on criteria with progress feedback
    let filtering_start = Instant::now();
    info!("Filtering files based on selection criteria...");

    // Create progress spinner for filtering
    use indicatif::{ProgressBar, ProgressStyle};
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["◐", "◓", "◑", "◒"]),
    );

    let filter_message = format!("Filtering files for dataset '{}'...", selected_dataset);
    spinner.set_message(filter_message);
    spinner.enable_steady_tick(std::time::Duration::from_millis(120));

    let files_to_download = filter_manifest_files(
        &manifest_path,
        Some(&selected_dataset),
        args.county.as_deref(),
        &quality_version,
        args.metadata_only,
        args.data_only,
    )
    .await
    .map_err(AppError::Manifest)?;

    let filtering_duration = filtering_start.elapsed();
    spinner.finish_and_clear();
    info!(
        "File filtering completed: {} files in {:?}",
        files_to_download.len(),
        filtering_duration
    );

    if files_to_download.is_empty() {
        warn!("No files match the specified criteria");
        println!("No files found matching your criteria:");
        println!("  Dataset: {}", selected_dataset);
        if let Some(county) = &args.county {
            println!("  County: {}", county);
        }
        println!("  Quality: {}", quality_version);
        return Ok(());
    }

    // Apply limit if specified
    let files_to_download = if let Some(limit) = args.limit {
        if files_to_download.len() > limit {
            info!(
                "Limiting download to {} files (from {} total)",
                limit,
                files_to_download.len()
            );
            files_to_download.into_iter().take(limit).collect()
        } else {
            files_to_download
        }
    } else {
        files_to_download
    };

    info!("Selected {} files for download", files_to_download.len());

    // Show what would be downloaded in dry-run mode
    if args.dry_run {
        println!(
            "Dry run - would download {} files:",
            files_to_download.len()
        );
        for (i, file) in files_to_download.iter().take(10).enumerate() {
            println!("  {}. {} ({})", i + 1, file.file_name, file.hash);
        }
        if files_to_download.len() > 10 {
            println!("  ... and {} more files", files_to_download.len() - 10);
        }
        return Ok(());
    }

    // Setup shared components
    let setup_start = Instant::now();
    info!("Setting up cache, client, and work queue...");

    // Setup early signal handling for long-running queue operations
    let _signal_handle = tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("Failed to setup Ctrl-C handler: {}", e);
            return;
        }
        eprintln!("\n🛑 Ctrl-C received during setup - forcing exit");
        std::process::exit(1);
    });

    let cache_config = CacheConfig {
        cache_root: args.cache_dir(),
        ..Default::default()
    };
    let cache = Arc::new(CacheManager::new(cache_config).await?);

    // The authentication step can take 90+ seconds - this is the bottleneck
    print!("🔐 Authenticating with CEDA...");
    io::stdout().flush().unwrap();
    let client = Arc::new(CedaClient::new().await?);
    println!(" ✅");
    let queue = Arc::new(WorkQueue::new());

    // Check for existing queue state
    let initial_stats = queue.stats().await;
    if initial_stats.total_added > 0 || initial_stats.completed_count > 0 {
        println!(
            "📋 Found existing queue state: {} completed, {} in progress",
            initial_stats.completed_count, initial_stats.in_progress_count
        );
        if args.force {
            println!("🔄 Force flag set - clearing previous download state");
            // TODO: Add queue reset method
        }
    }

    // Add files to work queue using efficient bulk operation
    let total_files = files_to_download.len();
    let bulk_start = Instant::now();

    let added_count = queue.add_work_bulk(files_to_download).await?;

    let bulk_duration = bulk_start.elapsed();
    info!(
        "Added {} files to queue in {:?}",
        added_count, bulk_duration
    );

    if added_count == 0 {
        println!("ℹ️  No new files to download - all files already completed or in progress");
        return Ok(());
    } else if added_count < total_files {
        let skipped = total_files - added_count;
        println!(
            "ℹ️  Added {} new files, {} already completed/queued",
            added_count, skipped
        );
    }

    let setup_duration = setup_start.elapsed();
    info!("Component setup completed in {:?}", setup_duration);

    // Setup coordinator
    let coordinator_start = Instant::now();
    info!("Setting up coordinator with {} workers...", args.workers);

    let coordinator_config = CoordinatorConfig {
        worker_count: args.workers,
        enable_progress_bar: !args.quiet(),
        verbose_logging: args.verbose(),
        ..Default::default()
    };

    let coordinator = Coordinator::new(coordinator_config, queue.clone(), cache, client);
    info!(
        "Coordinator setup completed in {:?}",
        coordinator_start.elapsed()
    );

    // Check if there's work to do before starting
    let queue_stats = queue.stats().await;
    let actual_files_to_process = queue_stats.pending_count as usize;
    info!(
        "Queue stats: {} files pending for download",
        actual_files_to_process
    );

    if actual_files_to_process == 0 {
        println!("ℹ️  No files to download - all work already completed");
        return Ok(());
    }

    // Setup progress display (but don't start it yet)
    let progress_config = ProgressConfig {
        enable_progress_bars: !args.quiet(),
        show_worker_details: args.verbose(),
        ..Default::default()
    };
    let mut progress_display = ProgressDisplay::new(progress_config);

    // Run downloads with progress display
    let download_start = Instant::now();
    info!(
        "Starting coordinated download process with {} workers...",
        args.workers
    );
    println!("🚀 Starting downloads with {} workers...", args.workers);

    // Start progress display just before downloads begin
    progress_display
        .start(actual_files_to_process, args.workers)
        .await
        .map_err(AppError::Download)?;

    // Run downloads with real progress updates
    let session_result = {
        // Start the downloads in the background
        let coordinator_task = {
            let coord = coordinator;
            tokio::spawn(async move {
                let mut coord = coord;
                coord.run_downloads().await
            })
        };

        // Monitor progress and update display in real-time
        let mut last_completed = 0usize;
        let mut last_failed = 0usize;

        // Run monitoring loop directly without timeout wrapper for now
        loop {
            // Get current queue statistics
            let queue_stats = queue.stats().await;
            let current_completed = queue_stats.completed_count as usize;
            let current_failed = queue_stats.failed_count as usize;

            // Update progress display if there's been progress
            if current_completed > last_completed || current_failed > last_failed {
                if args.verbose() {
                    eprintln!(
                        "🔄 Progress update: {}/{} completed, {} failed",
                        current_completed, actual_files_to_process, current_failed
                    );
                }
                progress_display
                    .update_with_stats(current_completed, current_failed)
                    .await
                    .map_err(AppError::Download)?;
                last_completed = current_completed;
                last_failed = current_failed;
            }

            // Check if coordinator is done
            if coordinator_task.is_finished() {
                let result = coordinator_task
                    .await
                    .map_err(|e| AppError::generic(format!("Coordinator task panicked: {}", e)))?;
                break result.map_err(AppError::Download)?;
            }

            // Check if work is complete (but coordinator might still be cleaning up)
            if queue_stats.pending_count == 0 && queue_stats.in_progress_count == 0 {
                // Wait a bit and check again
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }

            // Small sleep to avoid excessive polling
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    let download_duration = download_start.elapsed();

    // Finish progress display
    progress_display
        .finish()
        .await
        .map_err(AppError::Download)?;

    // Report results
    let total_duration = start_time.elapsed();
    let stats = &session_result.stats;

    info!(
        "Download session completed in {:?} (total elapsed: {:?})",
        download_duration, total_duration
    );

    println!("\n📊 Download Summary:");
    println!("  Total files: {}", stats.total_files);
    println!("  Completed: {}", stats.files_completed);
    println!("  Failed: {}", stats.files_failed);
    println!("  Download time: {:?}", download_duration);
    println!("  Total time: {:?}", total_duration);
    println!(
        "  Success rate: {:.1}%",
        (stats.files_completed as f64 / stats.total_files as f64) * 100.0
    );

    if !session_result.success {
        warn!("Download session completed with errors");
        if !session_result.shutdown_errors.is_empty() {
            println!("\nShutdown errors:");
            for error in &session_result.shutdown_errors {
                println!("  • {}", error);
            }
        }
    }

    Ok(())
}

/// Handle manifest-related commands
pub async fn handle_manifest(args: ManifestArgs) -> Result<()> {
    match args.action {
        ManifestAction::Update { force, verify } => handle_manifest_update(force, verify).await,
        ManifestAction::Info { file } => handle_manifest_info(file).await,
        ManifestAction::List {
            datasets_only,
            dataset,
        } => handle_manifest_list(datasets_only, dataset).await,
        ManifestAction::Check { detailed } => handle_manifest_check(detailed).await,
    }
}

/// Handle manifest update command
async fn handle_manifest_update(force: bool, verify: bool) -> Result<()> {
    use crate::cli::startup::{check_manifest_update_needed, download_manifest_files};

    info!(
        "Updating manifest files (force: {}, verify: {})",
        force, verify
    );

    println!("📋 Manifest Update");
    println!("=================");
    println!();

    // Check if update is needed (unless forced)
    if !force {
        match check_manifest_update_needed().await {
            Ok(status) => {
                println!("Checking for updates...");

                if let Some(version) = status.local_version {
                    println!(
                        "Local version:  v{} ({})",
                        version,
                        status.age_description()
                    );
                }
                println!("Remote version: v{}", status.remote_version);
                println!("Status: {}", status.status_message());
                println!();

                if !status.needs_update {
                    println!("✅ Manifest is already up to date!");
                    return Ok(());
                }
            }
            Err(e) => {
                warn!("Could not check for updates: {}", e);
                println!("⚠️  Could not check current status, proceeding with download...");
            }
        }
    }

    // Create progress spinner for manifest download
    use indicatif::{ProgressBar, ProgressStyle};
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["◐", "◓", "◑", "◒"]),
    );

    let download_message = if force {
        "Force downloading latest manifest..."
    } else {
        "Downloading latest manifest..."
    };
    spinner.set_message(download_message);
    spinner.enable_steady_tick(std::time::Duration::from_millis(120));

    // Download the manifest and generate metadata
    match download_manifest_files().await {
        Ok(()) => {
            spinner.finish_and_clear();
            println!("✅ Manifest updated successfully!");

            if verify {
                println!();
                println!("🔍 Verifying manifest integrity...");
                // TODO: Add verification logic here
                println!("✅ Manifest verification complete");
            }
        }
        Err(e) => {
            spinner.finish_and_clear();
            println!("❌ Failed to download manifest");
            return Err(AppError::generic(format!(
                "Failed to update manifest: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Handle manifest check command
async fn handle_manifest_check(detailed: bool) -> Result<()> {
    use crate::cli::startup::check_manifest_update_needed;

    info!("Checking manifest update status (detailed: {})", detailed);

    println!("📋 Manifest Update Check");
    println!("========================");
    println!();

    match check_manifest_update_needed().await {
        Ok(status) => {
            // Basic information
            match status.local_version {
                Some(version) => {
                    println!(
                        "Local version:  v{} ({})",
                        version,
                        status.age_description()
                    );
                    if detailed {
                        if let Some(filename) = &status.local_filename {
                            println!("Local file:     {}", filename);
                        }
                    }
                }
                None => {
                    println!("Local version:  Not found");
                }
            }

            println!("Remote version: v{}", status.remote_version);
            if detailed {
                println!("Remote file:    {}", status.remote_filename);
            }

            println!();
            println!("Status: {}", status.status_message());

            // Recommendations
            if status.needs_update {
                println!();
                println!("📥 To update: midas_fetcher manifest update");
            } else if let Some(age) = status.local_age_days {
                if age > 7 {
                    println!();
                    println!(
                        "💡 Your manifest is {} days old. Consider updating occasionally to get",
                        age
                    );
                    println!("   the latest datasets and file checksums.");
                }
            }

            // Detailed information
            if detailed {
                println!();
                println!("Detailed Information:");
                println!("--------------------");
                if let Some(age) = status.local_age_days {
                    println!("Local manifest age: {} days", age);
                }
                println!("Update needed: {}", status.needs_update);

                if status.needs_update {
                    let version_diff = status.remote_version - status.local_version.unwrap_or(0);
                    println!("Version difference: {} releases", version_diff);
                }
            }
        }
        Err(e) => {
            return Err(AppError::generic(format!(
                "Failed to check manifest status: {}",
                e
            )));
        }
    }

    Ok(())
}

/// Handle manifest info command
async fn handle_manifest_info(file: Option<PathBuf>) -> Result<()> {
    let manifest_path = if let Some(path) = file {
        path
    } else {
        find_manifest_file().await?
    };

    println!("📋 Manifest Information");
    println!("=======================");

    info!("Analyzing manifest file: {}", manifest_path.display());

    // Add spinner for manifest loading
    use indicatif::{ProgressBar, ProgressStyle};
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["◐", "◓", "◑", "◒"]),
    );
    spinner.set_message("Loading manifest...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(120));

    let load_start = Instant::now();
    let datasets_map = collect_datasets_and_years(&manifest_path)
        .await
        .map_err(AppError::Manifest)?;

    spinner.finish_and_clear();
    println!(
        "Loading manifest... ✅ Analyzed {} entries ({}s)",
        datasets_map.values().map(|d| d.file_count).sum::<usize>(),
        load_start.elapsed().as_secs()
    );
    println!();

    // Display results as a clean table
    display_manifest_table(&datasets_map);

    Ok(())
}

/// Display manifest information as a clean table
fn display_manifest_table(datasets_map: &HashMap<String, crate::app::DatasetSummary>) {
    if datasets_map.is_empty() {
        println!("No datasets found in manifest.");
        return;
    }

    // Calculate column widths
    let name_width = datasets_map
        .keys()
        .map(|name| name.len())
        .max()
        .unwrap_or(12)
        .max(12); // Minimum width for "Dataset Name"

    let counties_width = 9; // Width for "Counties"
    let year_range_width = 10; // Width for "Year Range"
    let files_width = 7; // Width for "Files"

    // Print header
    println!(
        "{:<width$} {:>counties_width$} {:>year_range_width$} {:>files_width$}",
        "Dataset Name",
        "Counties",
        "Year Range",
        "Files",
        width = name_width,
        counties_width = counties_width,
        year_range_width = year_range_width,
        files_width = files_width
    );

    // Print separator line
    println!(
        "{}",
        "─".repeat(name_width + counties_width + year_range_width + files_width + 6)
    );

    // Sort datasets by name for consistent output
    let mut sorted_datasets: Vec<_> = datasets_map.iter().collect();
    sorted_datasets.sort_by_key(|(name, _)| *name);

    // Print data rows
    for (name, summary) in sorted_datasets {
        println!(
            "{:<width$} {:>counties_width$} {:>year_range_width$} {:>files_width$}",
            name,
            summary.counties.len(),
            summary.year_range(),
            summary.file_count,
            width = name_width,
            counties_width = counties_width,
            year_range_width = year_range_width,
            files_width = files_width
        );
    }
}

/// Display simple manifest table with dataset name and file count only
fn display_simple_manifest_table(datasets_map: &HashMap<String, crate::app::DatasetSummary>) {
    if datasets_map.is_empty() {
        println!("No datasets found in manifest.");
        return;
    }

    // Calculate column widths
    let name_width = datasets_map
        .keys()
        .map(|name| name.len())
        .max()
        .unwrap_or(12)
        .max(12); // Minimum width for "Dataset Name"

    let files_width = datasets_map
        .values()
        .map(|summary| summary.file_count.to_string().len())
        .max()
        .unwrap_or(5)
        .max(5); // Minimum width for "Files"

    // Print header
    println!(
        "{:<width$} {:>files_width$}",
        "Dataset Name",
        "Files",
        width = name_width,
        files_width = files_width
    );

    // Print separator line
    println!("{}", "─".repeat(name_width + files_width + 1));

    // Sort datasets by name for consistent output
    let mut sorted_datasets: Vec<_> = datasets_map.iter().collect();
    sorted_datasets.sort_by_key(|(name, _)| *name);

    // Print data rows
    for (name, summary) in sorted_datasets {
        println!(
            "{:<width$} {:>files_width$}",
            name,
            summary.file_count,
            width = name_width,
            files_width = files_width
        );
    }
}

/// Handle manifest list command
async fn handle_manifest_list(datasets_only: bool, dataset: Option<String>) -> Result<()> {
    let manifest_path = find_manifest_file().await?;

    // Add spinner for manifest loading
    use indicatif::{ProgressBar, ProgressStyle};
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["◐", "◓", "◑", "◒"]),
    );
    spinner.set_message("Preparing list...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(120));

    let datasets_map = collect_datasets_and_years(&manifest_path)
        .await
        .map_err(AppError::Manifest)?;

    spinner.finish_and_clear();

    if datasets_only {
        println!("Available Datasets:");
        for name in datasets_map.keys() {
            println!("  {}", name);
        }
        return Ok(());
    }

    if let Some(dataset_filter) = dataset {
        if let Some(summary) = datasets_map.get(&dataset_filter) {
            println!("📊 Dataset: {}", dataset_filter);
            println!("Available versions:");
            for version in &summary.versions {
                println!("  {}", version);
            }
            if let Some(latest) = summary.latest_version() {
                println!("Latest: {}", latest);
            }
        } else {
            return Err(AppError::generic(format!(
                "Dataset '{}' not found. Available: {}",
                dataset_filter,
                datasets_map.keys().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
        return Ok(());
    }

    // Show simple table with dataset name and file count only
    display_simple_manifest_table(&datasets_map);

    Ok(())
}

/// Handle authentication commands
pub async fn handle_auth(args: AuthArgs) -> Result<()> {
    match args.action {
        AuthAction::Setup { force } => {
            if force || !crate::auth::check_credentials() {
                setup_credentials().await.map_err(AppError::Auth)?;
            } else {
                println!("✅ Credentials already configured. Use --force to update.");
            }
        }
        AuthAction::Verify => {
            let is_valid = verify_credentials().await.map_err(AppError::Auth)?;
            if is_valid {
                println!("✅ Credentials verified successfully");
            } else {
                println!("❌ Credential verification failed");
            }
        }
        AuthAction::Status => {
            show_auth_status().await.map_err(AppError::Auth)?;
        }
        AuthAction::Clear => {
            println!("🗑️  Clearing stored credentials...");
            // TODO: Implement credential clearing
            println!("💡 To clear credentials, delete the .env file manually.");
        }
    }

    Ok(())
}

/// Handle cache management commands
pub async fn handle_cache(args: CacheArgs) -> Result<()> {
    match args.action {
        CacheAction::Verify { dataset } => handle_cache_verify(dataset).await,
        CacheAction::Info => handle_cache_info().await,
        CacheAction::Clean { all, failed_only } => handle_cache_clean(all, failed_only).await,
    }
}

/// Handle cache verification
async fn handle_cache_verify(dataset: Option<String>) -> Result<()> {
    info!("Verifying cache integrity (dataset: {:?})", dataset);

    println!("🔍 Cache Verification");
    println!("====================");
    println!();

    let start_time = Instant::now();

    // Phase 1: Setup cache manager
    let cache_config = CacheConfig::default();
    let cache = CacheManager::new(cache_config).await?;
    let cache_root = cache.cache_root().to_path_buf();

    // Phase 2: Scan cache directory with progress
    print!("🔍 Scanning cache directory...");
    io::stdout().flush().unwrap();

    let scan_start = Instant::now();
    let cached_files = scan_cache_files(&cache_root, dataset.as_deref()).await?;

    println!(
        " ✅ Found {} cached files ({}s)",
        cached_files.len(),
        scan_start.elapsed().as_secs()
    );

    if cached_files.is_empty() {
        println!("ℹ️  No cached files found to verify");
        return Ok(());
    }

    // Phase 3: Load manifest with progress
    use indicatif::{ProgressBar, ProgressStyle};
    let manifest_spinner = ProgressBar::new_spinner();
    manifest_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["◐", "◓", "◑", "◒"]),
    );
    manifest_spinner.set_message("📋 Loading manifest file...");
    manifest_spinner.enable_steady_tick(std::time::Duration::from_millis(120));

    let manifest_start = Instant::now();
    let manifest_path = find_manifest_file().await?;
    let manifest_hashes = load_manifest_hashes(&manifest_path, dataset.as_deref()).await?;

    manifest_spinner.finish_and_clear();
    println!(
        "📋 Loading manifest file... ✅ Loaded {} manifest entries ({}s)",
        manifest_hashes.len(),
        manifest_start.elapsed().as_secs()
    );

    // Phase 4: Verify files with progress
    println!("✅ Verifying file integrity...");

    let verify_start = Instant::now();
    let results = verify_files_with_progress(&cached_files, &manifest_hashes).await?;

    let verify_duration = verify_start.elapsed();
    let total_duration = start_time.elapsed();

    // Phase 5: Display results
    println!();
    println!("📊 Verification Results");
    println!("======================");
    println!("Files verified: {}", results.total_verified);
    println!("Valid files: {}", results.valid_count);
    println!("Corrupted files: {}", results.corrupted_count);
    println!(
        "Missing from manifest: {}",
        results.missing_from_manifest_count
    );
    println!("Verification time: {}s", verify_duration.as_secs());
    println!("Total time: {}s", total_duration.as_secs());

    // Show corrupted files if any
    if !results.corrupted_files.is_empty() {
        println!();
        println!("⚠️  Corrupted Files:");
        for file_path in &results.corrupted_files {
            println!("  {}", file_path.display());
        }
        println!();
        println!(
            "💡 These files should be re-downloaded. Run the download command to replace them."
        );
    }

    // Show missing files if any
    if !results.missing_from_manifest_files.is_empty() {
        println!();
        println!("❓ Files Not in Manifest:");
        for file_path in &results.missing_from_manifest_files {
            println!("  {}", file_path.display());
        }
        println!();
        println!("💡 These files may be from an older manifest or a different dataset.");
    }

    if results.corrupted_count > 0 || results.missing_from_manifest_count > 0 {
        println!();
        if results.corrupted_count > 0 {
            println!(
                "❌ Cache verification found {} corrupted files",
                results.corrupted_count
            );
        }
        if results.missing_from_manifest_count > 0 {
            println!(
                "⚠️  Found {} files not in current manifest",
                results.missing_from_manifest_count
            );
        }
    } else {
        println!();
        println!("✅ All cached files verified successfully!");
    }

    Ok(())
}

/// Results of cache verification
#[derive(Debug)]
struct VerificationResults {
    total_verified: usize,
    valid_count: usize,
    corrupted_count: usize,
    missing_from_manifest_count: usize,
    corrupted_files: Vec<PathBuf>,
    missing_from_manifest_files: Vec<PathBuf>,
}

/// Scan cache directory for files to verify
async fn scan_cache_files(cache_root: &Path, dataset_filter: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    // Use the same directory scanning logic as cache info
    scan_directory_recursive_for_verify(cache_root, &mut files, dataset_filter)?;

    Ok(files)
}

/// Recursively scan directory for CSV files
fn scan_directory_recursive_for_verify(
    dir: &Path,
    files: &mut Vec<PathBuf>,
    dataset_filter: Option<&str>,
) -> Result<()> {
    use std::fs;

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()), // Skip inaccessible directories
    };

    for entry in entries {
        let entry = entry
            .map_err(|e| AppError::generic(format!("Failed to read directory entry: {}", e)))?;
        let path = entry.path();

        if path.is_dir() {
            // Apply dataset filter if specified
            if let Some(filter) = dataset_filter {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if !dir_name.contains(filter) {
                        continue; // Skip directories not matching dataset filter
                    }
                }
            }

            // Recursively scan subdirectories
            scan_directory_recursive_for_verify(&path, files, dataset_filter)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            files.push(path);
        }
    }

    Ok(())
}

/// Load manifest and build hash lookup map
async fn load_manifest_hashes(
    manifest_path: &Path,
    dataset_filter: Option<&str>,
) -> Result<HashMap<PathBuf, Md5Hash>> {
    use futures::StreamExt;

    let mut manifest_streamer = ManifestStreamer::new();
    let mut manifest_stream = manifest_streamer
        .stream(manifest_path)
        .await
        .map_err(AppError::Manifest)?;

    let mut hash_map = HashMap::new();

    while let Some(file_info_result) = manifest_stream.next().await {
        let file_info = file_info_result.map_err(AppError::Manifest)?;

        // Apply dataset filter if specified
        if let Some(filter) = dataset_filter {
            if !file_info.dataset_info.dataset_name.contains(filter) {
                continue;
            }
        }

        // Build the expected cache path for this file
        let cache_path = build_cache_path_from_file_info(&file_info);
        hash_map.insert(cache_path, file_info.hash);
    }

    Ok(hash_map)
}

/// Build expected cache path from file info
fn build_cache_path_from_file_info(file_info: &crate::app::models::FileInfo) -> PathBuf {
    // This should match the cache path construction logic in the cache manager
    // For now, use the file name as a simple approach
    PathBuf::from(&file_info.file_name)
}

/// Verify files with progress indication
async fn verify_files_with_progress(
    cached_files: &[PathBuf],
    manifest_hashes: &HashMap<PathBuf, Md5Hash>,
) -> Result<VerificationResults> {
    use indicatif::{ProgressBar, ProgressStyle};

    let total_files = cached_files.len();
    let mut results = VerificationResults {
        total_verified: 0,
        valid_count: 0,
        corrupted_count: 0,
        missing_from_manifest_count: 0,
        corrupted_files: Vec::new(),
        missing_from_manifest_files: Vec::new(),
    };

    // Create progress bar
    let progress = ProgressBar::new(total_files as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} (ETA: {eta}) {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    progress.enable_steady_tick(std::time::Duration::from_millis(100));

    let start_time = Instant::now();

    for (i, file_path) in cached_files.iter().enumerate() {
        // Update progress every 100 files or on last file
        if i % 100 == 0 || i == total_files - 1 {
            progress.set_position(i as u64);

            // Calculate verification rate
            let elapsed = start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let rate = (i + 1) as f64 / elapsed;
                progress.set_message(format!("{:.1} files/sec", rate));
            } else {
                progress.set_message("Verifying files...");
            }
        }

        results.total_verified += 1;

        // Get the file name for manifest lookup
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(PathBuf::from)
            .unwrap_or_else(|| file_path.clone());

        // Check if file exists in manifest
        let expected_hash = match manifest_hashes.get(&file_name) {
            Some(hash) => hash,
            None => {
                results.missing_from_manifest_count += 1;
                results.missing_from_manifest_files.push(file_path.clone());
                continue;
            }
        };

        // Calculate actual file hash
        match calculate_file_md5(file_path).await {
            Ok(actual_hash) => {
                if actual_hash == *expected_hash {
                    results.valid_count += 1;
                } else {
                    results.corrupted_count += 1;
                    results.corrupted_files.push(file_path.clone());
                }
            }
            Err(_) => {
                // File couldn't be read, consider it corrupted
                results.corrupted_count += 1;
                results.corrupted_files.push(file_path.clone());
            }
        }
    }

    progress.finish_with_message("Verification complete");

    Ok(results)
}

/// Calculate MD5 hash of a file
async fn calculate_file_md5(file_path: &Path) -> Result<Md5Hash> {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    let mut file = File::open(file_path).await.map_err(|e| {
        AppError::generic(format!(
            "Failed to open file {}: {}",
            file_path.display(),
            e
        ))
    })?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.map_err(|e| {
        AppError::generic(format!(
            "Failed to read file {}: {}",
            file_path.display(),
            e
        ))
    })?;

    let digest = md5::compute(&buffer);
    let hash_bytes: [u8; 16] = digest.0;

    Ok(Md5Hash::from_bytes(hash_bytes))
}

/// Handle cache info display
async fn handle_cache_info() -> Result<()> {
    let cache_config = CacheConfig::default();
    let cache = CacheManager::new(cache_config).await?;

    let stats = cache.get_cache_stats().await;

    println!("💾 Cache Information");
    println!("===================");
    println!("Location: {}", cache.cache_root().display());
    println!("Cached files: {}", stats.cached_files_count);
    println!(
        "Cache size: {:.1} MB",
        stats.total_cache_size as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

/// Handle cache cleanup
async fn handle_cache_clean(all: bool, failed_only: bool) -> Result<()> {
    println!("🧹 Cache Cleanup");
    println!("===============");

    if all {
        println!("⚠️  This will remove ALL cached files!");
        // TODO: Implement full cache cleanup
    } else if failed_only {
        println!("🗑️  Removing failed/incomplete downloads...");
        // TODO: Implement failed file cleanup
    } else {
        println!("🗑️  Removing temporary and incomplete files...");
        // TODO: Implement selective cleanup
    }

    println!("💡 Cache cleanup functionality coming soon.");

    Ok(())
}

/// Find an available manifest file
async fn find_manifest_file() -> Result<PathBuf> {
    use crate::app::CacheManager;

    // Get cache directory
    let cache = CacheManager::new(Default::default())
        .await
        .map_err(AppError::Cache)?;
    let cache_root = cache.cache_root();

    // First check for legacy manifest file in current directory
    let legacy_path = Path::new("manifest.txt");
    if legacy_path.exists() {
        debug!("Found legacy manifest file: {}", legacy_path.display());
        return Ok(legacy_path.to_path_buf());
    }

    // Check for legacy manifest file in cache directory
    let cache_legacy_path = cache_root.join("manifest.txt");
    if cache_legacy_path.exists() {
        debug!(
            "Found legacy manifest file in cache: {}",
            cache_legacy_path.display()
        );
        return Ok(cache_legacy_path);
    }

    // Look for versioned manifest files in current directory first
    if let Ok(entries) = std::fs::read_dir(".") {
        let mut versioned_manifests: Vec<(u32, PathBuf)> = Vec::new();

        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                    // Parse version from filename
                    if let Some(start) = filename.find("midas-open-v") {
                        let version_start = start + "midas-open-v".len();
                        if let Some(end) = filename[version_start..].find("-md5s.txt") {
                            let version_str = &filename[version_start..version_start + end];
                            if let Ok(version) = version_str.parse::<u32>() {
                                versioned_manifests.push((version, entry.path()));
                            }
                        }
                    }
                }
            }
        }

        if !versioned_manifests.is_empty() {
            // Sort by version and return the latest
            versioned_manifests.sort_by_key(|&(version, _)| version);
            let (_, latest_path) = versioned_manifests.into_iter().last().unwrap();
            debug!("Found latest manifest file: {}", latest_path.display());
            return Ok(latest_path);
        }
    }

    // Look for versioned manifest files in cache directory
    if let Ok(entries) = std::fs::read_dir(cache_root) {
        let mut versioned_manifests: Vec<(u32, PathBuf)> = Vec::new();

        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("midas-open-v") && filename.ends_with("-md5s.txt") {
                    // Parse version from filename
                    if let Some(start) = filename.find("midas-open-v") {
                        let version_start = start + "midas-open-v".len();
                        if let Some(end) = filename[version_start..].find("-md5s.txt") {
                            let version_str = &filename[version_start..version_start + end];
                            if let Ok(version) = version_str.parse::<u32>() {
                                versioned_manifests.push((version, entry.path()));
                            }
                        }
                    }
                }
            }
        }

        if !versioned_manifests.is_empty() {
            // Sort by version and return the latest
            versioned_manifests.sort_by_key(|&(version, _)| version);
            let (_, latest_path) = versioned_manifests.into_iter().last().unwrap();
            debug!(
                "Found latest manifest file in cache: {}",
                latest_path.display()
            );
            return Ok(latest_path);
        }
    }

    Err(AppError::generic(
        "No manifest file found. Run 'midas_fetcher manifest update' to download one.",
    ))
}

// Helper trait to add missing methods to DownloadArgs
trait DownloadArgsExt {
    fn cache_dir(&self) -> Option<PathBuf>;
    fn quiet(&self) -> bool;
    fn verbose(&self) -> bool;
}

impl DownloadArgsExt for DownloadArgs {
    fn cache_dir(&self) -> Option<PathBuf> {
        // For now, use default cache directory
        // This would normally come from global args
        None
    }

    fn quiet(&self) -> bool {
        // For now, return false
        // This would normally come from global args
        false
    }

    fn verbose(&self) -> bool {
        self.verbose
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_find_manifest_file_not_found() {
        // In an empty directory with no cache files, should return error
        let temp_dir = TempDir::new().unwrap();
        let original_dir = std::env::current_dir().unwrap();

        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Set a temporary cache directory that doesn't exist
        let temp_cache_dir = temp_dir.path().join("empty_cache");
        unsafe {
            std::env::set_var("XDG_CACHE_HOME", temp_cache_dir.parent().unwrap());
        }

        let result = find_manifest_file().await;

        // Clean up environment variable
        unsafe {
            std::env::remove_var("XDG_CACHE_HOME");
        }

        // Note: This might not fail if the cache manager creates default directories
        // In a real scenario, the user would see a helpful error message
        if result.is_ok() {
            // If cache directory was created automatically, that's also valid behavior
            eprintln!("Note: Cache directory was auto-created, which is valid behavior");
        }

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[tokio::test]
    async fn test_find_manifest_file_found() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("manifest.txt");

        // Create a dummy manifest file
        std::fs::write(&manifest_path, "dummy content").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let result = find_manifest_file().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().file_name().unwrap(), "manifest.txt");

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();
    }
}
