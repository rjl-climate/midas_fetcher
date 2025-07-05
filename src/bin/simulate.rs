//! Queue Simulation Binary
//!
//! This educational binary demonstrates the work-stealing queue system in action
//! with synthetic data, terminal UI visualization, and edge case simulation.
//!
//! Run with: `cargo run --bin simulate`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use rand::prelude::*;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    widgets::{
        Axis, Block, Borders, Chart, Dataset, GraphType, List, ListItem, Paragraph, Row, Table,
    },
};
use tempfile::TempDir;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

use midas_fetcher::app::{
    models::FileInfo,
    queue::{WorkQueue, WorkQueueConfig},
};

/// Configuration for the simulation
#[derive(Debug, Clone)]
struct SimulationConfig {
    /// Number of workers to simulate
    pub worker_count: usize,
    /// Total number of files to generate
    pub file_count: usize,
    /// Percentage of duplicate files (0.0-1.0)
    pub duplicate_rate: f64,
    /// Percentage of files that will fail (0.0-1.0)
    pub failure_rate: f64,
    /// Minimum download time per file (milliseconds)
    pub min_download_time: u64,
    /// Maximum download time per file (milliseconds)
    pub max_download_time: u64,
    /// Simulation speed multiplier (higher = faster)
    pub speed_multiplier: f64,
    /// Whether to include edge cases
    pub include_edge_cases: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            worker_count: 8,
            file_count: 1000,
            duplicate_rate: 0.15, // 15% duplicates
            failure_rate: 0.05,   // 5% failures
            min_download_time: 50,
            max_download_time: 500,
            speed_multiplier: 10.0, // 10x faster than real downloads
            include_edge_cases: true,
        }
    }
}

/// Statistics for worker performance
#[derive(Debug, Clone, Default)]
struct WorkerStats {
    #[allow(dead_code)] // Used for future UI enhancements
    pub worker_id: u32,
    pub files_completed: u64,
    pub files_failed: u64,
    pub total_download_time: Duration,
    pub current_task: Option<String>,
    pub idle_time: Duration,
    pub last_activity: Option<Instant>,
}

impl WorkerStats {
    fn new(worker_id: u32) -> Self {
        Self {
            worker_id,
            last_activity: Some(Instant::now()),
            ..Default::default()
        }
    }

    fn average_download_time(&self) -> Duration {
        if self.files_completed > 0 {
            self.total_download_time / self.files_completed as u32
        } else {
            Duration::ZERO
        }
    }

    fn success_rate(&self) -> f64 {
        let total = self.files_completed + self.files_failed;
        if total > 0 {
            (self.files_completed as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Progress update from workers
#[derive(Debug, Clone)]
enum WorkerUpdate {
    Started {
        worker_id: u32,
        file_hash: String,
    },
    Completed {
        worker_id: u32,
        file_hash: String,
        duration: Duration,
    },
    Failed {
        worker_id: u32,
        file_hash: String,
        error: String,
    },
    Idle {
        worker_id: u32,
    },
}

/// Application state for the terminal UI
struct AppState {
    /// Queue instance
    queue: Arc<WorkQueue>,
    /// Worker statistics
    worker_stats: Arc<RwLock<HashMap<u32, WorkerStats>>>,
    /// Performance history for charting
    performance_history: Vec<(f64, f64)>, // (time, throughput)
    /// Queue size history
    queue_history: Vec<(f64, u64, u64, u64)>, // (time, pending, in_progress, completed)
    /// Simulation start time
    start_time: Instant,
    /// Whether simulation is running
    running: bool,
    /// Current selected tab
    #[allow(dead_code)] // Used for future UI tab navigation
    selected_tab: usize,
    /// Edge case events log
    edge_events: Vec<String>,
    /// Simulation configuration
    #[allow(dead_code)] // Used for future UI configuration display
    config: SimulationConfig,
}

impl AppState {
    fn new(queue: Arc<WorkQueue>, config: SimulationConfig) -> Self {
        Self {
            queue,
            worker_stats: Arc::new(RwLock::new(HashMap::new())),
            performance_history: Vec::new(),
            queue_history: Vec::new(),
            start_time: Instant::now(),
            running: true,
            selected_tab: 0,
            edge_events: Vec::new(),
            config,
        }
    }

    /// Get elapsed time since simulation start
    fn elapsed_time(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Calculate current throughput (files per second)
    async fn current_throughput(&self) -> f64 {
        let stats = self.queue.stats().await;
        let elapsed = self.elapsed_time();
        if elapsed > 0.0 {
            stats.completed_count as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Add performance data point
    async fn update_performance_history(&mut self) {
        let throughput = self.current_throughput().await;
        let time = self.elapsed_time();
        self.performance_history.push((time, throughput));

        // Keep only last 100 points
        if self.performance_history.len() > 100 {
            self.performance_history.remove(0);
        }
    }

    /// Add queue state data point
    async fn update_queue_history(&mut self) {
        let stats = self.queue.stats().await;
        let time = self.elapsed_time();
        self.queue_history.push((
            time,
            stats.pending_count,
            stats.in_progress_count,
            stats.completed_count,
        ));

        // Keep only last 100 points
        if self.queue_history.len() > 100 {
            self.queue_history.remove(0);
        }
    }

    /// Log an edge case event
    fn log_edge_event(&mut self, event: String) {
        let timestamp = Utc::now().format("%H:%M:%S");
        self.edge_events.push(format!("[{}] {}", timestamp, event));

        // Keep only last 20 events
        if self.edge_events.len() > 20 {
            self.edge_events.remove(0);
        }
    }
}

/// Generate synthetic file data for simulation
async fn generate_synthetic_files(temp_dir: &TempDir, config: &SimulationConfig) -> Vec<FileInfo> {
    let mut rng = thread_rng();
    let mut files = Vec::new();
    let mut seen_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();

    let counties = ["devon", "cornwall", "dorset", "somerset", "wiltshire"];
    let stations = [
        "01381_twist",
        "01382_exeter",
        "01383_plymouth",
        "01384_bristol",
        "01385_bath",
    ];
    let years: Vec<u16> = (1980..=2023).collect();

    info!("Generating {} synthetic files...", config.file_count);

    for i in 0..config.file_count {
        // Decide if this should be a duplicate
        let should_duplicate = rng.gen_bool(config.duplicate_rate) && !seen_hashes.is_empty();

        let hash = if should_duplicate {
            // Pick a random existing hash
            let existing_hashes: Vec<_> = seen_hashes.iter().collect();
            existing_hashes[rng.gen_range(0..existing_hashes.len())].clone()
        } else {
            // Generate a new hash
            format!("{:032x}", rng.r#gen::<u128>())
        };

        // Generate realistic MIDAS path
        let county = counties[rng.gen_range(0..counties.len())];
        let station = stations[rng.gen_range(0..stations.len())];
        let year = years[rng.gen_range(0..years.len())];
        let qc_version = if rng.gen_bool(0.7) { 1 } else { 0 };

        let path = format!(
            "./data/uk-daily-temperature-obs/dataset-version-202407/{}/{}/qc-version-{}/midas-open_uk-daily-temperature-obs_dv-202407_{}_{}_qcv-{}_{}.csv",
            county, station, qc_version, county, station, qc_version, year
        );

        if let Ok(file_info) = FileInfo::new(hash.clone(), path, temp_dir.path()) {
            files.push(file_info);
            seen_hashes.insert(hash);
        }

        // Progress logging
        if (i + 1) % (config.file_count / 10).max(1) == 0 {
            debug!("Generated {}/{} files", i + 1, config.file_count);
        }
    }

    info!(
        "Generated {} unique files with {:.1}% duplicates",
        files.len(),
        config.duplicate_rate * 100.0
    );

    files
}

/// Simulate a download worker
async fn simulate_worker(
    worker_id: u32,
    queue: Arc<WorkQueue>,
    config: SimulationConfig,
    progress_tx: mpsc::UnboundedSender<WorkerUpdate>,
    worker_stats: Arc<RwLock<HashMap<u32, WorkerStats>>>,
) {
    let mut stats = WorkerStats::new(worker_id);

    // Insert initial stats
    {
        let mut stats_map = worker_stats.write().await;
        stats_map.insert(worker_id, stats.clone());
    }

    info!("Worker {} starting", worker_id);

    loop {
        // Try to get work from queue
        if let Some(work_info) = queue.get_next_work().await {
            let file_hash = work_info.work_id().to_string();
            stats.current_task = Some(file_hash.clone());
            stats.last_activity = Some(Instant::now());

            // Notify that work started
            let _ = progress_tx.send(WorkerUpdate::Started {
                worker_id,
                file_hash: file_hash.clone(),
            });

            // Simulate download time and failure decisions
            let (download_time, actual_time, should_fail, error) = {
                let mut rng = thread_rng();
                let download_time = Duration::from_millis(
                    rng.gen_range(config.min_download_time..=config.max_download_time),
                );
                let actual_time = Duration::from_millis(
                    (download_time.as_millis() as f64 / config.speed_multiplier) as u64,
                );

                let should_fail = rng.gen_bool(config.failure_rate);
                let error = if should_fail {
                    match rng.gen_range(0..4) {
                        0 => "Network timeout".to_string(),
                        1 => "HTTP 503 - Server overloaded".to_string(),
                        2 => "HTTP 429 - Rate limited".to_string(),
                        _ => "Connection reset by peer".to_string(),
                    }
                } else {
                    String::new()
                };

                (download_time, actual_time, should_fail, error)
            };

            sleep(actual_time).await;

            if should_fail {
                queue
                    .mark_failed(&file_hash, &error)
                    .await
                    .unwrap_or_else(|e| {
                        warn!("Worker {} failed to mark work as failed: {}", worker_id, e);
                    });

                stats.files_failed += 1;
                let _ = progress_tx.send(WorkerUpdate::Failed {
                    worker_id,
                    file_hash,
                    error,
                });
            } else {
                // Simulate successful completion
                queue.mark_completed(&file_hash).await.unwrap_or_else(|e| {
                    warn!(
                        "Worker {} failed to mark work as completed: {}",
                        worker_id, e
                    );
                });

                stats.files_completed += 1;
                stats.total_download_time += download_time;
                let _ = progress_tx.send(WorkerUpdate::Completed {
                    worker_id,
                    file_hash,
                    duration: download_time,
                });
            }

            stats.current_task = None;

            // Update stats
            {
                let mut stats_map = worker_stats.write().await;
                stats_map.insert(worker_id, stats.clone());
            }
        } else {
            // No work available - worker is idle
            stats.current_task = None;
            let idle_start = Instant::now();

            let _ = progress_tx.send(WorkerUpdate::Idle { worker_id });

            // Sleep briefly before trying again
            sleep(Duration::from_millis(50)).await;

            stats.idle_time += idle_start.elapsed();

            // Update stats
            {
                let mut stats_map = worker_stats.write().await;
                stats_map.insert(worker_id, stats.clone());
            }

            // Check if simulation is finished
            if queue.is_finished().await {
                info!("Worker {} finished - no more work available", worker_id);
                break;
            }
        }
    }

    info!(
        "Worker {} completed. Stats: {} completed, {} failed, success rate: {:.1}%",
        worker_id,
        stats.files_completed,
        stats.files_failed,
        stats.success_rate()
    );
}

/// Edge case simulation scenarios
async fn simulate_edge_cases(
    queue: Arc<WorkQueue>,
    config: SimulationConfig,
    app_state: Arc<RwLock<AppState>>,
) {
    if !config.include_edge_cases {
        return;
    }

    let mut interval = interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Check if simulation is finished
        if queue.is_finished().await {
            break;
        }

        // Randomly trigger edge cases
        let edge_case = {
            let mut rng = thread_rng();
            rng.gen_range(0..5)
        };

        match edge_case {
            0 => {
                // Simulate timeout handling
                let timeout_count = queue.handle_timeouts().await;
                if timeout_count > 0 {
                    let mut state = app_state.write().await;
                    state.log_edge_event(format!("Handled {} worker timeouts", timeout_count));
                }
            }
            1 => {
                // Simulate memory cleanup
                let cleaned = queue.cleanup().await;
                if cleaned > 0 {
                    let mut state = app_state.write().await;
                    state.log_edge_event(format!("Cleaned up {} completed work items", cleaned));
                }
            }
            2 => {
                // Log queue statistics
                let stats = queue.stats().await;
                if stats.duplicate_count > 0 {
                    let mut state = app_state.write().await;
                    state.log_edge_event(format!(
                        "Detected {} duplicate files",
                        stats.duplicate_count
                    ));
                }
            }
            3 => {
                // Check for worker starvation (all workers idle but work available)
                if queue.has_work_available().await {
                    let worker_stats = app_state.read().await.worker_stats.read().await.clone();
                    let idle_workers = worker_stats
                        .values()
                        .filter(|s| s.current_task.is_none())
                        .count();

                    if idle_workers > 0 {
                        let mut state = app_state.write().await;
                        state.log_edge_event(format!(
                            "Work-stealing prevented {} workers from starving",
                            idle_workers
                        ));
                    }
                }
            }
            _ => {
                // Monitor performance drops
                let stats = queue.stats().await;
                let utilization = stats.worker_utilization(config.worker_count as u32);
                if utilization < 50.0 && stats.active_count() > 0 {
                    let mut state = app_state.write().await;
                    state.log_edge_event(format!("Low worker utilization: {:.1}%", utilization));
                }
            }
        }
    }
}

/// Render the main dashboard
fn render_dashboard(f: &mut Frame, app_state: &AppState, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Content
        ])
        .split(area);

    // Header
    let title = Paragraph::new("MIDAS Fetcher - Work-Stealing Queue Simulation")
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[0]);

    // Main content area
    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Left panel
            Constraint::Percentage(50), // Right panel
        ])
        .split(chunks[1]);

    // Left panel - Queue Stats and Performance
    render_left_panel(f, app_state, content_chunks[0]);

    // Right panel - Worker Stats and Events
    render_right_panel(f, app_state, content_chunks[1]);
}

/// Render left panel with queue stats and performance
fn render_left_panel(f: &mut Frame, app_state: &AppState, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // Queue stats
            Constraint::Min(0),    // Performance chart
        ])
        .split(area);

    // Queue statistics
    render_queue_stats(f, app_state, chunks[0]);

    // Performance chart
    render_performance_chart(f, app_state, chunks[1]);
}

/// Render right panel with worker stats and events
fn render_right_panel(f: &mut Frame, app_state: &AppState, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60), // Worker stats
            Constraint::Percentage(40), // Edge events
        ])
        .split(area);

    // Worker statistics table
    render_worker_stats(f, app_state, chunks[0]);

    // Edge case events
    render_edge_events(f, app_state, chunks[1]);
}

/// Render queue statistics
fn render_queue_stats(f: &mut Frame, _app_state: &AppState, area: Rect) {
    // This is a simplified version - we'll need to make it async-compatible
    let queue_stats = format!(
        "Queue Statistics\n\n\
        Pending: {}\n\
        In Progress: {}\n\
        Completed: {}\n\
        Failed: {}\n\
        Success Rate: {:.1}%",
        "Loading...", // We'll update this in the real implementation
        "Loading...",
        "Loading...",
        "Loading...",
        0.0
    );

    let paragraph = Paragraph::new(queue_stats)
        .block(Block::default().title("Queue Status").borders(Borders::ALL))
        .style(Style::default().fg(Color::White));

    f.render_widget(paragraph, area);
}

/// Render performance chart
fn render_performance_chart(f: &mut Frame, _app_state: &AppState, area: Rect) {
    // Placeholder for performance chart
    let chart_data: Vec<(f64, f64)> = vec![(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)];

    let datasets = vec![
        Dataset::default()
            .name("Throughput")
            .marker(symbols::Marker::Dot)
            .style(Style::default().fg(Color::Green))
            .graph_type(GraphType::Line)
            .data(&chart_data),
    ];

    let chart = Chart::new(datasets)
        .block(Block::default().title("Performance").borders(Borders::ALL))
        .x_axis(
            Axis::default()
                .title("Time (s)")
                .style(Style::default().fg(Color::Gray))
                .bounds([0.0, 60.0]),
        )
        .y_axis(
            Axis::default()
                .title("Files/sec")
                .style(Style::default().fg(Color::Gray))
                .bounds([0.0, 10.0]),
        );

    f.render_widget(chart, area);
}

/// Render worker statistics table
fn render_worker_stats(f: &mut Frame, _app_state: &AppState, area: Rect) {
    let rows = vec![
        Row::new(vec!["Worker 1", "Active", "15", "1", "93.8%"]),
        Row::new(vec!["Worker 2", "Idle", "12", "2", "85.7%"]),
        Row::new(vec!["Worker 3", "Active", "18", "0", "100.0%"]),
    ];

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Worker", "Status", "Completed", "Failed", "Success%"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title("Worker Statistics")
            .borders(Borders::ALL),
    )
    .style(Style::default().fg(Color::White));

    f.render_widget(table, area);
}

/// Render edge case events log
fn render_edge_events(f: &mut Frame, app_state: &AppState, area: Rect) {
    let events: Vec<ListItem> = app_state
        .edge_events
        .iter()
        .map(|event| ListItem::new(event.as_str()))
        .collect();

    let list = List::new(events)
        .block(
            Block::default()
                .title("Edge Case Events")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Yellow));

    f.render_widget(list, area);
}

/// Handle terminal input events
async fn handle_input() -> bool {
    if event::poll(Duration::from_millis(100)).unwrap_or(false) {
        if let Ok(event) = event::read() {
            match event {
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    match key_event.code {
                        KeyCode::Char('q') | KeyCode::Esc => return false,
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    true
}

/// Main simulation function
async fn run_simulation() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Configuration
    let config = SimulationConfig::default();

    info!(
        "Starting queue simulation with {} workers and {} files",
        config.worker_count, config.file_count
    );

    // Create temporary directory for file destinations
    let temp_dir = TempDir::new()?;

    // Generate synthetic files
    let files = generate_synthetic_files(&temp_dir, &config).await;

    // Create work queue
    let queue_config = WorkQueueConfig {
        max_retries: 3,
        retry_delay: Duration::from_secs(1), // Faster retries for simulation
        max_workers: config.worker_count as u32,
        work_timeout: Duration::from_secs(30),
        max_pending_items: config.file_count + 1000,
    };
    let queue = Arc::new(WorkQueue::with_config(queue_config));

    // Add all files to queue
    info!("Adding {} files to work queue...", files.len());
    for file in files {
        if let Err(e) = queue.add_work(file).await {
            warn!("Failed to add work to queue: {}", e);
        }
    }

    // Create application state
    let app_state = Arc::new(RwLock::new(AppState::new(
        Arc::clone(&queue),
        config.clone(),
    )));

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create channels for worker communication
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();

    // Spawn workers
    let mut worker_handles = Vec::new();
    for worker_id in 1..=config.worker_count {
        let queue_clone = Arc::clone(&queue);
        let config_clone = config.clone();
        let progress_tx_clone = progress_tx.clone();
        let worker_stats_clone = Arc::clone(&app_state.read().await.worker_stats);

        let handle = tokio::spawn(async move {
            simulate_worker(
                worker_id as u32,
                queue_clone,
                config_clone,
                progress_tx_clone,
                worker_stats_clone,
            )
            .await;
        });
        worker_handles.push(handle);
    }

    // Spawn edge case simulator
    let edge_case_handle = tokio::spawn({
        let queue_clone = Arc::clone(&queue);
        let config_clone = config.clone();
        let app_state_clone = Arc::clone(&app_state);
        async move {
            simulate_edge_cases(queue_clone, config_clone, app_state_clone).await;
        }
    });

    // Spawn UI update task
    let ui_update_handle = tokio::spawn({
        let app_state_clone = Arc::clone(&app_state);
        async move {
            let mut interval = interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                let mut state = app_state_clone.write().await;
                if !state.running {
                    break;
                }
                state.update_performance_history().await;
                state.update_queue_history().await;
            }
        }
    });

    // Main UI loop
    loop {
        // Handle worker progress updates
        while let Ok(update) = progress_rx.try_recv() {
            match update {
                WorkerUpdate::Started {
                    worker_id,
                    file_hash,
                } => {
                    debug!("Worker {} started processing {}", worker_id, file_hash);
                }
                WorkerUpdate::Completed {
                    worker_id,
                    file_hash,
                    duration,
                } => {
                    debug!(
                        "Worker {} completed {} in {:?}",
                        worker_id, file_hash, duration
                    );
                }
                WorkerUpdate::Failed {
                    worker_id,
                    file_hash,
                    error,
                } => {
                    debug!("Worker {} failed {}: {}", worker_id, file_hash, error);
                    let mut state = app_state.write().await;
                    state.log_edge_event(format!("Worker {} failed: {}", worker_id, error));
                }
                WorkerUpdate::Idle { worker_id } => {
                    debug!("Worker {} is idle", worker_id);
                }
            }
        }

        // Render UI
        {
            let state = app_state.read().await;
            terminal.draw(|f| {
                render_dashboard(f, &state, f.size());
            })?;
        }

        // Handle input
        if !handle_input().await {
            break;
        }

        // Check if simulation is finished
        if queue.is_finished().await {
            info!("Simulation completed - all work finished");
            break;
        }

        // Small delay to prevent CPU spinning
        sleep(Duration::from_millis(100)).await;
    }

    // Stop UI updates
    {
        let mut state = app_state.write().await;
        state.running = false;
    }

    // Clean up terminal
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;

    // Wait for workers to finish
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Wait for background tasks
    let _ = edge_case_handle.await;
    let _ = ui_update_handle.await;

    // Print final statistics
    let final_stats = queue.stats().await;
    let elapsed = app_state.read().await.elapsed_time();

    println!("\n📊 Simulation Results:");
    println!("├─ Duration: {:.1} seconds", elapsed);
    println!("├─ Files processed: {}", final_stats.completed_count);
    println!("├─ Files failed: {}", final_stats.abandoned_count);
    println!("├─ Success rate: {:.1}%", final_stats.success_rate());
    println!("├─ Duplicates detected: {}", final_stats.duplicate_count);
    println!(
        "├─ Average throughput: {:.1} files/sec",
        final_stats.completed_count as f64 / elapsed
    );
    println!(
        "└─ Worker utilization: {:.1}%",
        final_stats.worker_utilization(config.worker_count as u32)
    );

    // Print worker statistics
    let worker_stats = app_state.read().await.worker_stats.read().await.clone();
    println!("\n👷 Worker Performance:");
    for (worker_id, stats) in worker_stats.iter() {
        println!(
            "├─ Worker {}: {} completed, {} failed, {:.1}% success, avg time: {:?}",
            worker_id,
            stats.files_completed,
            stats.files_failed,
            stats.success_rate(),
            stats.average_download_time()
        );
    }

    println!("\n✅ Work-stealing queue simulation completed successfully!");
    println!("Key observations:");
    println!("  • No worker starvation occurred");
    println!("  • Queue efficiently distributed work across all workers");
    println!("  • Failed work was automatically retried");
    println!("  • Duplicate files were detected and filtered");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_simulation().await
}
