# Feature: MIDAS Open weather data download library

This is a Rust library and command line app for downloading weather data from the UK Met Office MIDAS Open dataset. It will adhere strictly to the development principles outlined in `CLAUDE.md`.

This document describes the architecture for a Rust command-line application that downloads data files from the CEDA website based on a manifest file. The system handles concurrent downloads with multiple workers while preventing duplicate downloads and ensuring data integrity through atomic file operations.

The application is designed with clean separation of concerns to facilitate future adaptation as a library for a Tauri GUI application, but is implemented as a single binary for simplicity.

## Problem Statement

The application needs to:
- Parse a manifest file containing file metadata (hashes, paths, dataset information)
- Download files from CEDA with authentication and rate limiting
- Store files in a local cache with proper organization
- Handle multiple concurrent workers without downloading the same file multiple times
- Ensure atomic file operations to prevent corruption
- Respect CEDA's server constraints with proper backoff strategies

## Source data description

The MIDAS Open dataset is described [here](https://help.ceda.ac.uk/article/4982-midas-open-user-guide). You can see how the data is structured into datasets, years, counties, quality levels, stations, data and metadata in the section "How are the data structured?" on that webpage.

The CEDA website provides a manifest file of all downloadable datafiles, an example of which is /examples/midas-open-v202407-md5s.txt.

Downloading datafiles from the website requires authentication - the use must have an account on the website with a username and password.

## Core Architectural Principles

### Distributed Consensus at the Filesystem Level

The fundamental challenge is that the filesystem acts as shared mutable state accessed by multiple async workers. Without coordination, workers checking for file existence and then downloading create race conditions (Time-of-Check to Time-of-Use problems).

The solution treats the filesystem as a distributed system requiring explicit coordination:
- An in-memory reservation system provides the transactional semantics that the filesystem lacks
- Workers must acquire exclusive download rights through atomic operations
- The system maintains shared state tracking file statuses (downloading, completed, available)

### Single Responsibility Architecture

Each component has one clear purpose:
- **FileInfo**: Pure data structure holding manifest information
- **ManifestParser**: Transforms manifest files into streams of FileInfo
- **CedaClient**: Handles authenticated HTTP downloads from CEDA
- **CacheStore**: Manages filesystem operations and reservation system
- **DownloadWorker**: Processes individual files using other components
- **Coordinator**: Orchestrates workers and manages the overall flow

## Component Design

### 1. Data Model (FileInfo)

A simple struct containing:
- File hash (for verification and deduplication)
- Dataset name, version, and quality version
- County and station ID
- Filename and relative path

This is pure data with no methods or business logic.

### 2. Manifest Processing

The manifest parser:
- Reads manifest files line by line
- Parses entries with format: `<hash> <path>`
- Filters based on dataset name, version, and quality criteria
- Yields a stream of FileInfo objects
- Handles malformed lines gracefully without stopping processing

### 3. Robust CEDA Client

The CEDA client must be a responsible citizen of the CEDA infrastructure, implementing proper authentication, rate limiting, retry logic, and backoff strategies. This component encapsulates all the complexity of interacting with CEDA's servers while protecting both your application and their infrastructure.

#### Core Responsibilities

The CEDA client manages several interconnected concerns:
- **Authentication**: Maintains API key or session-based authentication with automatic refresh
- **Rate Limiting**: Enforces request limits using a token bucket or semaphore system
- **Retry Logic**: Implements exponential backoff with jitter for failed requests
- **Circuit Breaking**: Temporarily stops requests when error rates exceed thresholds
- **Connection Pooling**: Reuses HTTP connections efficiently
- **Timeout Management**: Different timeouts for connection vs. download phases
- **Error Classification**: Distinguishes between retryable and permanent failures

#### Rate Limiting Design

The client implements a multi-layer approach to rate limiting:

1. **Global Rate Limiter**: Semaphore-based limiter controlling total concurrent requests
2. **Per-Host Rate Limiter**: Ensures compliance with CEDA's specific limits
3. **Adaptive Throttling**: Automatically reduces request rate when detecting server strain
4. **Minimum Request Spacing**: Enforces minimum delays between consecutive requests

#### Backoff Strategy

The exponential backoff implementation should:
- Start with a base delay (e.g., 100ms)
- Double the delay on each consecutive failure (up to a maximum)
- Add random jitter to prevent thundering herd problems
- Track consecutive errors across all workers (shared state)
- Reset on successful requests
- Implement circuit breaker pattern for repeated failures

#### Error Handling

The client classifies errors into categories:
- **Retryable**: Network timeouts, 503 Service Unavailable, 429 Too Many Requests
- **Possibly Retryable**: 500 Internal Server Error (retry with longer backoff)
- **Non-Retryable**: 401 Unauthorized, 404 Not Found, 400 Bad Request
- **Circuit Break Triggers**: Repeated 503s or timeouts indicating server issues

#### Health Monitoring

The client maintains metrics for:
- Request success/failure rates
- Response times (P50, P95, P99)
- Current backoff state
- Active request count
- Circuit breaker status

This information helps tune performance and detect issues early.

### 4. Cache Management with Reservation System

The cache store combines two responsibilities:
- **Filesystem operations**: Atomic file saving, directory creation
- **Coordination**: Reservation system to prevent duplicate downloads

#### Reservation System Design

The reservation system maintains an in-memory map of file status:
- **Downloading**: Worker ID, start time, and notification handle
- **Completed**: File successfully downloaded and saved

Key operations:
- `try_reserve()`: Atomic check-and-claim for download rights
- `mark_completed()`: Update status and notify waiting workers
- `mark_failed()`: Release reservation for retry

#### Cache Store Operations

1. **check_and_reserve()**: The critical entry point
   - Checks if file exists on disk
   - Verifies file integrity if present
   - Attempts to reserve if download needed
   - Returns one of three states: AlreadyExists, NeedToDownload, or SomeoneElseDownloading

2. **save_file()**: Atomic file writing
   - Creates directory structure
   - Writes to temporary file first
   - Performs atomic rename to final location
   - Updates reservation system on success

3. **mark_download_failed()**: Cleanup on failure
   - Releases reservation
   - Allows another worker to retry

### 5. Download Workers and Work Distribution

#### Critical Design Issue: Avoiding Worker Starvation

A naive implementation of the reservation system could lead to severe performance degradation. Consider this flawed approach:

**The Problem**: If Worker A encounters a file that Worker B is already downloading, Worker A might wait for that specific file to complete. This creates a situation where workers sleep instead of finding other available work. In the worst case with many duplicate files, most workers could end up sleeping while only one remains active, reducing your parallel system to sequential performance.

**The Solution**: Workers must never wait for specific files. Instead, they should continuously seek available work. When a worker encounters a file that's already being handled, it should immediately move on to the next available task.

#### Work-Stealing Architecture

The system implements a work-stealing pattern where workers actively seek available tasks rather than being assigned fixed work:

**Work Queue Design**: Instead of a simple channel, the system uses a stateful work queue that tracks:
- Pending work (not yet started)
- In-progress work (being downloaded by a worker)
- Completed work (successfully downloaded or permanently failed)

**Worker Behavior**: Each worker follows this algorithm:
1. Request next available work from the queue
2. If work is available and not already being processed, claim it
3. If no work is available, brief sleep then retry
4. Never wait for a specific file to complete

This ensures all workers remain active as long as unclaimed work exists in the queue.

#### Handling Duplicate Files

The system handles duplicate files (same hash, different destinations) efficiently:

**At Parse Time**: The manifest parser identifies files with identical hashes and groups them into a single download task with multiple destination paths.

**During Download**: Each unique file is downloaded exactly once. The cache manager handles copying or linking to multiple destinations after successful download.

**Benefits**: This approach prevents redundant downloads while keeping all workers actively processing unique files.

#### Work Queue Implementation Details

The work queue is the heart of the work-stealing architecture. Unlike a simple channel where messages are consumed in order, this queue maintains sophisticated state to enable efficient work distribution:

**Queue State Management**:
```
WorkQueue {
    pending: VecDeque<DownloadTask>      // Tasks not yet claimed
    in_progress: HashMap<Hash, WorkInfo>  // Tasks currently being processed
    completed: HashSet<Hash>              // Tasks finished (success or permanent failure)
    failed_retry: VecDeque<DownloadTask>  // Tasks to retry after temporary failures
}

WorkInfo {
    worker_id: usize
    started_at: Instant
    task: DownloadTask
}
```

**Key Operations**:

1. **get_next_available_work()**: Atomically finds and claims the next task
   - Skips tasks already in-progress or completed
   - Moves task from pending to in-progress
   - Returns None only when no unclaimed work exists

2. **mark_complete()**: Updates task status after processing
   - Removes from in-progress
   - Adds to completed set
   - Handles destination copying for duplicate files

3. **handle_worker_failure()**: Manages worker crashes
   - Detects stale in-progress entries (timeout-based)
   - Returns tasks to pending queue for retry
   - Prevents permanent task loss

**Critical Insight**: The queue acts as a central coordinator that maintains a global view of work status. This is more complex than a simple channel but prevents the worker starvation problem while ensuring each file is downloaded exactly once.

#### Example Scenario: Avoiding Worker Starvation

Consider a manifest with 1000 files where files 1-100 are unique and files 101-1000 are all duplicates of file 1:

**With Naive Waiting Approach**:
- Worker 1: Downloads file 1
- Workers 2-4: Get files 101-104, see they're duplicates of file 1, go to sleep
- Result: Only Worker 1 remains active, 75% performance loss

**With Work-Stealing Approach**:
- Worker 1: Downloads file 1
- Worker 2: Tries file 101, sees it's handled, immediately tries file 2
- Worker 3: Tries file 102, sees it's handled, immediately tries file 3
- Worker 4: Tries file 103, sees it's handled, immediately tries file 4
- Result: All workers remain active, maximum throughput maintained

The work-stealing approach ensures that duplicate detection never reduces worker utilization. Workers only sleep when the pending queue is truly empty.

## Performance Implications of Work-Stealing

The choice between workers waiting versus work-stealing has dramatic performance implications that compound with scale:

**Performance Comparison**: Consider downloading 10,000 files with 20% duplicates using 8 workers:

With Waiting Architecture:
- Average active workers: 3-4 (others sleeping on duplicates)
- Throughput reduction: 50-60%
- Total time: 2-3x longer than optimal

With Work-Stealing Architecture:
- Average active workers: 8 (until queue exhausted)
- Throughput: Maximum possible
- Total time: Optimal based on network/disk speed

**Real-World Impact**: For a 2-hour download job, the waiting approach could extend this to 4-6 hours, while work-stealing maintains the 2-hour target. This difference becomes critical for large datasets or time-sensitive operations.

The work-stealing architecture ensures that system performance scales linearly with worker count, rather than degrading based on data patterns.

## Error Handling Strategy

The system handles several failure modes:

1. **Download failures**: Release reservation for retry
2. **Filesystem errors**: Propagate with context
3. **Corrupted files**: Delete and re-download
4. **Worker crashes**: Could implement reservation timeouts
5. **Partial writes**: Prevented by atomic temp-file pattern

## Implementation Considerations

### Work-Stealing as a Core Principle

The work-stealing architecture is not just an optimization but a fundamental design principle that affects every component. When implementing this system, keep these critical points in mind:

**Avoid Hidden Waiting**: Any code path where a worker waits for a specific resource (rather than seeking alternative work) is a potential performance bottleneck. This includes waiting for locks, I/O operations, or other workers. Always structure operations so workers can move on to other tasks.

**Queue Over Channel**: While Rust's channels are excellent for many use cases, this architecture specifically requires a stateful queue that can answer "what work is available?" rather than just "what work is next?" This distinction is crucial for preventing duplicate processing while maintaining high worker utilization.

**Atomic State Transitions**: The work queue must handle state transitions atomically to prevent race conditions. When a worker claims a task, the transition from 'pending' to 'in-progress' must be atomic to prevent multiple workers from claiming the same task.

### Atomicity Guarantees

- File writes use temp files with atomic rename
- Work queue operations use Mutex for thread-safe updates
- State transitions are atomic to prevent duplicate claims
- Directory creation uses create_dir_all (idempotent)

### Scalability

- Work-stealing ensures linear scaling with worker count
- Bounded memory usage through streaming processing
- Queue size limits prevent memory exhaustion
- Natural load balancing without complex distribution logic

### Monitoring and Debugging

- Track queue depths to identify bottlenecks
- Monitor worker utilization rates
- Log task state transitions for debugging
- Measure time tasks spend in each state

## Code Organization for Future Library Use

While this application is built as a single command-line binary, the code should be organized with clear module boundaries to facilitate future extraction into a library for use with a Tauri GUI application. This approach provides simplicity now while maintaining flexibility for the future.

### Recommended Project Structure

```
src/
├── main.rs              # CLI entry point - kept thin
├── lib.rs              # Optional: exposes public API for future library use
├── app/                # Application logic (future library code)
│   ├── mod.rs
│   ├── models.rs       # FileInfo and other data structures
│   ├── manifest.rs     # Manifest parsing
│   ├── client.rs       # CEDA HTTP client
│   ├── cache.rs        # Cache management
│   ├── queue.rs        # Work queue implementation
│   ├── worker.rs       # Download worker logic
│   └── coordinator.rs  # Orchestration
├── cli/                # CLI-specific code
│   ├── mod.rs
│   ├── args.rs         # Command-line argument parsing
│   └── progress.rs     # Terminal progress display
└── config.rs           # Configuration structures

```

### Design Principles for Future Library Extraction

1. **Keep main.rs minimal**: The main function should primarily handle CLI argument parsing and call into the app module. This makes it easy to later move the app module into a library crate.

2. **Avoid CLI assumptions in app code**: The core application logic shouldn't assume it's running in a terminal. Progress updates should use channels or callbacks rather than directly printing to stdout.

3. **Configuration as data**: Pass configuration as Rust structs rather than reading files directly in the app code. The CLI layer can handle file reading and construct these structs.

4. **Error types that work everywhere**: Design error types that make sense in both CLI and GUI contexts, avoiding terminal-specific error formatting in the core logic.

### Example Code Organization

The main.rs file remains simple:
```rust
// main.rs
mod app;
mod cli;
mod config;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments
    let args = cli::Args::parse();

    // Load configuration
    let config = config::load_from_file(&args.config_file)?;

    // Create coordinator with progress callback
    let (progress_tx, progress_rx) = tokio::sync::mpsc::channel(100);
    let coordinator = app::Coordinator::new(config, progress_tx);

    // Start progress display
    let progress_handle = tokio::spawn(cli::display_progress(progress_rx));

    // Run the download
    coordinator.run(args.manifest).await?;

    // Wait for progress display to finish
    progress_handle.await?;

    Ok(())
}
```

The app module contains all the core logic without CLI dependencies:
```rust
// app/coordinator.rs
pub struct Coordinator {
    config: Config,
    progress_tx: Option<Sender<ProgressEvent>>,
}

// Progress events that work for any interface
pub enum ProgressEvent {
    Started { total_files: usize },
    FileCompleted { hash: String },
    // ... etc
}
```

### Future Library Extraction

When ready to create a Tauri app, the process would be:
1. Move the `app` module to a new library crate
2. Update imports in the CLI binary to use the library
3. The Tauri app can use the same library with its own progress handling

This approach provides a clear path forward without the complexity of managing multiple crates during initial development.

## Performance Tuning and Configuration

The system provides extensive configuration options to optimize performance for different scenarios, network conditions, and CEDA server constraints. These settings allow you to balance download speed, resource usage, and server friendliness.

### Core Performance Parameters

#### Worker Pool Configuration
- **worker_count**: Number of concurrent download workers (default: 4)
  - Higher values increase parallelism but may overwhelm CEDA servers
  - Consider CPU cores and network bandwidth when tuning
  - Monitor error rates when increasing

- **worker_restart_on_failure**: Whether to restart failed workers (default: true)
- **worker_restart_delay_ms**: Delay before restarting a failed worker (default: 5000)

#### Batching and Queue Management
- **manifest_batch_size**: Number of files to parse before yielding (default: 100)
  - Larger batches reduce overhead but increase memory usage
  - Smaller batches improve responsiveness and memory efficiency

- **channel_buffer_size**: Size of the channel between manifest parser and workers (default: 100)
  - Controls backpressure and memory usage
  - Too small causes parser to block frequently
  - Too large may cause memory issues with large manifests

- **prefetch_count**: Number of files to check in cache ahead of processing (default: 50)
  - Improves efficiency by overlapping cache checks with downloads

#### CEDA Client Tuning
- **max_concurrent_requests**: Maximum simultaneous requests to CEDA (default: 3)
  - Independent of worker count to provide fine-grained control
  - CEDA may have specific limits documented in their API guidelines

- **request_timeout_seconds**: Timeout for individual requests (default: 300)
- **connection_timeout_seconds**: Timeout for establishing connections (default: 30)
- **min_request_spacing_ms**: Minimum time between requests (default: 100)
- **max_retries**: Maximum retry attempts per file (default: 3)
- **base_backoff_ms**: Starting backoff delay (default: 1000)
- **max_backoff_seconds**: Maximum backoff delay (default: 300)
- **backoff_multiplier**: Exponential backoff multiplier (default: 2.0)
- **backoff_jitter_factor**: Random jitter to add (0.0-1.0, default: 0.1)

#### Circuit Breaker Configuration
- **circuit_breaker_enabled**: Enable circuit breaker pattern (default: true)
- **circuit_breaker_failure_threshold**: Failures before opening circuit (default: 5)
- **circuit_breaker_success_threshold**: Successes needed to close circuit (default: 2)
- **circuit_breaker_timeout_seconds**: Time before attempting reset (default: 60)

#### Cache Performance
- **cache_verification_enabled**: Whether to verify file hashes (default: true)
- **cache_verification_sample_rate**: Percentage of files to verify (0.0-1.0, default: 1.0)
- **parallel_directory_creation**: Create directories concurrently (default: true)
- **temp_file_directory**: Location for temporary files during download
  - Place on same filesystem as cache for atomic renames
  - Consider using fast SSD storage

#### Memory Management
- **max_file_size_in_memory**: Files larger than this use streaming (default: 100MB)
- **download_chunk_size**: Size of chunks when streaming large files (default: 8KB)
- **max_memory_percent**: Maximum percentage of system memory to use (default: 50)

### Performance Profiles

The system should support predefined profiles for common scenarios:

#### Conservative Profile
Prioritizes server friendliness and stability:
- worker_count: 2
- max_concurrent_requests: 1
- min_request_spacing_ms: 500
- max_retries: 5
- circuit_breaker_failure_threshold: 3

#### Balanced Profile (Default)
Balances performance and reliability:
- worker_count: 4
- max_concurrent_requests: 3
- min_request_spacing_ms: 100
- max_retries: 3
- circuit_breaker_failure_threshold: 5

#### Aggressive Profile
Maximizes download speed (use only with permission):
- worker_count: 8
- max_concurrent_requests: 6
- min_request_spacing_ms: 50
- max_retries: 2
- circuit_breaker_failure_threshold: 10

#### Development Profile
Optimized for testing and debugging:
- worker_count: 1
- max_concurrent_requests: 1
- detailed_logging: true
- cache_verification_enabled: true
- cache_verification_sample_rate: 1.0

### Dynamic Performance Adjustment

The system should support runtime performance adjustments:

1. **Adaptive Rate Limiting**: Automatically reduce request rate when detecting:
   - Increased error rates (>10% failures)
   - Response time degradation (>2x baseline)
   - HTTP 429 (Too Many Requests) responses

2. **Load-Based Tuning**: Adjust worker count based on:
   - System CPU usage
   - Network bandwidth utilization
   - CEDA server response times

3. **Time-Based Profiles**: Different settings for different times:
   - Business hours: Conservative settings
   - Night/weekend: More aggressive settings
   - Maintenance windows: Pause or minimal activity

### Monitoring and Metrics

Performance tuning requires visibility into system behavior:

#### Key Metrics to Track
- Downloads per second
- Average download time
- Cache hit rate
- Error rate by type
- Worker utilization
- Queue depths
- Memory usage
- Network bandwidth usage

#### Performance Logging
- Log performance metrics every N seconds
- Include rolling averages for trend detection
- Alert on performance degradation
- Export metrics in standard formats (Prometheus, StatsD)

### Configuration Format

Configuration should use a hierarchical format (TOML recommended):

```toml
[performance]
profile = "balanced"  # or "conservative", "aggressive", "custom"

[performance.workers]
count = 4
restart_on_failure = true
restart_delay_ms = 5000

[performance.batching]
manifest_batch_size = 100
channel_buffer_size = 100
prefetch_count = 50

[performance.ceda_client]
max_concurrent_requests = 3
request_timeout_seconds = 300
min_request_spacing_ms = 100

[performance.ceda_client.backoff]
max_retries = 3
base_backoff_ms = 1000
max_backoff_seconds = 300
multiplier = 2.0
jitter_factor = 0.1

[performance.circuit_breaker]
enabled = true
failure_threshold = 5
success_threshold = 2
timeout_seconds = 60

[performance.cache]
verification_enabled = true
verification_sample_rate = 1.0
parallel_directory_creation = true

[performance.memory]
max_file_size_in_memory = 104857600  # 100MB
download_chunk_size = 8192  # 8KB
max_memory_percent = 50
```

## Progress Monitoring and User Feedback

For long-running downloads spanning hours, clear progress indication is essential. The monitoring system must handle the complexity of async operations while remaining simple and reliable. The design prioritizes accuracy and user experience over complex features.

### Design Principles for Progress Monitoring

The progress system follows these core principles to avoid common pitfalls:

1. **Single Source of Truth**: All progress updates flow through a single component to prevent race conditions
2. **Monotonic Progress**: Progress only moves forward, never backwards
3. **Bounded Updates**: Limit update frequency to prevent overwhelming the UI
4. **Graceful Degradation**: If progress tracking fails, downloads continue
5. **Simple State Model**: Track only essential metrics to minimize bugs

### Progress Tracking Architecture

#### Central Progress Tracker

A dedicated progress tracking component manages all state updates:

```
Workers → Progress Messages → Progress Tracker → Display
```

The tracker maintains these essential metrics:
- Total files to process (discovered during manifest parsing)
- Files completed successfully
- Files failed (with retry tracking)
- Files currently downloading
- Bytes downloaded (for bandwidth calculation)
- Start time and elapsed time

#### Progress Message Types

Workers send simple, typed messages to avoid ambiguity:
- `FileQueued { hash, size_estimate }` - File added to processing queue
- `DownloadStarted { hash, worker_id }` - Worker began downloading
- `DownloadProgress { hash, bytes_so_far }` - Periodic progress updates
- `DownloadCompleted { hash, total_bytes, duration }` - Success
- `DownloadFailed { hash, error, will_retry }` - Failure with retry intent
- `DownloadAbandoned { hash, error }` - Permanent failure

This explicit messaging prevents the common bug of incrementing counters incorrectly or losing track of in-flight operations.

#### Display Strategy

The progress display uses a multi-line format that provides essential information without overwhelming detail:

```
Dataset: uk-daily-temperature-obs (2024-07)
Files: 1,234 / 5,678 completed (308 failed, 4 downloading)
Progress: ████████████░░░░░░░░ 21.7%
Speed: 2.4 MB/s | Downloaded: 156 MB / 720 MB
Time: 00:24:15 elapsed | ETA: 01:23:45 remaining
Status: Downloading temperature_2024_devon.csv...
```

Key design decisions:
- Fixed layout prevents terminal flicker
- Show both count and size progress (files can vary greatly in size)
- Display current operation for user assurance
- Include failure count to indicate if intervention needed

### ETA Calculation

Estimating time remaining requires careful consideration of download patterns:

#### Adaptive ETA Algorithm

Rather than simple linear extrapolation, the system uses:

1. **Rolling Window Average**: Calculate speed over last 60 seconds to smooth fluctuations
2. **File-Based Estimation**: Track average time per file rather than just bytes
3. **Weighted Calculation**: Combine both file count and byte size estimates
4. **Confidence Indication**: Show ETA confidence based on variance

The formula balances recent performance with historical average:
```
eta = (remaining_bytes / rolling_avg_speed) * 0.7 +
      (remaining_files * avg_file_time) * 0.3
```

This handles the common case where early files are smaller (metadata) and later files are larger (data).

#### Handling Edge Cases

The ETA calculator handles special scenarios:
- First few minutes: Show "Calculating..." until sufficient data
- Network stalls: Increase window size to prevent wild swings
- Mass failures: Exclude failed files from speed calculations
- Near completion: Switch to more conservative estimates

### Implementation Considerations

#### Update Frequency

Progress updates follow a batching strategy:
- Collect updates for 100ms before refreshing display
- Limit terminal updates to 10Hz maximum
- Batch multiple worker updates into single display refresh
- Use atomic operations for counter updates

#### Memory Efficiency

For hour-long operations, memory usage must be considered:
- Don't store full history of all operations
- Use circular buffers for rolling averages
- Track only active downloads in detail
- Summarize completed files into aggregate statistics

#### Error Resilience

Progress tracking must not interfere with core functionality:
- Use try-send for progress messages (don't block on full channels)
- Separate progress tracking errors from download errors
- Continue operation if display fails
- Log progress to file for post-mortem analysis

### Progress Persistence

For very long operations, consider progress persistence:
- Periodically save progress state to disk
- Allow resuming with accurate progress display
- Include in crash recovery mechanisms
- Store minimal state (file hashes and status only)

### Terminal UI Library Choice

For the progress display, use a library that:
- Handles terminal resize gracefully
- Works over SSH connections
- Supports Unicode progress bars
- Doesn't require complex dependencies

Recommended: `indicatif` for Rust, which provides:
- Multiple progress bars for concurrent operations
- Built-in ETA calculations
- Smooth updates without flicker
- Template-based formatting

### Monitoring Integration

Progress data should be available to external monitoring:
- Export metrics in Prometheus format
- Provide JSON endpoint for current status
- Log progress milestones to structured logs
- Support headless operation with file-based progress

### User Experience Guidelines

The progress system should:
- Show immediate feedback when starting
- Indicate when workers are warming up
- Clearly show when complete
- Summarize results (success/failure counts)
- Provide actionable next steps for failures

Example completion summary:
```
✓ Download completed in 1h 24m 35s

Summary:
- Successfully downloaded: 5,370 files (698 MB)
- Failed downloads: 308 files
- Cache hits: 1,455 files (skipped)
- Average speed: 2.1 MB/s

Failed files have been logged to: ./download-errors-2024-01-15.log
To retry failed downloads, run: ./downloader --retry-failed
```



## Other notes:

Examples from another app are provided in /examples of a CEDA client (client.rs.ex) and how to authorise (auth.rs.ex). These are for guidance - feel free to refactor as requried.
There is a file .env in root with my authm credentials. You will develope a CLI command `auth` to obtain and verify these for a user.
