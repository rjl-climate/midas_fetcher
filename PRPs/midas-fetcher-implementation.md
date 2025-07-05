# PRP: MIDAS Fetcher - Complete Implementation

## Goal
Implement a complete Rust library and command-line application for downloading weather data from the UK Met Office MIDAS Open dataset. The system must handle concurrent downloads with multiple workers, prevent duplicate downloads, ensure data integrity through atomic file operations, and withstand hostile academic scrutiny through rigorous methodology.

## Why
- **Research Value**: Enable researchers to efficiently download massive volumes of UK weather data (100GB+ datasets)
- **Academic Integrity**: Provide reproducible, verified downloads for climate research that can withstand peer review
- **Server Responsibility**: Respectful interaction with CEDA servers through proper rate limiting and backoff strategies
- **Performance**: Transform 20+ minute manual processes into sub-second operations through manifest-based verification

## What
A sophisticated work-stealing concurrent download system with:
- Authenticated CEDA client with proper session management
- Work-stealing task queue preventing worker starvation
- Atomic file operations with reservation system
- Comprehensive progress monitoring and ETA calculations
- Manifest-based verification system for instant cache validation
- Command-line interface with authentication management
- Future-ready library structure for Tauri GUI integration

### Success Criteria
- [ ] Successfully authenticate with CEDA archive using session cookies
- [ ] Download 1000+ files concurrently without server errors (429/503 responses)
- [ ] Achieve linear performance scaling with worker count (no worker starvation)
- [ ] Handle interruptions gracefully with resumable downloads
- [ ] Verify cache integrity in <1 second using manifest system
- [ ] Zero data corruption through atomic file operations
- [ ] All tests pass: `cargo test --all`
- [ ] All lints pass: `cargo clippy --all -- -D warnings`
- [ ] Code formatted: `cargo fmt --all -- --check`

## All Needed Context

### Documentation & References
```yaml
# CRITICAL PATTERNS - Include in context window
- file: examples/client.rs.ex
  why: Complete HTTP client implementation with authentication, rate limiting, retry logic
  critical: Session-based auth pattern, exponential backoff, atomic temp file operations

- file: examples/auth.rs.ex
  why: Interactive credential management, .env file handling, verification workflows
  critical: CSRF token extraction, secure credential storage, validation patterns

- file: examples/midas-open-v202407-md5s.txt
  why: Manifest file format showing hash-based verification system
  critical: "hash ./path/to/file" format, nested directory structure

- url: https://patshaughnessy.net/2020/1/20/downloading-100000-files-using-async-rust
  why: Proven concurrent download patterns with 50 concurrent tasks downloading 100k files
  critical: buffer_unordered() for controlled concurrency, avoiding server overload

- url: https://docs.rs/governor/latest/governor/
  why: Rate limiting implementation using Generic Cell Rate Algorithm
  critical: until_ready_with_jitter() for avoiding thundering herd effects

- url: https://docs.rs/tokio/latest/tokio/
  why: Work-stealing runtime with local queues per worker thread
  critical: Fixed worker threads (one per core), local queue capacity of 256 tasks

- url: https://tokio.rs/tokio/tutorial/shared-state
  why: Shared state management patterns for reservation system
  critical: Arc<Mutex<T>> for shared state, avoiding locks across .await calls

- docfile: CLAUDE.md
  why: Non-negotiable development principles including TDD, error handling, code quality
  critical: Test-driven development, Result propagation, clippy enforcement

- docfile: PLANNING.md
  why: Complete architectural specification including work-stealing design
  critical: Reservation system, atomic operations, worker starvation prevention
```

### Current Codebase Structure
```bash
├── src/
│   └── main.rs              # "Hello, world!" placeholder
├── examples/
│   ├── client.rs.ex         # Complete HTTP client with auth & rate limiting
│   ├── auth.rs.ex           # Credential management & verification
│   └── midas-open-v202407-md5s.txt  # Manifest format example
├── Cargo.toml               # Minimal dependencies
├── CLAUDE.md                # Development principles (TDD, error handling)
├── INITIAL.md               # Complete architectural specification
└── README.md                # Feature documentation
```

### Desired Codebase Structure
```bash
src/
├── main.rs                  # CLI entry point - thin wrapper
├── lib.rs                   # Library API for future Tauri integration
├── constants.rs             # All constants (CEDA URLs, timeouts, etc.)
├── app/                     # Core application logic
│   ├── mod.rs
│   ├── models.rs            # FileInfo and data structures
│   ├── manifest.rs          # Manifest parsing and management
│   ├── client.rs            # CEDA HTTP client (from examples/client.rs.ex)
│   ├── cache.rs             # Cache management with reservation system
│   ├── queue.rs             # Work-stealing queue implementation
│   ├── worker.rs            # Download worker logic
│   └── coordinator.rs       # Main orchestration
├── cli/                     # CLI-specific code
│   ├── mod.rs
│   ├── args.rs              # Command-line argument parsing
│   └── progress.rs          # Terminal progress display
├── auth/                    # Authentication module (from examples/auth.rs.ex)
│   ├── mod.rs
│   └── credentials.rs       # Credential management
└── errors.rs                # Custom error types using thiserror
```

### Known Gotchas & Library Quirks
```rust
// CRITICAL: Governor rate limiter requires NonZeroU32 for quotas
let quota = Quota::per_second(NonZeroU32::new(5).expect("Rate limit must be non-zero"));

// CRITICAL: Reqwest client needs cookie_store(true) for CEDA session auth
let client = Client::builder().cookie_store(true).build()?;

// CRITICAL: Tokio file operations are NOT truly async - they spawn OS threads
// Use tokio::fs::File for consistency but understand performance implications

// CRITICAL: Avoid locks across .await points - will block entire event loop
// Use Arc<Mutex<T>> but release before any async operations

// CRITICAL: CEDA auth requires CSRF token extraction from HTML
// Use scraper crate with CSS selectors: input[name='csrfmiddlewaretoken']

// CRITICAL: Work-stealing queues prevent worker starvation
// Workers must never wait for specific files - always seek new work

// CRITICAL: Atomic file operations require temp file + rename pattern
// Write to .tmp extension, then rename to final location
```

## Implementation Blueprint

### Core Dependencies and Architecture
```rust
// Core async runtime and networking
tokio = { version = "1.0", features = ["full"] }
reqwest = { version = "0.11", features = ["json", "cookies"] }
url = "2.4"

// Rate limiting and backoff
governor = "0.6"
backoff = "0.4"

// HTML parsing for CSRF tokens
scraper = "0.18"

// CLI and progress
clap = { version = "4.0", features = ["derive"] }
indicatif = "0.17"

// Error handling and logging
thiserror = "1.0"
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"

// Serialization and credentials
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
rpassword = "7.0"
dotenv = "0.15"

// Testing
tempfile = "3.8"
tokio-test = "0.4"
```

## Task Implementation Order

### Task 1: Project Structure and Dependencies
```yaml
CREATE Cargo.toml:
  - ADD all required dependencies from blueprint
  - CONFIGURE workspace if needed

CREATE src/constants.rs:
  - DEFINE all CEDA URLs, timeouts, rate limits
  - MIRROR patterns from examples/client.rs.ex constants
  - USE pub const for all values

CREATE src/errors.rs:
  - DEFINE custom error types using thiserror
  - INCLUDE AuthError, DownloadError, ManifestError
  - MIRROR error patterns from examples
```

### Task 2: Authentication Module
```yaml
CREATE src/auth/mod.rs:
  - COPY patterns from examples/auth.rs.ex
  - ADAPT for proper module structure
  - PRESERVE all validation and security features

CREATE src/auth/credentials.rs:
  - IMPLEMENT credential storage and retrieval
  - INCLUDE interactive setup workflows
  - MAINTAIN security best practices (file permissions)
```

### Task 3: HTTP Client with Rate Limiting
```yaml
CREATE src/app/client.rs:
  - COPY base implementation from examples/client.rs.ex
  - INTEGRATE governor rate limiting
  - IMPLEMENT exponential backoff with jitter
  - INCLUDE circuit breaker pattern for server overload

IMPLEMENT authentication flow:
  - PARSE CSRF tokens from HTML using scraper
  - MANAGE session cookies automatically
  - VERIFY authentication with test downloads
```

### Task 4: Data Models and Manifest System
```yaml
CREATE src/app/models.rs:
  - DEFINE FileInfo struct with hash, path, destination
  - IMPLEMENT From trait for manifest parsing
  - INCLUDE metadata for retry counts, timestamps

CREATE src/app/manifest.rs:
  - PARSE manifest format: "hash ./path/to/file"
  - IMPLEMENT streaming parser for large manifests
  - INCLUDE manifest verification and updating
```

### Task 5: Work-Stealing Queue System
```yaml
CREATE src/app/queue.rs:
  - IMPLEMENT work-stealing queue with VecDeque + HashMap
  - TRACK pending, in_progress, completed states
  - PREVENT worker starvation through continuous work-seeking
  - HANDLE duplicate file detection and merging

CRITICAL algorithm:
  - Workers request next_available_work() atomically
  - Skip already claimed or completed files
  - Never wait for specific files - always seek alternatives
  - Maintain global view of work status
```

### Task 6: Cache Management with Reservation System
```yaml
CREATE src/app/cache.rs:
  - IMPLEMENT reservation system with Arc<Mutex<HashMap>>
  - TRACK file status: downloading, completed, failed
  - PROVIDE atomic check_and_reserve() operation
  - HANDLE atomic file operations with temp files

IMPLEMENT cache verification:
  - FAST manifest-based checking (compare hashes)
  - FULL verification with actual CEDA downloads
  - CORRUPT file detection and automatic retry
```

### Task 7: Download Workers
```yaml
CREATE src/app/worker.rs:
  - IMPLEMENT worker loop with work-stealing pattern
  - INTEGRATE with client for rate-limited downloads
  - HANDLE all error cases with proper retry logic
  - REPORT progress through channels

WORKER algorithm:
  1. Request work from queue
  2. If no work available, brief sleep then retry
  3. If work available, attempt reservation
  4. If reservation fails, immediately seek new work
  5. If reservation succeeds, download and report progress
```

### Task 8: Orchestration and Progress
```yaml
CREATE src/app/coordinator.rs:
  - SPAWN worker pool with configurable concurrency
  - COLLECT progress reports from workers
  - HANDLE shutdown and cleanup
  - PROVIDE status reporting for CLI

CREATE src/cli/progress.rs:
  - IMPLEMENT real-time progress display using indicatif
  - CALCULATE ETA using rolling window averages
  - HANDLE terminal resize and cleanup
```

### Task 9: CLI Interface
```yaml
CREATE src/cli/args.rs:
  - DEFINE command structure using clap derive
  - INCLUDE download, auth, cache, optimize subcommands
  - VALIDATE arguments and provide helpful errors

CREATE src/main.rs:
  - IMPLEMENT thin CLI wrapper
  - HANDLE argument parsing and command dispatch
  - INTEGRATE with coordinator for actual work
  - PROVIDE proper error reporting to stderr
```

### Task 10: Library Integration
```yaml
CREATE src/lib.rs:
  - EXPOSE public API for future Tauri integration
  - RE-EXPORT key types and functions
  - DOCUMENT usage patterns and examples
  - ENSURE no CLI dependencies leak through
```

## Critical Implementation Details

### Work-Stealing Queue Pseudocode
```rust
// CRITICAL: This prevents worker starvation
impl WorkQueue {
    // Atomic operation to claim next work
    async fn get_next_available_work(&self) -> Option<DownloadTask> {
        let mut state = self.state.lock().await;

        // Try pending work first
        if let Some(task) = state.pending.pop_front() {
            state.in_progress.insert(task.hash.clone(), WorkInfo::new(task.clone()));
            return Some(task);
        }

        // Try failed work that's ready for retry
        if let Some(task) = state.failed_retry.pop_front() {
            state.in_progress.insert(task.hash.clone(), WorkInfo::new(task.clone()));
            return Some(task);
        }

        None // No work available
    }

    // NEVER wait for specific files - always seek alternatives
    fn mark_complete(&self, hash: &str, success: bool) {
        let mut state = self.state.lock().await;
        state.in_progress.remove(hash);

        if success {
            state.completed.insert(hash.to_string());
        } else {
            // Add back to retry queue if retries available
            if let Some(task) = self.get_failed_task(hash) {
                state.failed_retry.push_back(task);
            }
        }
    }
}
```

### Atomic File Operations Pattern
```rust
// CRITICAL: Prevents corruption from interruptions
async fn save_file_atomically(content: &[u8], dest: &Path) -> Result<(), DownloadError> {
    let temp_path = dest.with_extension(format!("{}tmp",
        dest.extension().unwrap_or_default().to_string_lossy()));

    // Write to temp file first
    tokio::fs::write(&temp_path, content).await?;

    // Atomic rename to final location
    tokio::fs::rename(&temp_path, dest).await?;

    Ok(())
}
```

### Rate Limiting Integration
```rust
// CRITICAL: Prevents server overload while maintaining throughput
impl CedaClient {
    async fn download_with_rate_limit(&self, url: &str) -> Result<Response, DownloadError> {
        // Apply rate limiting before request
        self.rate_limiter.until_ready_with_jitter(
            governor::Jitter::up_to(Duration::from_millis(100))
        ).await;

        // Implement exponential backoff on failures
        let mut retries = 0;
        loop {
            match self.client.get(url).send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) if response.status() == 429 => {
                    // Server asking us to slow down - increase backoff
                    let delay = Duration::from_millis(1000 * 2_u64.pow(retries));
                    tokio::time::sleep(delay).await;
                    retries += 1;
                }
                Err(e) if retries < 3 => {
                    let delay = Duration::from_millis(100 * 2_u64.pow(retries));
                    tokio::time::sleep(delay).await;
                    retries += 1;
                }
                Err(e) => return Err(DownloadError::Http(e)),
            }
        }
    }
}
```

## Validation Loops

### Level 1: Syntax & Style
```bash
# CRITICAL: Run after every major change
cargo fmt --all                                    # Format code
cargo clippy --all -- -D warnings                 # All warnings as errors
cargo check --all                                 # Basic compilation check

# Expected: No errors. Fix any issues before proceeding.
```

### Level 2: Unit Tests (TDD Required)
```rust
// CREATE comprehensive tests for each module
#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn test_work_queue_no_starvation() {
        let queue = WorkQueue::new();
        // Add 100 duplicate files to test worker starvation prevention
        // Verify all workers remain active
    }

    #[tokio::test]
    async fn test_atomic_file_operations() {
        // Test interruption during download doesn't corrupt files
        // Verify temp file cleanup on failure
    }

    #[tokio::test]
    async fn test_rate_limiting_backoff() {
        // Test exponential backoff with jitter
        // Verify circuit breaker on repeated failures
    }

    #[test]
    fn test_manifest_parsing() {
        // Test parsing of manifest format
        // Verify error handling for malformed lines
    }
}
```

```bash
# Run tests and iterate until all pass
cargo test --all -- --nocapture
# If failing: Read error, fix code, re-run (never skip tests)
```

### Level 3: Integration Tests
```bash
# Test authentication flow
cargo run -- auth setup    # Follow interactive prompts
cargo run -- auth verify   # Should succeed with real credentials

# Test download functionality
cargo run -- download --dataset uk-daily-temperature-obs --limit 10
# Expected: 10 files downloaded successfully with progress indication

# Test cache verification
cargo run -- cache verify --fast
# Expected: Sub-second verification of downloaded files
```

## Final Validation Checklist
- [ ] All tests pass: `cargo test --all`
- [ ] No linting errors: `cargo clippy --all -- -D warnings`
- [ ] Code formatted: `cargo fmt --all -- --check`
- [ ] Authentication works: `cargo run -- auth verify`
- [ ] Download 100 files successfully: `cargo run -- download --limit 100`
- [ ] Cache verification in <1 second: `cargo run -- cache verify --fast`
- [ ] Progress bars work correctly during downloads
- [ ] Graceful shutdown on Ctrl+C
- [ ] Error messages are helpful and actionable
- [ ] All temporary files cleaned up after operations
- [ ] Performance scales linearly with worker count
- [ ] No worker starvation under high duplicate file scenarios

---

## Anti-Patterns to Avoid
- ❌ Don't use blocking operations in async contexts
- ❌ Don't hold locks across .await points
- ❌ Don't let workers wait for specific files (causes starvation)
- ❌ Don't skip rate limiting to "go faster" (will get blocked by CEDA)
- ❌ Don't use unwrap() or expect() in library code
- ❌ Don't create files without atomic operations (corruption risk)
- ❌ Don't ignore 429/503 HTTP status codes (server overload)
- ❌ Don't hardcode values that should be configurable
- ❌ Don't skip tests or validation steps

## Confidence Score: 9/10
This PRP provides comprehensive context for one-pass implementation success, including:
- Complete architectural specifications with proven patterns
- Real-world examples from the codebase
- External research from successful implementations
- Detailed validation loops with executable tests
- Clear anti-patterns and gotchas
- Step-by-step implementation order

The high confidence comes from the detailed INITIAL.md specification, working examples in the codebase, and extensive external research showing proven patterns for similar systems.
