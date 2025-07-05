# Comprehensive Rust Development Guidelines

**IMPORTANT**: You will use Rust 2024 to exploit its Async Closures feature, Improved Lifetime and Temporary Scopes, and Stabilized APIs.

This document defines the principles and practices for developing production-quality Rust applications. These are directives, not suggestions. The goal is to produce reliable, maintainable, and well-architected Rust code that can evolve with changing requirements.

## Table of Contents
1. [Project Context & Task Management](#project-context--task-management)
2. [Core Development Philosophy](#core-development-philosophy)
3. [Architecture & Design Patterns](#architecture--design-patterns)
4. [Error Handling Strategy](#error-handling-strategy)
5. [Configuration Management](#configuration-management)
6. [Testing Philosophy & Practices](#testing-philosophy--practices)
7. [Async & Concurrency Patterns](#async--concurrency-patterns)
8. [Code Organization & Style](#code-organization--style)
9. [Logging & Observability](#logging--observability)
10. [Dependencies & Security](#dependencies--security)
11. [CI/CD & Quality Gates](#cicd--quality-gates)
12. [AI Assistant Behavior Rules](#ai-assistant-behavior-rules)

---

## Project Context & Task Management

### Initial Context Loading
- **Always read `PLANNING.md`** first to understand project architecture, goals, and constraints
- **Check `TASK.md`** before starting work. Add new tasks with descriptions and dates if not listed
- **Review existing code structure** to maintain consistency with established patterns

### Task Tracking
- **Mark tasks complete** in `TASK.md` immediately upon completion
- **Document discovered work** under "Discovered During Development" section
- **Update README.md** when adding features, changing dependencies, or modifying setup

### Documentation Standards
- **Every public API** requires rustdoc comments with purpose, parameters, return values
- **Complex logic** needs inline `// Why:` comments explaining reasoning
- **Architecture decisions** should be documented in `PLANNING.md`

---

## Core Development Philosophy

### 1. Explicit Over Implicit
Code behavior should be obvious from reading it. No magic. No surprises.

```rust
// BAD: Implicit behavior
fn process(data: &str) -> String {
    data.parse().unwrap_or_default() // What does this parse to?
}

// GOOD: Explicit behavior
fn process(data: &str) -> Result<Port, ParseError> {
    data.parse::<u16>()
        .map(|p| Port::new(p))
        .map_err(|e| ParseError::InvalidPort(e))
}
```

### 2. Errors Are Values
Errors are part of the type system, not exceptions. Use them to make impossible states unrepresentable.

### 3. Ownership Is Architecture
How data flows through ownership boundaries defines your application's architecture. Design ownership first, implement second.

### 4. Test-Driven Development
No feature is complete without tests. Red-Green-Refactor is the only way.

---

## Architecture & Design Patterns

### Separation of Concerns

Structure applications with clear boundaries:

```
src/
├── main.rs           # Thin CLI layer - argument parsing, error display
├── lib.rs           # Public API surface
├── app/             # Core application logic (future library)
│   ├── mod.rs
│   ├── models.rs    # Domain models - pure data
│   ├── services/    # Business logic - pure functions when possible
│   └── adapters/    # External world interaction (DB, HTTP, FS)
├── cli/             # CLI-specific code
└── config.rs        # Configuration structures
```

### Dependency Injection for Testability

Design components to accept dependencies rather than creating them:

```rust
// BAD: Hard to test
pub struct Downloader {
    client: reqwest::Client,
}

impl Downloader {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(), // Created internally
        }
    }
}

// GOOD: Testable
pub struct Downloader {
    client: Arc<dyn HttpClient>, // Trait object for testing
}

impl Downloader {
    pub fn new(client: Arc<dyn HttpClient>) -> Self {
        Self { client }
    }
}
```

### Interface Segregation

Define narrow interfaces (traits) that components can implement:

```rust
// Define capabilities, not implementations
pub trait FileStore: Send + Sync {
    async fn exists(&self, path: &Path) -> Result<bool>;
    async fn read(&self, path: &Path) -> Result<Vec<u8>>;
    async fn write(&self, path: &Path, data: &[u8]) -> Result<()>;
}

// Now you can have FileSystemStore, S3Store, MemoryStore for tests
```

---

## Error Handling Strategy

### Library Error Design

Libraries define specific, actionable error types:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration file not found at {path}")]
    NotFound { path: PathBuf },

    #[error("Invalid port number {port}: must be between 1-65535")]
    InvalidPort { port: String },

    #[error("Missing required field: {field}")]
    MissingField { field: &'static str },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// Libraries NEVER:
// - Call .unwrap() or .expect()
// - Print to stdout/stderr
// - Exit the process
```

### Application Error Handling

Applications add context and present errors to users:

```rust
use anyhow::{Context, Result};

fn main() -> Result<()> {
    let config = load_config()
        .context("Failed to load application configuration")?;

    let downloader = create_downloader(&config)
        .context("Failed to initialize downloader")?;

    // Main application loop
    if let Err(e) = run_downloads(downloader).await {
        eprintln!("Application error: {:#}", e);
        std::process::exit(1);
    }

    Ok(())
}
```

### Error Categorization

Classify errors by recovery strategy:

```rust
pub enum ErrorKind {
    Transient,    // Retry might succeed
    User,         // User must fix something
    Bug,          // Programming error - should never happen
}
```

---

## Configuration Management

### Configuration as Code

Configuration should be strongly typed and validated:

```rust
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)] // Catch typos early
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,

    pub workers: WorkerConfig,

    #[serde(default)]
    pub logging: LogConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkerConfig {
    #[serde(deserialize_with = "validate_worker_count")]
    pub count: usize,

    pub retry_attempts: u32,
}

fn validate_worker_count<'de, D>(deserializer: D) -> Result<usize, D::Error>
where D: serde::Deserializer<'de> {
    let count = usize::deserialize(deserializer)?;
    if count == 0 || count > 100 {
        return Err(serde::de::Error::custom(
            "worker count must be between 1 and 100"
        ));
    }
    Ok(count)
}
```

### Configuration Loading Pattern

```rust
impl Config {
    /// Load configuration with layered precedence:
    /// 1. Default values
    /// 2. Configuration file
    /// 3. Environment variables
    /// 4. Command-line arguments
    pub fn load(args: &Args) -> Result<Self> {
        let mut config = Self::default();

        // Layer 1: Config file (if exists)
        if let Some(path) = &args.config_file {
            let file_config = Self::from_file(path)
                .context("Failed to load config file")?;
            config.merge(file_config);
        }

        // Layer 2: Environment variables
        config.merge_env()?;

        // Layer 3: Command-line overrides
        config.apply_args(args);

        // Validate final configuration
        config.validate()?;

        Ok(config)
    }
}
```

---

## Testing Philosophy & Practices

### Test Organization

```rust
// Unit tests live with the code
mod cache {
    // Implementation

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_cache_miss() { /* ... */ }
    }
}

// Integration tests in tests/ directory
// tests/downloads.rs
#[test]
fn test_full_download_flow() { /* ... */ }
```

### Async Testing

```rust
#[tokio::test]
async fn test_concurrent_downloads() {
    // Given: Mock HTTP server
    let server = MockServer::start().await;
    server.mock(|when, then| {
        when.path("/file.txt");
        then.status(200).body("content");
    });

    // When: Multiple workers download
    let results = run_downloads(server.url()).await;

    // Then: Exactly one download occurred
    server.verify_hits("/file.txt", 1);
}
```

### Test Helpers and Fixtures

```rust
/// Create a test fixture with proper cleanup
pub struct TestEnv {
    pub temp_dir: TempDir,
    pub config: Config,
}

impl TestEnv {
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let config = Config {
            cache_root: temp_dir.path().to_owned(),
            ..Default::default()
        };
        Ok(Self { temp_dir, config })
    }
}

#[test]
fn test_with_fixture() {
    let env = TestEnv::new().unwrap();
    // Test runs with isolated filesystem
    // Cleanup happens automatically on drop
}
```

### Property-Based Testing

For complex invariants, use proptest:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_parse_roundtrip(s in "[0-9]{1,5}") {
        let port: u16 = s.parse().unwrap();
        let formatted = port.to_string();
        prop_assert_eq!(s, formatted);
    }
}
```

---

## Async & Concurrency Patterns

### Structured Concurrency

Always use structured concurrency patterns:

```rust
use tokio::task::JoinSet;

pub async fn process_files(files: Vec<FileInfo>) -> Result<Stats> {
    let mut tasks = JoinSet::new();

    // Spawn all tasks
    for file in files {
        tasks.spawn(async move {
            download_file(file).await
        });
    }

    // Collect results - all tasks complete or error
    let mut stats = Stats::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(file_stats)) => stats.merge(file_stats),
            Ok(Err(e)) => stats.record_error(e),
            Err(e) => return Err(e.into()), // Task panicked
        }
    }

    Ok(stats)
}
```

### Cancellation and Timeouts

```rust
use tokio::time::{timeout, Duration};

pub async fn download_with_timeout(
    url: &str,
    max_duration: Duration,
) -> Result<Vec<u8>> {
    timeout(max_duration, async {
        // Download logic
    })
    .await
    .context("Download timed out")?
}
```

### Sharing State Between Tasks

```rust
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// For frequently read, rarely written state
type SharedState = Arc<RwLock<State>>;

// For state with equal read/write patterns
type SharedQueue = Arc<Mutex<WorkQueue>>;

// For single-producer, multiple-consumer
use tokio::sync::broadcast;

// For coordinating task completion
use tokio::sync::Notify;
```

---

## Code Organization & Style

### Module Guidelines

- **One concept per module** - If you can't describe it in one sentence, split it
- **500 line limit** - Files approaching this need refactoring
- **Public API at top** - Public items before private in each file
- **Tests with code** - Unit tests in same file, integration tests separate

### Import Organization

```rust
// Standard library
use std::collections::HashMap;
use std::path::PathBuf;

// External crates
use anyhow::{Context, Result};
use tokio::sync::mpsc;

// Local crates
use crate::config::Config;
use crate::models::FileInfo;

// Never use glob imports except for preludes
use some_crate::prelude::*; // OK for preludes only
```

### Constant Management

All constants in `src/constants.rs`:

```rust
// src/constants.rs
pub const DEFAULT_WORKER_COUNT: usize = 4;
pub const MAX_RETRY_ATTEMPTS: u32 = 3;
pub const DOWNLOAD_TIMEOUT_SECS: u64 = 300;

// Group related constants
pub mod limits {
    pub const MAX_FILE_SIZE: usize = 1024 * 1024 * 100; // 100MB
    pub const MAX_CONCURRENT_DOWNLOADS: usize = 10;
}
```

### Formatting and Linting

- **rustfmt** with default settings - no custom configuration
- **clippy** with all warnings as errors: `#![deny(clippy::all)]`
- Fix all issues before commits - no exceptions

---

## Logging & Observability

### Structured Logging

Use `tracing` for all logging:

```rust
use tracing::{debug, error, info, instrument, warn};

#[instrument(skip(client))] // Auto-log function entry/exit
pub async fn download_file(
    client: &Client,
    url: &str,
) -> Result<Vec<u8>> {
    info!(%url, "Starting download");

    let response = client.get(url).send().await?;
    let status = response.status();

    if !status.is_success() {
        error!(%url, %status, "Download failed");
        return Err(anyhow!("HTTP {}", status));
    }

    let bytes = response.bytes().await?;
    info!(%url, size = bytes.len(), "Download complete");

    Ok(bytes.to_vec())
}
```

### Output Conventions

- **stdout**: Machine-readable output only (JSON, CSV)
- **stderr**: All human-readable output (logs, progress, errors)
- **Never println! in libraries**: Only the binary crate writes output

---

## Dependencies & Security

### Dependency Management

```toml
# Cargo.toml
[dependencies]
# Pin minor versions for stability
tokio = "1.28"
serde = { version = "1.0", features = ["derive"] }

# Use workspace dependencies for consistency
[workspace.dependencies]
anyhow = "1.0"
thiserror = "1.0"
```

### Security Practices

- Run `cargo audit` before every release
- Use `cargo-deny` for license compliance
- Review all dependency updates - don't blindly update
- Prefer well-maintained crates with recent commits

---

## CI/CD & Quality Gates

### Required GitHub Actions Workflow

```yaml
name: Rust CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    name: Test Suite
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy

      - uses: Swatinem/rust-cache@v2

      - name: Check formatting
        run: cargo fmt --all -- --check

      - name: Clippy
        run: cargo clippy --all-targets --all-features -- -D warnings

      - name: Test
        run: cargo test --all-features

      - name: Doc tests
        run: cargo test --doc

      - name: Security audit
        run: |
          cargo install cargo-audit
          cargo audit

  coverage:
    name: Code Coverage
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Install tarpaulin
        run: cargo install cargo-tarpaulin

      - name: Generate coverage
        run: cargo tarpaulin --out Xml

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

### Pre-commit Hooks

```bash
#!/bin/bash
# .git/hooks/pre-commit

set -e

echo "Running pre-commit checks..."

# Format
cargo fmt --all

# Lint
cargo clippy --all-targets --all-features -- -D warnings

# Test
cargo test --all-features

echo "Pre-commit checks passed!"
```

---

## AI Assistant Behavior Rules

### Context Awareness
- **Always read project files** in this order: PLANNING.md → TASK.md → existing code
- **Never assume context** - ask for clarification when needed
- **Verify before suggesting** - check that files/modules exist before referencing

### Code Generation Rules
- **Never use unwrap() in libraries** - always propagate errors
- **Always write tests first** - TDD is non-negotiable
- **Include error context** - use `.context()` for error messages
- **Prefer explicit over clever** - clarity beats brevity

### When Making Changes
- **Never delete without permission** - ask before removing code
- **Maintain consistency** - follow existing patterns in the codebase
- **Update documentation** - keep README.md and inline docs current
- **Add discovered tasks** - update TASK.md with found issues

### Communication Style
- **Be direct and clear** - no unnecessary pleasantries
- **Explain the why** - provide reasoning for design decisions
- **Show, don't tell** - provide code examples
- **Flag uncertainties** - clearly mark assumptions that need verification

---

## Environment Notes

- Rust edition is 2024 (specified in Cargo.toml)
- MSRV (Minimum Supported Rust Version) should be documented
- Use stable Rust unless nightly features are absolutely required


## Miscellaneous

- Do NOT use emojis. They are distracting and unprofessional.
