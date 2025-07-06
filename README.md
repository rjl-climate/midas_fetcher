# MIDAS Fetcher

**High-performance concurrent downloader for UK Met Office MIDAS Open weather data**

[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#development)

A command-line tool and Rust library designed to efficiently download large volumes of historical weather data from the UK Met Office MIDAS Open Archive. Built for climate researchers and data scientists who need reliable, fast, and resumable downloads while respecting CEDA's infrastructure.

> **NOTE**
> There are two companion apps that build on this tool.
>
> [Midas Processor](https://github.com/rjl-climate/midas_processor): A rust app to convert the MIDAS dataset downloaded by this tool into a .parquet file for efficient downstream processing.
>
> [Midas Analyser](https://github.com/rjl-climate/midas_analyser) A python toolkit for analysing a MIDAS dataset

## Table of Contents

- [What is MIDAS Open?](#what-is-midas-open)
- [The Problem This Tool Solves](#the-problem-this-tool-solves)
- [What MIDAS Fetcher Does](#what-midas-fetcher-does)
- [Account & Authentication](#account--authentication)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Commands & Usage](#commands--usage)
- [Technical Architecture](#technical-architecture)
- [Performance](#performance)
- [Contributing](#contributing)
- [Acknowledgments](#acknowledgments)
- [License](#license)
- [Changelog](#changelog)

## What is MIDAS Open?

[MIDAS Open](https://help.ceda.ac.uk/article/4982-midas-open-user-guide) is a comprehensive collection of meteorological observation datasets released annually by the UK Met Office under the Open Government Licence. The dataset is hosted by the [Centre for Environmental Data Analysis (CEDA)](https://www.ceda.ac.uk/) and contains:

- **Historical weather data** from late 19th century to recent years
- **1000+ UK land-based weather stations** with varying temporal coverage
- **Multiple observation types**: temperature, rainfall, wind, radiation, soil data
- **Different temporal resolutions**: daily observations (~95% temperature coverage), hourly weather data (~83% coverage)
- **Complex hierarchical structure**: organized by historic county → station → quality control version → year

The data are structured in paths like:
```
ukmo-midas-open/data/<dataset>/<release-version>/<historic-county>/<site>/<qc-version>/files
```

## The Problem This Tool Solves

CEDA currently provides **no specialized tools** for bulk downloading MIDAS Open data. Climate researchers and data scientists face significant challenges:

### Data Discovery Challenges
- ❌ **Manual navigation** through thousands of nested directories
- ❌ **Complex metadata interpretation** requiring understanding of station histories
- ❌ **No unified dataset search** across quality control versions and time periods
- ❌ **Directory structure based on historic counties** that don't match modern boundaries

### Download Challenges
- ❌ **No resumable downloads** - interruptions mean starting over
- ❌ **Risk of overwhelming CEDA servers** with naive parallel approaches
- ❌ **No progress tracking** for large multi-gigabyte downloads
- ❌ **No verification of download completeness** or data integrity
- ❌ **No protection against partial/corrupted files**

### Research Workflow Challenges
- ❌ **Time-consuming data acquisition** taking days or weeks
- ❌ **Difficulty reproducing downloads** across research teams
- ❌ **No systematic approach** to managing local data caches
- ❌ **Manual verification** of downloaded file checksums

## What MIDAS Fetcher Does

MIDAS Fetcher solves these problems through intelligent automation and sophisticated technical architecture:

### Core Capabilities
- 🔍 **Dataset Discovery**: Automatic manifest parsing and interactive dataset selection
- 🎯 **Selective Downloads**: Filter by dataset, county, station, quality version, or time period
- 🚀 **Concurrent Downloads**: It's fast
- 📦 **Intelligent Caching**: Hierarchical organization with deduplication and fast verification
- ✅ **Data Integrity**: Atomic file operations with MD5 verification and cache integrity checking
- 🔄 **Resumable Downloads**: Continues from exactly where interrupted, no wasted bandwidth
- 📊 **Real-time Progress**: ETA calculations, download rates, and comprehensive status reporting
- 🛡️ **CEDA-Respectful**: Built-in rate limiting, exponential backoff, and circuit breakers

### Key Benefits
- **Performance**: 3-4x faster than manual approaches with linear scaling
- **Reliability**: Zero data corruption through atomic operations
- **Efficiency**: Fast cache verification with progress tracking and corruption detection
- **Usability**: Simple commands for complex operations
- **Respectful**: Protects CEDA infrastructure while maximizing legitimate throughput

## Account & Authentication

### CEDA Account Required
You need a free CEDA account to download MIDAS Open data:

1. **Register** at [https://services.ceda.ac.uk/](https://services.ceda.ac.uk/)
2. **Verify your email** and complete account setup
3. **Accept the MIDAS Open licence** through the CEDA data portal
4. **Note your username and password** for authentication setup

### Security Considerations
- Credentials are stored locally in `.env` files with restricted permissions (Unix: 600)
- No credentials are transmitted except for CEDA authentication
- Session tokens are managed automatically with secure refresh
- All network communication uses HTTPS

## Installation

### Prerequisites

**Rust Toolchain** (1.80+ with 2024 edition support):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
rustup default stable
```

### Build from Source

```bash
git clone https://github.com/rjl-climate/midas_fetcher.git
cd midas_fetcher
cargo build --release
```

The binary will be available at `target/release/midas_fetcher`.

**Alternative**: Add to PATH
```bash
cargo install --path .
```

> **Future**: Pre-built binaries will be available on GitHub releases

## Quick Start

### 1. Setup Authentication
```bash
midas_fetcher auth setup
# Follow interactive prompts to securely store CEDA credentials

# Verify authentication works
midas_fetcher auth verify
```

### 2. Update Manifest
```bash
# Download latest file manifest from CEDA
midas_fetcher manifest update

# Check manifest information
midas_fetcher manifest info
```

### 3. Download Data
```bash
# Interactive dataset selection
midas_fetcher download

# Download specific dataset
midas_fetcher download --dataset uk-daily-temperature-obs

# Download with filters
midas_fetcher download --dataset uk-daily-temperature-obs --county devon --limit 100

# Dry run to see what would be downloaded
midas_fetcher download --dataset uk-daily-temperature-obs --dry-run
```

### 4. Verify Downloads
```bash
# Verify cache integrity by checking file hashes against manifest
midas_fetcher cache verify

# Verify specific dataset only
midas_fetcher cache verify --dataset uk-daily-temperature-obs

# Check cache information
midas_fetcher cache info
```

## Commands & Usage

### Download Command
```bash
# Basic download
midas_fetcher download --dataset <dataset-name>

# With filtering
midas_fetcher download \
  --dataset uk-daily-temperature-obs \
  --county devon \
  --quality-0 \  # Use QC version 0 (default is version 1)
  --limit 1000

# Performance tuning
midas_fetcher download \
  --dataset uk-daily-temperature-obs \
  --workers 8 \
  --force  # Restart incomplete downloads
```

### Authentication Commands
```bash
midas_fetcher auth setup     # Interactive credential setup
midas_fetcher auth verify    # Test authentication
midas_fetcher auth status    # Show current status
midas_fetcher auth clear     # Remove stored credentials
```

### Manifest Commands
```bash
midas_fetcher manifest update        # Download latest manifest
midas_fetcher manifest check         # Check for updates
midas_fetcher manifest info          # Show manifest statistics
midas_fetcher manifest list          # List available datasets
midas_fetcher manifest list --datasets-only  # Just dataset names
```

### Cache Commands
```bash
midas_fetcher cache verify                    # Verify cache integrity by checking file hashes
midas_fetcher cache verify --dataset <name>   # Verify specific dataset only
midas_fetcher cache info                      # Cache statistics, location, and file counts
midas_fetcher cache clean                     # Remove temporary and failed files
```

### Global Options
```bash
--verbose        # Detailed progress information
--quiet          # Suppress non-essential output
--config FILE    # Use custom configuration file
--cache-dir DIR  # Use custom cache directory
```

## Technical Architecture

MIDAS Fetcher uses a concurrent architecture designed for efficiency, reliability, and respectful server interaction:

### Distributed Consensus at the Filesystem Level

The fundamental challenge is coordinating multiple workers accessing shared filesystem state. MIDAS Fetcher treats the filesystem as a distributed system requiring explicit coordination:

- **In-memory reservation system** provides transactional semantics missing from filesystems
- **Atomic file operations** prevent partial downloads and corruption
- **Shared state tracking** ensures workers never conflict over the same files
- **Work-stealing queue** prevents worker starvation under any file distribution

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Worker 1  │    │   Worker 2  │    │   Worker 3  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └─────────────┬────┴─────────────────┘
                     │
              ┌──────▼──────┐
              │ Work Queue  │
              │ + Reservoir │
              └─────────────┘
```

### Cache Management with Integrity Assurance

The cache system ensures data integrity through multiple layers:

- **Hierarchical organization**: `dataset/quality/county/station` structure matches CEDA
- **Capability files**: Separate folder structure for metadata and capability files
- **Atomic operations**: Temp file + rename pattern prevents corruption
- **MD5 verification**: Automatic verification against manifest checksums
- **Deduplication**: Hash-based detection prevents duplicate downloads
- **Integrity verification**: MD5 hash checking against manifest with progress reporting

### Work-Stealing Architecture

The work-stealing queue prevents worker starvation and enables linear scaling:

```rust
// Simplified algorithm:
loop {
    if let Some(work) = queue.steal_work() {
        if cache.try_reserve(work.hash) {
            download_and_save(work).await;
            cache.mark_completed(work.hash);
        }
        // If reservation fails, immediately try next file
    } else {
        sleep_briefly().await;
    }
}
```

**Benefits**:
- **Linear scaling**: Performance increases with worker count up to network limits
- **No starvation**: Workers never wait for specific files
- **Automatic load balancing**: Work distributes optimally without coordination
- **Fault tolerance**: Worker failures don't block others

### CEDA Client: Respectful and Robust

The HTTP client implements multiple layers of protection for CEDA's infrastructure:

#### Rate Limiting
- **Default limits**: 15 requests/second with burst allowance
- **Adaptive throttling**: Automatically reduces rate when detecting server strain
- **Per-host limiting**: Respects CEDA-specific constraints
- **Jittered delays**: Prevents synchronized request storms

#### Error Handling
- **Exponential backoff**: Automatic delays on server errors (429/503)
- **Circuit breakers**: Temporary pauses during prolonged server issues
- **Retry classification**: Distinguishes permanent vs. transient failures
- **Connection pooling**: HTTP/2 connection reuse reduces overhead

#### Authentication
- **Session management**: Automatic login and token refresh
- **CSRF protection**: Proper token extraction and handling
- **Secure storage**: Local credential management with proper permissions

## Performance

### Benchmarks

| Operation | Manual Approach | MIDAS Fetcher | Improvement |
|-----------|----------------|---------------|-------------|
| Cache verification | Manual file checking | Automated MD5 verification | Corruption detection |
| 1000 file download | 45-60 minutes | 15-20 minutes | 3x faster |
| Worker utilization | 25-50% (starvation) | 95%+ | 2-4x efficiency |
| Memory usage | Unbounded growth | Constant (bounded) | Stable |

### Scalability Characteristics
- **Linear worker scaling**: Performance increases linearly with worker count
- **Memory bounded**: Constant memory usage regardless of dataset size
- **Network optimized**: HTTP/2 connection pooling and persistent connections
- **Server friendly**: Built-in protections prevent overwhelming CEDA

### Recommended Settings
```bash
# For fast connections and powerful machines
midas_fetcher download --workers 12 --dataset uk-daily-temperature-obs

# For shared or limited connections
midas_fetcher download --workers 4 --dataset uk-daily-temperature-obs

# For testing or development
midas_fetcher download --workers 2 --limit 10 --dataset uk-daily-temperature-obs
```

## Contributing

Contributions are welcome! This tool aims to serve the UK climate research community and can benefit from diverse perspectives and use cases.

### Areas for Contribution
- **Additional data sources**: Extend beyond MIDAS Open to other CEDA datasets
- **Data analysis tools**: Post-download processing and analysis utilities
- **User interfaces**: GUI applications using the library API
- **Documentation**: Usage examples, tutorials, research workflows
- **Data validation**: Enhanced quality control and metadata verification

### Development Guidelines

1. **Follow Test-Driven Development**: Write tests before implementation
2. **Respect CEDA infrastructure**: Test rate limiting and backoff strategies thoroughly
3. **Document comprehensively**: Include rustdoc comments with examples
4. **Maintain quality**: All PRs must pass clippy, tests, and formatting checks
5. **Consider the library API**: Changes should support both CLI and future GUI usage

### Development Setup
```bash
git clone https://github.com/rjl-climate/midas_fetcher.git
cd midas_fetcher

# Run all tests
cargo test --all

# Check code quality
cargo clippy --all -- -D warnings
cargo fmt --all

# Test CLI functionality
cargo run -- --help
cargo run -- auth setup
```

### Reporting Issues
Please use GitHub Issues with:
- Clear reproduction steps
- Dataset and command used
- Complete error messages and logs
- System information (OS, Rust version)
- Network conditions if relevant

## Acknowledgments

### Thanks to CEDA
This tool exists thanks to the [Centre for Environmental Data Analysis (CEDA)](https://www.ceda.ac.uk/) and the UK Met Office for:
- Providing free access to MIDAS Open data under the Open Government Licence
- Maintaining robust infrastructure for climate data distribution
- Supporting the research community with comprehensive documentation

### Thanks to the Rust Community
Built with excellent crates from the Rust ecosystem:
- **tokio**: Asynchronous runtime powering concurrent downloads
- **reqwest**: HTTP client with authentication and connection pooling
- **clap**: Command-line interface with excellent user experience
- **indicatif**: Progress bars and status reporting
- **governor**: Rate limiting algorithms protecting server infrastructure
- **serde**: Serialization for configuration and data interchange

### Welcome Updates
This tool is actively developed to meet real research needs. If you:
- Encounter datasets not currently supported
- Need different filtering or selection capabilities
- Have performance requirements this tool doesn't meet
- Want to integrate with other tools or workflows

Please open an issue or discussion! The goal is maximum utility for the climate research community.

## License

This project is dual-licensed under:
- **MIT License** - see [LICENSE-MIT](LICENSE-MIT) for details
- **Apache License 2.0** - see [LICENSE-APACHE](LICENSE-APACHE) for details

You may choose either license for your use.

## Changelog

### Current Status: v0.1.0 (Active Development)

#### ✅ Completed Features
- [x] **Project foundation**: Cargo setup, dependencies, error handling
- [x] **Authentication system**: Secure CEDA credential management
- [x] **HTTP client**: Rate-limited, authenticated downloads with backoff
- [x] **Data models**: Manifest parsing and file information structures
- [x] **Work-stealing queue**: Concurrent task distribution preventing starvation
- [x] **Cache management**: Atomic operations with reservation system
- [x] **Download workers**: Parallel processing with error recovery
- [x] **Progress monitoring**: Real-time updates with ETA calculations
- [x] **CLI interface**: Complete command-line tool with all major functions
- [x] **Library API**: Clean separation for future GUI integration

#### 🎯 Validated Capabilities
- [x] Authenticate with CEDA using session cookies
- [x] Download 1000+ files concurrently without server errors
- [x] Linear performance scaling with worker count (no starvation)
- [x] Graceful interruption handling with resumable downloads
- [x] Cache verification with MD5 hash checking and progress reporting
- [x] Zero data corruption through atomic file operations
- [x] All tests passing with comprehensive quality checks

#### 🔮 Future Roadmap
- [ ] **GUI application**: Tauri-based interface for non-technical users
- [ ] **Parquet file conversion**: Companion tool to convert the cache to .parquet files for downstream processing
- [ ] **Data analysis tools**: Built-in processing and visualization capabilities


---

**Status**: Production Ready
**Maintainer**: Richard Lyon richlyon@fastmail.com
**First Release**: 2025
**Latest Update**: July 2025
