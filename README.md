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
- [Configuration](#configuration)
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
- 🎯 **Complete Downloads**: Downloads all file types (data, capability, metadata, station logs) for comprehensive analysis
- 🚀 **Concurrent Downloads**: High-performance parallel processing with linear scaling
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

# Download specific dataset (includes all file types: data, capability, metadata, and station logs)
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

## Configuration

MIDAS Fetcher uses a unified configuration system that automatically creates sensible defaults while allowing customization for specific needs.

### Configuration File Location

The configuration file is automatically created on first run at:

**macOS/Linux:**
```
~/.config/midas-fetcher/config.toml
```

**Windows:**
```
%APPDATA%\midas-fetcher\config.toml
```

### Key User Settings

These are the main settings you might want to adjust:

| Setting | Default | Purpose | Safety Level |
|---------|---------|---------|--------------|
| `rate_limit_rps` | 15 | CEDA server request rate | ⚠️ **Critical** |
| `worker_count` | 8 | Download concurrency | ⚠️ **Performance** |
| `cache_root` | Auto | Custom cache location | ✅ **Safe** |
| `request_timeout_secs` | 60 | Download timeout | ✅ **Safe** |
| `connect_timeout_secs` | 30 | Connection timeout | ✅ **Safe** |

#### Rate Limiting (Critical Setting)
```toml
[client]
rate_limit_rps = 15  # Total requests per second across ALL workers
```

> ⚠️ **IMPORTANT**: This controls how fast you hit CEDA's servers. The 15 RPS default is shared across all workers and is respectful to CEDA infrastructure. **Don't increase this unless you have explicit permission from CEDA.** Too aggressive settings can result in IP blocking.

#### Worker Count (Performance Setting)
```toml
[coordinator]
worker_count = 8  # Number of concurrent download workers
```

> ⚠️ **Performance Impact**: More workers = faster downloads, but diminishing returns beyond 8-12 workers. The rate limit (15 RPS) is shared across all workers, so adding workers won't exceed the server politeness limits.

#### Cache Location (Safe to Modify)
```toml
[cache]
cache_root = "/custom/path/to/cache"  # Uncomment and modify to use custom location
```

By default, cache uses the same unified directory as the config file:
- **macOS/Linux**: `~/.config/midas-fetcher/cache/`
- **Windows**: `%APPDATA%\midas-fetcher\cache\`

#### Timeout Settings (Safe to Modify)
```toml
[client]
request_timeout_secs = 60   # How long to wait for downloads
connect_timeout_secs = 30   # How long to wait for connections
```

Increase these if you have a slow connection or are downloading large files.

### Advanced Settings Warning

> ⚠️ **WARNING**: The configuration file contains many advanced settings for HTTP connections, retry logic, queue management, and progress reporting. **Do not modify these unless you understand their implications.** Incorrect settings can cause:
>
> - Download failures
> - Server overload (potentially resulting in IP blocks)
> - Performance degradation
> - Cache corruption

**Advanced settings you should NOT modify without expertise:**
- HTTP/2 settings (`http2`, `tcp_nodelay`, `pool_*`)
- Retry mechanisms (`max_retries`, `retry_*`)
- Queue management (`work_timeout_secs`)
- Progress reporting intervals
- Manifest processing settings

### Configuration Management

#### Viewing Current Configuration
```bash
# See where your config file is located
midas_fetcher cache info

# Edit the configuration file
nano ~/.config/midas-fetcher/config.toml
# or on macOS:
open ~/.config/midas-fetcher/config.toml
```

#### Resetting to Defaults
```bash
# Remove the config file to regenerate defaults
rm ~/.config/midas-fetcher/config.toml

# Next run will recreate with default settings
midas_fetcher auth status
```

#### Configuration Override Priority
Settings are applied in this order (later overrides earlier):

1. **Default values** (built into the application)
2. **Configuration file** (`~/.config/midas-fetcher/config.toml`)
3. **Environment variables** (future feature)
4. **Command-line arguments** (`--workers`, `--cache-dir`, etc.)

#### Example: Conservative Settings for Shared Connections
```toml
[client]
rate_limit_rps = 5          # More conservative for shared networks

[coordinator]
worker_count = 4            # Fewer workers for limited bandwidth

[client]
request_timeout_secs = 120  # Longer timeout for slow connections
```

#### Example: Faster Settings for Dedicated Connections
```toml
[client]
rate_limit_rps = 15         # Default (don't increase without CEDA permission)

[coordinator]
worker_count = 12           # More workers for fast connections

[coordinator.worker]
download_timeout_secs = 300 # Shorter timeout for fast networks
```

> 💡 **Tip**: Test any configuration changes with `--limit 10` first to ensure they work before doing large downloads.

## Commands & Usage

### Download Command
```bash
# Basic download (includes all file types: data, capability, metadata, and station logs)
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

#### File Types Downloaded
Each dataset download includes all available file types:

- **Data files**: Weather observations organized by `qcv-1/county/station/`
- **Capability files**: Station metadata and capabilities in `capability/county/station/`
- **Station metadata**: Master station information in `station-metadata/`
- **Change logs**: Dataset version changes in root directory
- **Station logs**: Individual station change histories in `station-log-files/`

#### Cache Directory Structure
```
cache/uk-daily-temperature-obs/
├── qcv-1/                    # Quality-controlled data files
│   ├── devon/
│   │   └── 01381_twist/
│   └── ...
├── capability/               # Station capability files
│   ├── devon/
│   │   └── 01381_twist/
│   └── ...
├── station-metadata/         # Master station metadata
│   └── uk-daily-temperature-obs_station-metadata.csv
├── station-log-files/        # Individual station change logs
│   ├── station_log_01381_twist_2020.txt
│   └── ...
└── change_log.txt           # Dataset-level change log
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
- **Complete file coverage**: Automatically downloads all available file types (data, capability, metadata, station logs)
- **Intelligent organization**: Special files organized in dedicated directories (station-metadata/, station-log-files/)
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

This project is licensed under:
- **MIT License** - see [LICENSE](LICENSE) for details

You may choose either license for your use.

## Changelog

### v0.1.3 (July 2025) - Cache Module Refactoring & Missing Dataset Versions

- **Enhanced dataset version handling**: Added support for missing dataset versions in manifest processing, allowing the tool to handle datasets with gaps in their version history
- **Major cache module refactoring**: Transformed monolithic 1690-line cache.rs into a modular architecture with 6 focused modules for improved maintainability
- **Improved worker ID generation**: Replaced thread-name-based worker IDs with proper sequential generation system
- **Enhanced disk space calculation**: Added framework for cross-platform disk space monitoring
- **Better code organization**: Unit tests now stay with their modules, integration tests properly separated

### v0.1.2 (July 2025) - UX Improvements & Queue Fix

- **Fixed race condition**: Downloads no longer fail immediately due to coordinator completion detection treating empty queue as finished during startup
- **Improved dataset selection**: Dataset list now shows file counts to help users choose
- **Better progress feedback**: Added "Getting datasets..." spinner and "Initializing workers..." spinner to explain delays
- **Clearer verification summary**: Replaced confusing "Success rate: 70%" with intuitive "Already complete: 100%" when files are already cached

### v0.1.1 (July 2025) - Bug Fixes & Simplification

- **Fixed special file downloads**: Station metadata and station log files now download correctly
- **Simplified download command**: Removed confusing flags, now downloads all file types automatically
- **Better cache organization**: Special files properly organized in dedicated directories

### v0.1.0 (July 2025) - Initial Release

- **Core functionality**: Concurrent downloads with work-stealing queue
- **CEDA authentication**: Secure credential management with session handling
- **Cache management**: Atomic operations with MD5 verification
- **CLI interface**: Complete command-line tool with progress monitoring


---

**Status**: Beta
**Maintainer**: Richard Lyon richlyon@fastmail.com
**First Release**: 2025
**Latest Update**: July 2025 (v0.1.3)
