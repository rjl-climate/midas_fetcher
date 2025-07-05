# MIDAS Fetcher

**High-performance concurrent downloader for UK Met Office MIDAS Open weather data**

[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-in_progress-yellow.svg)]()

MIDAS Fetcher is a specialized command-line tool and Rust library designed to efficiently download large volumes of historical weather data from the UK Met Office MIDAS Open Archive. It provides intelligent discovery, resumable downloads, and adaptive performance optimization while respecting CEDA's server resources.

## Key Features

- **Work-Stealing Concurrency**: Advanced work distribution preventing worker starvation
- **CEDA-Respectful**: Built-in rate limiting with exponential backoff and circuit breakers
- **8+ Dataset Types**: Temperature, rainfall, wind, radiation, soil, and comprehensive weather data
- **Resumable Downloads**: Atomic file operations with automatic recovery from interruptions
- **Fast Verification**: Manifest-based cache checking (sub-second vs 20+ minutes)
- **Zero Data Corruption**: Atomic file operations with temporary file patterns
- **Real-time Progress**: ETA calculations with rolling window averages
- **Secure Authentication**: Interactive credential setup with secure storage

## Table of Contents

- [What is MIDAS Open?](#what-is-midas-open)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Development](#development)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## What is MIDAS Open?

The [Centre for Environmental Data Analysis (CEDA)](https://help.ceda.ac.uk/article/4982-midas-open-user-guide) hosts the **MIDAS Open** dataset - a comprehensive collection of UK meteorological observations released annually by the Met Office under the Open Government Licence.

### The Data Challenge

MIDAS Open contains **massive volumes** of historical weather data:
- **Daily observations**: ~95% of temperature data, 13% of rainfall data
- **Hourly observations**: ~83% of weather data  
- **Time span**: Late 19th century to recent years
- **Geographic coverage**: 1000+ UK land-based weather stations
- **File structure**: Complex nested directories by county → station → year

### The Problem This Tool Solves

CEDA currently provides **no specialized tools** for bulk downloading MIDAS Open data. Researchers face:
- ❌ Manual navigation through thousands of nested directories
- ❌ No resumable download capability
- ❌ Risk of overwhelming CEDA servers with naive approaches
- ❌ Difficulty verifying download completeness
- ❌ No protection against data corruption

**MIDAS Fetcher solves these problems** with intelligent automation, built-in verification, and CEDA-respectful download patterns.

## Installation

### Prerequisites

1. **Rust Toolchain** (1.80+ with 2024 edition support):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source ~/.cargo/env
   rustup default stable
   ```

2. **CEDA Account**: Register at [https://services.ceda.ac.uk/](https://services.ceda.ac.uk/)

### Build from Source

```bash
git clone https://github.com/your-org/midas_fetcher.git
cd midas_fetcher
cargo build --release
```

The binary will be available at `target/release/midas_fetcher`.

> **Future Releases**: Pre-built binaries will be available on the GitHub releases page.

## Quick Start

### 1. Setup Authentication
```bash
cargo run -- auth setup
# Follow interactive prompts to securely store CEDA credentials
```

### 2. Download a Dataset
```bash
# Interactive dataset selection (coming soon)
cargo run -- download

# Or specify directly (coming soon)
cargo run -- download --dataset uk-daily-temperature-obs
```

### 3. Verify Your Cache
```bash
# Fast manifest-based verification (coming soon)
cargo run -- cache verify --fast
```

## Architecture

MIDAS Fetcher uses a sophisticated **work-stealing concurrent download architecture** designed for efficiency, reliability, and server-friendliness:

### Core Components

- **Authentication Module**: Secure CEDA session management with CSRF token handling
- **Manifest System**: Hash-based verification and duplicate detection
- **Work-Stealing Queue**: Prevents worker starvation with intelligent task distribution
- **HTTP Client**: Rate-limited requests with exponential backoff and circuit breakers
- **Cache Manager**: Atomic file operations with reservation system
- **Worker Pool**: Configurable concurrency with graceful error handling
- **Progress Monitoring**: Real-time updates with ETA calculations

### Key Benefits

#### Intelligent Concurrency
- **Work-stealing prevents starvation**: Workers never wait for specific files
- **Linear scaling**: Performance scales with worker count up to network/server limits
- **Adaptive load balancing**: Automatic distribution without complex coordination

#### Reliability
- **Atomic downloads**: Temp files + rename prevents partial files
- **Automatic retry logic**: Exponential backoff with jitter for transient failures
- **Circuit breakers**: Temporary pauses when servers are overloaded
- **Manifest consistency**: Enables exact resume from interruptions

#### Performance  
- **Sub-second verification**: Manifest-based cache checking vs 20+ minute scans
- **HTTP/2 connection pooling**: Reduces connection overhead
- **Parallel discovery + downloads**: Maximizes throughput while respecting limits
- **Memory efficiency**: Streaming processing prevents memory growth

### Responsible Usage

MIDAS Fetcher is designed with **CEDA-respectful defaults**:

- **Default Rate Limiting**: 5 requests/second with burst allowance
- **Exponential Backoff**: Automatic delays on server errors (429/503)
- **Circuit Breakers**: Temporary pauses during server overload
- **Configurable Limits**: Adjust based on your requirements and permissions

## Development

### Development Setup

```bash
git clone https://github.com/your-org/midas_fetcher.git
cd midas_fetcher
cargo test --all
cargo run -- --help
```

### Project Structure

```
src/
├── main.rs                  # CLI entry point
├── lib.rs                   # Library API for future integration
├── constants.rs             # Centralized constants (URLs, timeouts, limits)
├── errors.rs                # Comprehensive error types with categorization
├── app/                     # Core application logic (future modules)
├── cli/                     # CLI-specific code (future modules)
└── auth/                    # Authentication module (future modules)
```

### Quality Standards

This project follows strict quality standards:

- **Test-Driven Development**: All features developed with tests first
- **Error Propagation**: Libraries use `Result<T, E>`, applications handle presentation
- **Clippy Compliance**: All warnings treated as errors (`-D warnings`)
- **Formatting**: Automatic formatting with `rustfmt`
- **Documentation**: Comprehensive rustdoc for all public APIs

### Running Tests

```bash
# Run all tests
cargo test --all

# Run with output
cargo test --all -- --nocapture

# Run specific test
cargo test test_constants_accessible
```

### Code Quality Checks

```bash
# Format code
cargo fmt --all

# Check for issues
cargo clippy --all -- -D warnings

# Check compilation
cargo check --all
```

## Performance

### Benchmarks (Projected)

| Operation | Traditional Approach | MIDAS Fetcher | Improvement |
|-----------|---------------------|---------------|-------------|
| Cache Verification | 20+ minutes | <1 second | 1200x faster |
| 1000 File Download | 45-60 minutes | 15-20 minutes | 3x faster |
| Worker Utilization | 25-50% (starvation) | 95%+ | 2-4x efficiency |
| Memory Usage | Unbounded growth | Bounded streaming | Stable |

### Scalability

- **Linear worker scaling**: Performance increases linearly with worker count
- **Memory bounded**: Constant memory usage regardless of dataset size
- **Network optimized**: HTTP/2 connection pooling and keep-alive
- **Server friendly**: Built-in rate limiting prevents server overload

## Contributing

Contributions are welcome! This tool aims to serve the UK climate research community.

### Areas for Contribution

- **Additional data sources**: Extend beyond MIDAS Open
- **Data analysis tools**: Post-download processing utilities  
- **Performance optimizations**: Better discovery algorithms
- **User interfaces**: GUI or web interface development
- **Documentation**: Usage examples, tutorials

### Development Guidelines

1. **Follow TDD**: Write tests before implementation
2. **Respect CEDA**: Test rate limiting and backoff strategies
3. **Document thoroughly**: Include rustdoc comments and examples
4. **Maintain quality**: All PRs must pass clippy, tests, and formatting

### Reporting Issues

Please use GitHub Issues with:
- Clear reproduction steps
- Dataset and command used
- Error messages and logs
- System information (OS, Rust version)

## Current Status

### ✅ Completed (Task 1)
- [x] Project structure and dependencies
- [x] Comprehensive constants system
- [x] Robust error handling with categorization
- [x] Library foundation for future GUI integration
- [x] Testing framework and quality gates

### 🚧 In Progress
- [ ] Authentication module (Task 2)
- [ ] HTTP client with rate limiting (Task 3)
- [ ] Data models and manifest system (Task 4)
- [ ] Work-stealing queue implementation (Task 5)
- [ ] Cache management with reservations (Task 6)
- [ ] Download workers (Task 7)
- [ ] Progress monitoring (Task 8)
- [ ] CLI interface (Task 9)
- [ ] Library integration (Task 10)

### 🎯 Success Criteria
- [ ] Authenticate with CEDA using session cookies
- [ ] Download 1000+ files concurrently without server errors
- [ ] Achieve linear performance scaling with worker count
- [ ] Handle interruptions gracefully with resumable downloads
- [ ] Verify cache integrity in <1 second using manifest system
- [ ] Zero data corruption through atomic file operations

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- **CEDA Archive**: [https://www.ceda.ac.uk/](https://www.ceda.ac.uk/)
- **MIDAS Open Guide**: [https://help.ceda.ac.uk/article/4982-midas-open-user-guide](https://help.ceda.ac.uk/article/4982-midas-open-user-guide)
- **Met Office**: [https://www.metoffice.gov.uk/](https://www.metoffice.gov.uk/)

---

**Author**: Climate Research Tools  
**Status**: Active Development  
**Rust Edition**: 2024