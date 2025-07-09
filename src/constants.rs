//! Application constants for MIDAS Fetcher
//!
//! This module centralizes all constants used throughout the application,
//! organized by functional domain for maintainability and clarity.

#![allow(dead_code)] // Many constants will be used in later tasks
#![allow(unused_imports)] // Re-exports will be used in later tasks

use std::time::Duration;

/// Environment variable names for authentication
pub mod env {
    /// Environment variable name for CEDA username
    pub const USERNAME: &str = "CEDA_USERNAME";

    /// Environment variable name for CEDA password
    pub const PASSWORD: &str = "CEDA_PASSWORD";
}

/// Authentication and credential-related constants
pub mod auth {
    /// Minimum allowed username length
    pub const MIN_USERNAME_LENGTH: usize = 3;

    /// Maximum allowed username length
    pub const MAX_USERNAME_LENGTH: usize = 50;

    /// File permissions for .env file (Unix only) - owner read/write only
    #[cfg(unix)]
    pub const ENV_FILE_PERMISSIONS: u32 = 0o600;

    /// CEDA authentication base URL
    pub const CEDA_AUTH_BASE_URL: &str = "https://auth.ceda.ac.uk";

    /// CEDA login page URL
    pub const CEDA_LOGIN_URL: &str = "https://auth.ceda.ac.uk/account/signin";

    /// CSS selector for CSRF token extraction
    pub const CSRF_TOKEN_SELECTOR: &str = "input[name='csrfmiddlewaretoken']";
}

/// HTTP client configuration constants
pub mod http {
    use super::Duration;

    /// Default user agent for all HTTP requests
    pub const USER_AGENT: &str = "MIDAS-Fetcher/0.1.0 (Climate Research Tool)";

    /// Default HTTP request timeout
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

    /// Connection establishment timeout
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

    /// HTTP/2 keep-alive interval
    pub const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

    /// HTTP/2 keep-alive timeout
    pub const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Connection pool idle timeout
    pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

    /// Maximum connections per host in pool
    pub const POOL_MAX_PER_HOST: usize = 25;

    /// Maximum number of redirects to follow
    pub const MAX_REDIRECTS: usize = 10;
}

/// Rate limiting and retry configuration
pub mod limits {
    /// Default rate limit for CEDA requests (requests per second)
    pub const DEFAULT_RATE_LIMIT_RPS: u32 = 15;

    /// Web scraping rate limit (requests per second)
    pub const WEB_SCRAPING_RATE_LIMIT: u32 = 2;

    /// Web scraping timeout in seconds
    pub const WEB_SCRAPING_TIMEOUT_SECS: u64 = 30;

    /// Maximum retry attempts for failed requests
    pub const MAX_RETRIES: u32 = 3;

    /// Base delay for exponential backoff (milliseconds)
    pub const RETRY_BASE_DELAY_MS: u64 = 1000;

    /// Maximum backoff delay (seconds)
    pub const MAX_BACKOFF_SECS: u64 = 300;

    /// Jitter factor for randomizing delays (0.0-1.0)
    pub const BACKOFF_JITTER_FACTOR: f64 = 0.1;

    /// Circuit breaker failure threshold
    pub const CIRCUIT_BREAKER_FAILURE_THRESHOLD: u32 = 5;

    /// Circuit breaker success threshold for recovery
    pub const CIRCUIT_BREAKER_SUCCESS_THRESHOLD: u32 = 2;

    /// Circuit breaker timeout before attempting reset (seconds)
    pub const CIRCUIT_BREAKER_TIMEOUT_SECS: u64 = 60;
}

/// CEDA service URLs and endpoints
pub mod ceda {
    /// CEDA data archive base URL
    pub const BASE_URL: &str = "https://data.ceda.ac.uk";

    /// CEDA archive data path
    pub const DATA_PATH: &str = "/neodc/esacci/climate/land_surface_temperature/data";

    /// Manifest base URL for dataset discovery
    pub const MANIFEST_BASE_URL: &str = "https://data.ceda.ac.uk/neodc/esacci/climate";

    /// Manifest download base URL
    pub const MANIFEST_DOWNLOAD_BASE_URL: &str = "https://data.ceda.ac.uk/neodc/esacci";

    /// Test file URL for authentication verification
    pub const TEST_FILE_URL: &str = "https://data.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1980.csv";
}

/// Web scraping CSS selectors
pub mod selectors {
    /// CSS selector for dataset links
    pub const DATASET_LINK_SELECTOR: &str = "a[href*='dataset-version-']";

    /// CSS selector for version links
    pub const VERSION_LINK_SELECTOR: &str = "a[href*='version-']";

    /// CSS selector for manifest links
    pub const MANIFEST_LINK_SELECTOR: &str = "a[href$='.txt']";

    /// CSS selector for directory links
    pub const DIRECTORY_LINK_SELECTOR: &str = "a[href$='/']";

    /// CSS selector for CSV file links
    pub const CSV_FILE_SELECTOR: &str = "a[href$='.csv']";

    /// CSS selector for manifest MD5 files
    pub const MANIFEST_MD5_SELECTOR: &str = "a[href*='midas-open-v'][href$='-md5s.txt']";
}

/// File operation constants
pub mod files {
    /// Temporary file suffix for atomic operations
    pub const TEMP_FILE_SUFFIX: &str = ".tmp";

    /// Backup file suffix
    pub const BACKUP_FILE_SUFFIX: &str = ".bak";

    /// Manifest file name for cache verification
    pub const MANIFEST_FILE_NAME: &str = ".ceda_manifest.json";

    /// Maximum file size for in-memory operations (100MB)
    pub const MAX_FILE_SIZE_IN_MEMORY: usize = 100 * 1024 * 1024;

    /// Download chunk size for streaming (8KB)
    pub const DOWNLOAD_CHUNK_SIZE: usize = 8 * 1024;
}

/// Worker and concurrency configuration
pub mod workers {
    use super::Duration;

    /// Default number of download workers
    pub const DEFAULT_WORKER_COUNT: usize = 8;

    /// Maximum recommended concurrent workers
    pub const MAX_WORKER_COUNT: usize = 16;

    /// Worker queue capacity
    pub const WORKER_QUEUE_CAPACITY: usize = 256;

    /// Channel buffer size for worker communication
    pub const CHANNEL_BUFFER_SIZE: usize = 100;

    /// Manifest batch size for processing
    pub const MANIFEST_BATCH_SIZE: usize = 100;

    /// Prefetch count for cache operations
    pub const PREFETCH_COUNT: usize = 50;

    /// Maximum pending items in queue before applying backpressure
    /// Used for pull-based streaming to prevent memory exhaustion
    pub const MAX_PENDING_ITEMS: usize = 50_000;

    /// Maximum retry attempts for failed work items
    pub const MAX_RETRIES: u32 = 3;

    /// Delay between retry attempts
    pub const RETRY_DELAY: Duration = Duration::from_secs(30);

    /// Timeout for work items (if worker doesn't report progress)
    pub const WORK_TIMEOUT: Duration = Duration::from_secs(300);

    /// Default priority for work items
    pub const DEFAULT_PRIORITY: u32 = 100;

    /// High priority for important work items
    pub const HIGH_PRIORITY: u32 = 200;

    /// Low priority for background work items
    pub const LOW_PRIORITY: u32 = 50;
}

/// Progress reporting and monitoring
pub mod progress {
    /// Progress update frequency (milliseconds)
    pub const UPDATE_FREQUENCY_MS: u64 = 100;

    /// Maximum progress update frequency (Hz)
    pub const MAX_UPDATE_HZ: u64 = 10;

    /// Rolling window size for ETA calculation (seconds)
    pub const ETA_WINDOW_SIZE_SECS: u64 = 60;

    /// Minimum samples for ETA calculation
    pub const MIN_ETA_SAMPLES: usize = 5;
}

/// Cache verification constants
pub mod cache {
    /// Cache verification sample rate (0.0-1.0)
    pub const VERIFICATION_SAMPLE_RATE: f64 = 1.0;

    /// Fast verification mode (manifest-based)
    pub const FAST_VERIFICATION: bool = true;

    /// Full verification mode (network-based)
    pub const FULL_VERIFICATION: bool = false;

    /// Cache index file name
    pub const CACHE_INDEX_FILE: &str = ".cache_index.json";
}

/// Memory management constants
pub mod memory {
    /// Maximum percentage of system memory to use
    pub const MAX_MEMORY_PERCENT: f64 = 50.0;

    /// Buffer size for memory-mapped files
    pub const MMAP_BUFFER_SIZE: usize = 64 * 1024;

    /// Garbage collection threshold
    pub const GC_THRESHOLD: usize = 1000;
}

/// Logging and debugging constants
pub mod logging {
    /// Default log level
    pub const DEFAULT_LOG_LEVEL: &str = "info";

    /// Log file rotation size (10MB)
    pub const LOG_ROTATION_SIZE: u64 = 10 * 1024 * 1024;

    /// Maximum log files to keep
    pub const MAX_LOG_FILES: usize = 10;

    /// Structured logging format
    pub const LOG_FORMAT: &str = "json";
}

/// Coordinator and orchestration constants
pub mod coordinator {
    use super::Duration;

    /// Interval for cleanup tasks (cache reservations, timeouts)
    pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(120);

    /// Interval for periodic progress logging
    pub const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(60);

    /// Interval for timeout monitoring checks
    pub const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(300);

    /// Timeout for background task shutdown
    pub const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    /// Timeout for progress (no progress for this long triggers exit)
    pub const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30 * 60); // 30 minutes

    /// Rolling window size for download rate calculation
    pub const RATE_CALCULATION_WINDOW: usize = 10;

    /// Minimum progress rate threshold (files/sec)
    pub const MIN_PROGRESS_RATE_THRESHOLD: f64 = 5.0;

    /// Completion check iteration log frequency
    pub const COMPLETION_LOG_FREQUENCY: u32 = 100; // Every 10 seconds at 100ms intervals

    /// Cleanup frequency during completion detection
    pub const COMPLETION_CLEANUP_FREQUENCY: u32 = 50; // Every 5 seconds
}

// Re-export commonly used constants for convenience
pub use auth::{CEDA_AUTH_BASE_URL, CEDA_LOGIN_URL};
pub use ceda::{BASE_URL as CEDA_BASE_URL, TEST_FILE_URL};
pub use env::{PASSWORD as ENV_PASSWORD, USERNAME as ENV_USERNAME};
pub use files::{MANIFEST_FILE_NAME, TEMP_FILE_SUFFIX};
pub use http::{DEFAULT_TIMEOUT as HTTP_TIMEOUT, USER_AGENT};
pub use limits::{DEFAULT_RATE_LIMIT_RPS, MAX_RETRIES, RETRY_BASE_DELAY_MS};
pub use workers::DEFAULT_WORKER_COUNT;
