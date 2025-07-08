//! Error types for MIDAS Fetcher
//!
//! This module defines comprehensive error types for all components of the application.
//! Errors are designed to be actionable and provide clear context for debugging and
//! user feedback.

#![allow(dead_code)] // Error types will be used in later tasks

use std::path::PathBuf;
use thiserror::Error;

/// Authentication-related errors
#[derive(Error, Debug)]
pub enum AuthError {
    /// Missing environment variables for credentials
    #[error(
        "Missing CEDA credentials. Set CEDA_USERNAME and CEDA_PASSWORD environment variables or run 'auth setup'"
    )]
    MissingCredentials,

    /// Environment variable error
    #[error("Environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),

    /// HTTP request failed during authentication
    #[error("HTTP request failed during authentication")]
    Http(#[from] reqwest::Error),

    /// Login failed - invalid credentials or server error
    #[error("CEDA login failed. Please check your credentials and try again")]
    LoginFailed,

    /// CSRF token not found in login page
    #[error("CSRF token not found in login page. The CEDA login page format may have changed")]
    CsrfTokenNotFound,

    /// Session expired or invalid
    #[error("CEDA session expired or invalid. Please re-authenticate")]
    SessionExpired,

    /// Invalid username format
    #[error("Invalid username format: {reason}")]
    InvalidUsername { reason: String },

    /// File I/O error during credential storage
    #[error("Failed to save credentials to file")]
    CredentialStorage(#[from] std::io::Error),

    /// Permission error on credential file
    #[error("Permission denied accessing credential file: {path}")]
    PermissionDenied { path: PathBuf },
}

/// Download and HTTP client errors
#[derive(Error, Debug)]
pub enum DownloadError {
    /// HTTP request error
    #[error("HTTP request failed")]
    Http(#[from] reqwest::Error),

    /// File already exists and force flag not set
    #[error("File already exists: {path}. Use --force to overwrite")]
    FileExists { path: String },

    /// I/O error during file operations
    #[error("File I/O error")]
    Io(#[from] std::io::Error),

    /// Download timeout
    #[error("Download timed out after {seconds} seconds")]
    Timeout { seconds: u64 },

    /// Invalid URL provided
    #[error("Invalid URL: {url} - {error}")]
    InvalidUrl { url: String, error: String },

    /// Server returned error status
    #[error("Server error: HTTP {status}")]
    ServerError { status: u16 },

    /// Rate limit exceeded
    #[error("Rate limit exceeded. Server responded with HTTP 429")]
    RateLimitExceeded,

    /// Server overloaded
    #[error("Server overloaded. Server responded with HTTP 503")]
    ServerOverloaded,

    /// Circuit breaker open - too many failures
    #[error("Circuit breaker open due to repeated failures. Waiting before retry")]
    CircuitBreakerOpen,

    /// Invalid file hash - download corrupted
    #[error("File hash mismatch. Expected: {expected}, got: {actual}")]
    HashMismatch { expected: String, actual: String },

    /// File size mismatch
    #[error("File size mismatch. Expected: {expected} bytes, got: {actual} bytes")]
    SizeMismatch { expected: u64, actual: u64 },

    /// Temporary file operation failed
    #[error("Temporary file operation failed: {path}")]
    TempFileError { path: PathBuf },

    /// Atomic file operation failed
    #[error("Atomic file operation failed: could not rename {temp_path} to {final_path}")]
    AtomicOperationFailed {
        temp_path: PathBuf,
        final_path: PathBuf,
    },

    /// Maximum retries exceeded
    #[error("Maximum retry attempts ({max_retries}) exceeded for download")]
    MaxRetriesExceeded { max_retries: u32 },

    /// Incomplete download
    #[error("Incomplete download: received {received} bytes, expected {expected} bytes")]
    IncompleteDownload { received: u64, expected: u64 },

    /// Work item not found in queue
    #[error("Work item not found: {work_id}")]
    WorkNotFound { work_id: String },

    /// Generic error for other issues
    #[error("{0}")]
    Other(String),
}

/// Manifest parsing and management errors
#[derive(Error, Debug)]
pub enum ManifestError {
    /// Manifest file not found
    #[error("Manifest file not found: {path}")]
    NotFound { path: PathBuf },

    /// Invalid manifest format
    #[error("Invalid manifest format at line {line}: {content}")]
    InvalidFormat { line: usize, content: String },

    /// JSON parsing error
    #[error("JSON parsing error in manifest")]
    JsonParse(#[from] serde_json::Error),

    /// I/O error reading manifest
    #[error("I/O error reading manifest")]
    Io(#[from] std::io::Error),

    /// Manifest hash verification failed
    #[error("Manifest hash verification failed for file: {file_path}")]
    HashVerificationFailed { file_path: PathBuf },

    /// Duplicate entry in manifest
    #[error("Duplicate entry found in manifest: {hash}")]
    DuplicateEntry { hash: String },

    /// Invalid hash format
    #[error("Invalid hash format: {hash}. Expected MD5 hex string")]
    InvalidHash { hash: String },

    /// Invalid file path in manifest
    #[error("Invalid file path in manifest: {path}")]
    InvalidPath { path: String },

    /// Manifest version mismatch
    #[error("Manifest version mismatch. Expected: {expected}, found: {found}")]
    VersionMismatch { expected: String, found: String },

    /// Manifest corruption detected
    #[error("Manifest corruption detected: {reason}")]
    Corruption { reason: String },
}

/// Cache management errors
#[derive(Error, Debug)]
pub enum CacheError {
    /// Cache directory not found or inaccessible
    #[error("Cache directory not accessible: {path}")]
    DirectoryNotAccessible { path: PathBuf },

    /// Cache index corruption
    #[error("Cache index corrupted: {reason}")]
    IndexCorrupted { reason: String },

    /// Reservation conflict - file being downloaded by another worker
    #[error("File reservation conflict: {file_hash} is being downloaded by worker {worker_id}")]
    ReservationConflict { file_hash: String, worker_id: usize },

    /// Cache verification failed
    #[error("Cache verification failed for {files_failed} files")]
    VerificationFailed { files_failed: usize },

    /// Insufficient disk space
    #[error("Insufficient disk space. Required: {required} bytes, available: {available} bytes")]
    InsufficientSpace { required: u64, available: u64 },

    /// Permission denied for cache operations
    #[error("Permission denied for cache operation: {operation}")]
    PermissionDenied { operation: String },

    /// Cache lock timeout
    #[error("Cache lock timeout after {seconds} seconds")]
    LockTimeout { seconds: u64 },

    /// Invalid cache state
    #[error("Invalid cache state: {reason}")]
    InvalidState { reason: String },

    /// Cache size limit exceeded
    #[error("Cache size limit exceeded. Current: {current} bytes, limit: {limit} bytes")]
    SizeLimitExceeded { current: u64, limit: u64 },
}

/// Work queue and coordination errors
#[derive(Error, Debug)]
pub enum QueueError {
    /// Worker pool exhausted
    #[error("Worker pool exhausted. All {worker_count} workers are busy")]
    WorkerPoolExhausted { worker_count: usize },

    /// Task queue overflow
    #[error("Task queue overflow. Queue capacity: {capacity}")]
    QueueOverflow { capacity: usize },

    /// Worker panic or unexpected termination
    #[error("Worker {worker_id} panicked or terminated unexpectedly")]
    WorkerPanic { worker_id: usize },

    /// Coordinator shutdown timeout
    #[error("Coordinator shutdown timeout after {seconds} seconds")]
    ShutdownTimeout { seconds: u64 },

    /// Deadlock detected in work distribution
    #[error("Deadlock detected: {reason}")]
    Deadlock { reason: String },

    /// Invalid task state transition
    #[error("Invalid task state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    /// Channel communication error
    #[error("Channel communication error")]
    ChannelError,

    /// Task timeout
    #[error("Task timeout after {seconds} seconds")]
    TaskTimeout { seconds: u64 },
}

/// Web scraping errors
#[derive(Error, Debug)]
pub enum WebScrapingError {
    /// HTTP request failed
    #[error("Web scraping HTTP request failed")]
    Http(#[from] reqwest::Error),

    /// HTML parsing failed
    #[error("HTML parsing failed: {reason}")]
    HtmlParsing { reason: String },

    /// CSS selector error
    #[error("Invalid CSS selector: {selector}")]
    InvalidSelector { selector: String },

    /// Expected element not found
    #[error("Expected element not found: {selector}")]
    ElementNotFound { selector: String },

    /// Rate limit exceeded during scraping
    #[error("Web scraping rate limit exceeded")]
    RateLimitExceeded,

    /// Invalid URL discovered during scraping
    #[error("Invalid URL discovered: {url}")]
    InvalidUrl { url: String },

    /// Scraping timeout
    #[error("Web scraping timeout after {seconds} seconds")]
    Timeout { seconds: u64 },
}

/// Configuration errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Configuration file not found
    #[error("Configuration file not found: {path}")]
    NotFound { path: PathBuf },

    /// Invalid configuration format
    #[error("Invalid configuration format")]
    InvalidFormat(#[from] toml::de::Error),

    /// Missing required configuration field
    #[error("Missing required configuration field: {field}")]
    MissingField { field: String },

    /// Invalid configuration value
    #[error("Invalid configuration value for {field}: {value}. {reason}")]
    InvalidValue {
        field: String,
        value: String,
        reason: String,
    },

    /// Environment variable expansion failed
    #[error("Environment variable expansion failed: {var}")]
    EnvExpansionFailed { var: String },

    /// Configuration validation failed
    #[error("Configuration validation failed: {errors:?}")]
    ValidationFailed { errors: Vec<String> },
}

/// Progress reporting errors
#[derive(Error, Debug)]
pub enum ProgressError {
    /// Progress channel closed
    #[error("Progress reporting channel closed")]
    ChannelClosed,

    /// Progress calculation error
    #[error("Progress calculation error: {reason}")]
    CalculationError { reason: String },

    /// Terminal output error
    #[error("Terminal output error")]
    TerminalError(#[from] std::io::Error),

    /// Progress state inconsistency
    #[error("Progress state inconsistency: {reason}")]
    StateInconsistency { reason: String },
}

/// Top-level application error that can represent any error type
#[derive(Error, Debug)]
pub enum AppError {
    /// Authentication error
    #[error(transparent)]
    Auth(#[from] AuthError),

    /// Download error
    #[error(transparent)]
    Download(#[from] DownloadError),

    /// Manifest error
    #[error(transparent)]
    Manifest(#[from] ManifestError),

    /// Cache error
    #[error(transparent)]
    Cache(#[from] CacheError),

    /// Queue error
    #[error(transparent)]
    Queue(#[from] QueueError),

    /// Web scraping error
    #[error(transparent)]
    WebScraping(#[from] WebScrapingError),

    /// Configuration error
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Progress error
    #[error(transparent)]
    Progress(#[from] ProgressError),

    /// Generic I/O error
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Generic application error with context
    #[error("Application error: {message}")]
    Generic { message: String },
}

impl AppError {
    /// Create a generic application error with a message
    pub fn generic(message: impl Into<String>) -> Self {
        Self::Generic {
            message: message.into(),
        }
    }

    /// Check if the error is recoverable (transient)
    pub fn is_recoverable(&self) -> bool {
        match self {
            AppError::Download(DownloadError::Timeout { .. })
            | AppError::Download(DownloadError::RateLimitExceeded)
            | AppError::Download(DownloadError::ServerOverloaded)
            | AppError::Download(DownloadError::Http(_))
            | AppError::Auth(AuthError::Http(_))
            | AppError::WebScraping(WebScrapingError::Http(_))
            | AppError::WebScraping(WebScrapingError::RateLimitExceeded)
            | AppError::Cache(CacheError::LockTimeout { .. })
            | AppError::Queue(QueueError::TaskTimeout { .. }) => true,

            AppError::Auth(AuthError::LoginFailed)
            | AppError::Download(DownloadError::FileExists { .. })
            | AppError::Download(DownloadError::MaxRetriesExceeded { .. })
            | AppError::Manifest(ManifestError::InvalidFormat { .. })
            | AppError::Config(ConfigError::InvalidFormat(_)) => false,

            _ => false,
        }
    }

    /// Get error category for logging and metrics
    pub fn category(&self) -> &'static str {
        match self {
            AppError::Auth(_) => "authentication",
            AppError::Download(_) => "download",
            AppError::Manifest(_) => "manifest",
            AppError::Cache(_) => "cache",
            AppError::Queue(_) => "queue",
            AppError::WebScraping(_) => "scraping",
            AppError::Config(_) => "config",
            AppError::Progress(_) => "progress",
            AppError::Io(_) => "io",
            AppError::Generic { .. } => "generic",
        }
    }
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, AppError>;

/// Authentication result type alias
pub type AuthResult<T> = std::result::Result<T, AuthError>;

/// Download result type alias
pub type DownloadResult<T> = std::result::Result<T, DownloadError>;

/// Manifest result type alias
pub type ManifestResult<T> = std::result::Result<T, ManifestError>;

/// Cache result type alias
pub type CacheResult<T> = std::result::Result<T, CacheError>;

/// Queue result type alias
pub type QueueResult<T> = std::result::Result<T, QueueError>;

// Additional error conversions for worker module
impl From<CacheError> for DownloadError {
    fn from(cache_error: CacheError) -> Self {
        match cache_error {
            CacheError::DirectoryNotAccessible { path } => DownloadError::TempFileError { path },
            CacheError::VerificationFailed { .. } => DownloadError::HashMismatch {
                expected: "unknown".to_string(),
                actual: "unknown".to_string(),
            },
            CacheError::InvalidState { reason } => DownloadError::TempFileError {
                path: PathBuf::from(reason),
            },
            _ => DownloadError::TempFileError {
                path: PathBuf::from("cache_error"),
            },
        }
    }
}
