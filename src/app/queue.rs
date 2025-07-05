//! Work-stealing queue system for concurrent downloads
//!
//! This module implements a work-stealing queue that prevents worker starvation
//! and ensures efficient distribution of download tasks across multiple workers.
//! The design follows the principles outlined in the PRP specification.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::app::hash::Md5Hash;
use crate::app::models::FileInfo;
use crate::constants::workers;
use crate::errors::{DownloadError, DownloadResult};

/// Status of a work item in the queue system
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkStatus {
    /// Work is waiting to be claimed by a worker
    Pending,
    /// Work is currently being processed by a worker
    InProgress {
        worker_id: u32,
        started_at: DateTime<Utc>,
        previous_failures: u32,
    },
    /// Work completed successfully
    Completed { completed_at: DateTime<Utc> },
    /// Work failed and is waiting for retry
    Failed {
        failure_count: u32,
        last_failure: DateTime<Utc>,
        error: String,
    },
    /// Work failed permanently (exceeded retry limit)
    Abandoned {
        failure_count: u32,
        abandoned_at: DateTime<Utc>,
        final_error: String,
    },
}

impl WorkStatus {
    /// Check if this work item is ready for retry
    pub fn is_ready_for_retry(&self, retry_delay: Duration) -> bool {
        match self {
            WorkStatus::Failed { last_failure, .. } => {
                let elapsed = Utc::now()
                    .signed_duration_since(*last_failure)
                    .to_std()
                    .unwrap_or(Duration::ZERO);
                elapsed >= retry_delay
            }
            _ => false,
        }
    }

    /// Get failure count if this is a failed work item
    pub fn failure_count(&self) -> u32 {
        match self {
            WorkStatus::Failed { failure_count, .. } => *failure_count,
            WorkStatus::Abandoned { failure_count, .. } => *failure_count,
            WorkStatus::InProgress {
                previous_failures, ..
            } => *previous_failures,
            _ => 0,
        }
    }
}

/// Information about work being processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkInfo {
    /// The file to be downloaded
    pub file_info: FileInfo,
    /// Current status of this work
    pub status: WorkStatus,
    /// When this work was first added to the queue
    pub created_at: DateTime<Utc>,
    /// Priority of this work (higher = more important)
    pub priority: u32,
}

impl WorkInfo {
    /// Create new work info for a file
    pub fn new(file_info: FileInfo) -> Self {
        Self {
            file_info,
            status: WorkStatus::Pending,
            created_at: Utc::now(),
            priority: workers::DEFAULT_PRIORITY,
        }
    }

    /// Create new work info with custom priority
    pub fn with_priority(file_info: FileInfo, priority: u32) -> Self {
        Self {
            file_info,
            status: WorkStatus::Pending,
            created_at: Utc::now(),
            priority,
        }
    }

    /// Get the unique identifier for this work (file hash)
    pub fn work_id(&self) -> &Md5Hash {
        &self.file_info.hash
    }

    /// Check if this work is ready to be processed
    pub fn is_available(&self, retry_delay: Duration) -> bool {
        matches!(self.status, WorkStatus::Pending) || self.status.is_ready_for_retry(retry_delay)
    }

    /// Check if this work is currently being processed
    pub fn is_in_progress(&self) -> bool {
        matches!(self.status, WorkStatus::InProgress { .. })
    }

    /// Check if this work is completed
    pub fn is_completed(&self) -> bool {
        matches!(self.status, WorkStatus::Completed { .. })
    }

    /// Check if this work has failed permanently
    pub fn is_abandoned(&self) -> bool {
        matches!(self.status, WorkStatus::Abandoned { .. })
    }
}

/// Configuration for the work queue
#[derive(Debug, Clone)]
pub struct WorkQueueConfig {
    /// Maximum number of retry attempts per file
    pub max_retries: u32,
    /// Delay between retry attempts
    pub retry_delay: Duration,
    /// Maximum number of workers that can process work simultaneously
    pub max_workers: u32,
    /// Timeout for work items (if a worker doesn't report progress)
    pub work_timeout: Duration,
    /// Maximum size of pending queue (memory limit)
    pub max_pending_items: usize,
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            max_retries: workers::MAX_RETRIES,
            retry_delay: workers::RETRY_DELAY,
            max_workers: workers::DEFAULT_WORKER_COUNT as u32,
            work_timeout: workers::WORK_TIMEOUT,
            max_pending_items: workers::MAX_PENDING_ITEMS,
        }
    }
}

/// Statistics about queue operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    /// Total work items added to queue
    pub total_added: u64,
    /// Work items currently pending
    pub pending_count: u64,
    /// Work items currently in progress
    pub in_progress_count: u64,
    /// Work items completed successfully
    pub completed_count: u64,
    /// Work items failed and awaiting retry
    pub failed_count: u64,
    /// Work items permanently abandoned
    pub abandoned_count: u64,
    /// Total duplicate items encountered
    pub duplicate_count: u64,
    /// Average processing time for completed items
    pub avg_processing_time: Duration,
    /// Queue creation time
    pub created_at: DateTime<Utc>,
}

impl QueueStats {
    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total_processed = self.completed_count + self.abandoned_count;
        if total_processed == 0 {
            0.0
        } else {
            (self.completed_count as f64 / total_processed as f64) * 100.0
        }
    }

    /// Calculate total active items (pending + in_progress + failed)
    pub fn active_count(&self) -> u64 {
        self.pending_count + self.in_progress_count + self.failed_count
    }

    /// Calculate worker utilization (in_progress / max_workers)
    pub fn worker_utilization(&self, max_workers: u32) -> f64 {
        if max_workers == 0 {
            0.0
        } else {
            (self.in_progress_count as f64 / max_workers as f64).min(1.0) * 100.0
        }
    }
}

/// Work-stealing queue state
#[derive(Debug)]
struct QueueState {
    /// Pending work items ordered by priority
    pending: VecDeque<Md5Hash>, // work_id queue
    /// All work items indexed by work_id (file hash)
    work_items: HashMap<Md5Hash, WorkInfo>,
    /// Completed work IDs for fast duplicate checking
    completed: HashSet<Md5Hash>,
    /// Current statistics
    stats: QueueStats,
    /// Next worker ID for assignment
    next_worker_id: u32,
}

impl QueueState {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            work_items: HashMap::new(),
            completed: HashSet::new(),
            stats: QueueStats {
                created_at: Utc::now(),
                ..Default::default()
            },
            next_worker_id: 1,
        }
    }

    /// Get next available work ID from pending queue
    fn pop_pending(&mut self) -> Option<Md5Hash> {
        self.pending.pop_front()
    }

    /// Add work ID to pending queue
    fn push_pending(&mut self, work_id: Md5Hash) {
        self.pending.push_back(work_id);
    }

    /// Get work item by ID
    fn get_work(&self, work_id: &Md5Hash) -> Option<&WorkInfo> {
        self.work_items.get(work_id)
    }

    /// Get mutable work item by ID
    fn get_work_mut(&mut self, work_id: &Md5Hash) -> Option<&mut WorkInfo> {
        self.work_items.get_mut(work_id)
    }

    /// Update queue statistics
    fn update_stats(&mut self) {
        self.stats.pending_count = self.pending.len() as u64;
        self.stats.in_progress_count = self
            .work_items
            .values()
            .filter(|w| w.is_in_progress())
            .count() as u64;
        self.stats.completed_count = self.completed.len() as u64;
        self.stats.failed_count = self
            .work_items
            .values()
            .filter(|w| matches!(w.status, WorkStatus::Failed { .. }))
            .count() as u64;
        self.stats.abandoned_count = self
            .work_items
            .values()
            .filter(|w| w.is_abandoned())
            .count() as u64;
    }
}

/// Work-stealing queue for concurrent downloads
///
/// This queue prevents worker starvation by ensuring workers never wait for
/// specific files and always seek new work when available.
#[derive(Debug)]
pub struct WorkQueue {
    /// Configuration for queue behavior
    config: WorkQueueConfig,
    /// Shared state protected by async mutex
    state: Arc<Mutex<QueueState>>,
    /// Read-write lock for statistics (allows concurrent reads)
    stats_cache: Arc<RwLock<QueueStats>>,
}

impl WorkQueue {
    /// Create a new work queue with default configuration
    pub fn new() -> Self {
        Self::with_config(WorkQueueConfig::default())
    }

    /// Create a new work queue with custom configuration
    pub fn with_config(config: WorkQueueConfig) -> Self {
        let state = Arc::new(Mutex::new(QueueState::new()));
        let stats_cache = Arc::new(RwLock::new(QueueStats {
            created_at: Utc::now(),
            ..Default::default()
        }));

        Self {
            config,
            state,
            stats_cache,
        }
    }

    /// Add a file to the work queue
    ///
    /// # Arguments
    ///
    /// * `file_info` - File information to queue for download
    ///
    /// # Returns
    ///
    /// `Ok(true)` if work was added, `Ok(false)` if duplicate, `Err` on error
    pub async fn add_work(&self, file_info: FileInfo) -> DownloadResult<bool> {
        self.add_work_with_priority(file_info, workers::DEFAULT_PRIORITY)
            .await
    }

    /// Add a file to the work queue with custom priority
    ///
    /// # Arguments
    ///
    /// * `file_info` - File information to queue for download
    /// * `priority` - Priority level (higher = more important)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if work was added, `Ok(false)` if duplicate, `Err` on error
    pub async fn add_work_with_priority(
        &self,
        file_info: FileInfo,
        priority: u32,
    ) -> DownloadResult<bool> {
        let mut state = self.state.lock().await;
        let work_id = file_info.hash;

        // Check if this work is already completed
        if state.completed.contains(&work_id) {
            state.stats.duplicate_count += 1;
            debug!("Skipping duplicate completed work: {}", work_id);
            let stats = state.stats.clone();
            drop(state); // Release the lock before async operation
            self.update_stats_cache(&stats).await;
            return Ok(false);
        }

        // Check if this work already exists
        if state.work_items.contains_key(&work_id) {
            state.stats.duplicate_count += 1;
            debug!("Skipping duplicate existing work: {}", work_id);
            let stats = state.stats.clone();
            drop(state); // Release the lock before async operation
            self.update_stats_cache(&stats).await;
            return Ok(false);
        }

        // Check memory limits
        if state.pending.len() >= self.config.max_pending_items {
            return Err(DownloadError::QueueFull {
                current_size: state.pending.len(),
                max_size: self.config.max_pending_items,
            });
        }

        // Create work info
        let work_info = WorkInfo::with_priority(file_info, priority);

        // Add to pending queue and work items
        state.work_items.insert(work_id, work_info);
        state.push_pending(work_id);
        state.stats.total_added += 1;

        debug!("Added work to queue: {} (priority: {})", work_id, priority);

        // Update statistics
        state.update_stats();
        self.update_stats_cache(&state.stats).await;

        Ok(true)
    }

    /// Add multiple files to the work queue efficiently (bulk operation)
    pub async fn add_work_bulk(&self, file_infos: Vec<FileInfo>) -> DownloadResult<usize> {
        let total_files = file_infos.len();
        let mut state = self.state.lock().await;
        let mut added_count = 0;
        let mut skipped_duplicates = 0;
        let mut hit_capacity_limit = false;

        info!("Starting bulk add of {} files to queue", total_files);
        info!(
            "Current queue state: pending={}, total_items={}",
            state.pending.len(),
            state.work_items.len()
        );

        for file_info in file_infos {
            let work_id = file_info.hash;

            // Skip if already completed or exists
            if state.completed.contains(&work_id) || state.work_items.contains_key(&work_id) {
                state.stats.duplicate_count += 1;
                skipped_duplicates += 1;
                continue;
            }

            // Check memory limits (only count pending items, not completed/failed ones)
            let current_pending_count = state.pending.len();
            if current_pending_count + added_count >= self.config.max_pending_items {
                hit_capacity_limit = true;
                let total_work_items = state.work_items.len();
                warn!(
                    "Queue capacity limit reached: {} pending + {} new = {} items (limit: {}, total work items: {})",
                    current_pending_count,
                    added_count,
                    current_pending_count + added_count,
                    self.config.max_pending_items,
                    total_work_items
                );
                break;
            }

            // Create and add work info
            let work_info = WorkInfo::with_priority(file_info, workers::DEFAULT_PRIORITY);
            state.work_items.insert(work_id, work_info);
            state.push_pending(work_id);
            added_count += 1;
        }

        // Update stats once at the end
        state.stats.total_added += added_count as u64;
        state.update_stats();

        // Clone stats before dropping the lock
        let stats = state.stats.clone();
        drop(state);

        // Update stats cache once outside the lock
        self.update_stats_cache(&stats).await;

        info!(
            "Bulk add completed: {}/{} files added, {} duplicates skipped",
            added_count, total_files, skipped_duplicates
        );

        if hit_capacity_limit {
            let remaining = total_files - added_count - skipped_duplicates;
            warn!(
                "Queue capacity limit reached - {} files not added due to capacity constraints",
                remaining
            );
        }

        Ok(added_count)
    }

    /// Get next available work item for a worker
    ///
    /// This is the core work-stealing operation that prevents worker starvation.
    /// Workers call this method to atomically claim work without waiting for
    /// specific files.
    ///
    /// # Returns
    ///
    /// `Some(WorkInfo)` if work is available, `None` if queue is empty
    pub async fn get_next_work(&self) -> Option<WorkInfo> {
        let mut state = self.state.lock().await;

        // First, try to find pending work
        while let Some(work_id) = state.pop_pending() {
            // Check if work is available and get worker ID
            let (is_available, worker_id) = {
                if let Some(work_info) = state.get_work(&work_id) {
                    (
                        work_info.is_available(self.config.retry_delay),
                        state.next_worker_id,
                    )
                } else {
                    (false, 0)
                }
            };

            if is_available {
                // Get the work info for cloning
                let work_info = state.get_work(&work_id).unwrap().clone();

                // Update state
                state.next_worker_id += 1;

                // Create claimed work
                let mut claimed_work = work_info;
                claimed_work.status = WorkStatus::InProgress {
                    worker_id,
                    started_at: Utc::now(),
                    previous_failures: 0,
                };

                // Update the work item in state
                if let Some(work_info_mut) = state.get_work_mut(&work_id) {
                    work_info_mut.status = claimed_work.status.clone();
                }

                debug!("Claimed work {} for worker {}", work_id, worker_id);

                // Update statistics
                state.update_stats();
                let stats = state.stats.clone();
                drop(state); // Release the lock before async operation
                self.update_stats_cache(&stats).await;

                return Some(claimed_work);
            }
        }

        // No pending work found, try failed work ready for retry
        let retry_candidates: Vec<Md5Hash> = state
            .work_items
            .iter()
            .filter_map(|(work_id, work_info)| {
                if matches!(work_info.status, WorkStatus::Failed { .. })
                    && work_info.status.is_ready_for_retry(self.config.retry_delay)
                {
                    Some(*work_id)
                } else {
                    None
                }
            })
            .collect();

        if let Some(work_id) = retry_candidates.first() {
            // Get work info and worker ID
            let (work_info, worker_id, previous_failure_count) = {
                if let Some(work_info) = state.get_work(work_id) {
                    let previous_failure_count = work_info.status.failure_count();
                    (
                        Some(work_info.clone()),
                        state.next_worker_id,
                        previous_failure_count,
                    )
                } else {
                    (None, 0, 0)
                }
            };

            if let Some(work_info) = work_info {
                // Update state
                state.next_worker_id += 1;

                // Create claimed work
                let mut claimed_work = work_info;
                claimed_work.status = WorkStatus::InProgress {
                    worker_id,
                    started_at: Utc::now(),
                    previous_failures: previous_failure_count,
                };

                // Update the work item in state
                if let Some(work_info_mut) = state.get_work_mut(work_id) {
                    work_info_mut.status = claimed_work.status.clone();
                }

                debug!("Claimed retry work {} for worker {}", work_id, worker_id);

                // Update statistics
                state.update_stats();
                let stats = state.stats.clone();
                drop(state); // Release the lock before async operation
                self.update_stats_cache(&stats).await;

                return Some(claimed_work);
            }
        }

        // No work available
        None
    }

    /// Mark work as completed successfully
    ///
    /// # Arguments
    ///
    /// * `work_id` - ID of the completed work (file hash)
    pub async fn mark_completed(&self, work_id: &Md5Hash) -> DownloadResult<()> {
        let stats = {
            let mut state = self.state.lock().await;

            if let Some(work_info) = state.work_items.get_mut(work_id) {
                work_info.status = WorkStatus::Completed {
                    completed_at: Utc::now(),
                };

                // Move to completed set for fast duplicate checking
                state.completed.insert(*work_id);

                info!("Marked work completed: {}", work_id);

                // Update statistics
                state.update_stats();
                state.stats.clone()
            } else {
                return Err(DownloadError::WorkNotFound {
                    work_id: work_id.to_string(),
                });
            }
        };

        self.update_stats_cache(&stats).await;
        Ok(())
    }

    /// Mark work as failed
    ///
    /// # Arguments
    ///
    /// * `work_id` - ID of the failed work (file hash)
    /// * `error` - Error that caused the failure
    pub async fn mark_failed(&self, work_id: &Md5Hash, error: &str) -> DownloadResult<()> {
        let stats = {
            let mut state = self.state.lock().await;

            if let Some(work_info) = state.work_items.get_mut(work_id) {
                let failure_count = work_info.status.failure_count() + 1;

                if failure_count >= self.config.max_retries {
                    // Permanently abandon this work
                    work_info.status = WorkStatus::Abandoned {
                        failure_count,
                        abandoned_at: Utc::now(),
                        final_error: error.to_string(),
                    };

                    warn!(
                        "Abandoned work after {} failures: {} - {}",
                        failure_count, work_id, error
                    );
                } else {
                    // Mark for retry
                    work_info.status = WorkStatus::Failed {
                        failure_count,
                        last_failure: Utc::now(),
                        error: error.to_string(),
                    };

                    warn!(
                        "Marked work failed (attempt {}/{}): {} - {}",
                        failure_count, self.config.max_retries, work_id, error
                    );
                }

                // Update statistics
                state.update_stats();
                state.stats.clone()
            } else {
                return Err(DownloadError::WorkNotFound {
                    work_id: work_id.to_string(),
                });
            }
        };

        self.update_stats_cache(&stats).await;
        Ok(())
    }

    /// Get current queue statistics
    pub async fn stats(&self) -> QueueStats {
        self.stats_cache.read().await.clone()
    }

    /// Check if the queue has any work available
    pub async fn has_work_available(&self) -> bool {
        let state = self.state.lock().await;

        // Check for pending work
        if !state.pending.is_empty() {
            return true;
        }

        // Check for failed work ready for retry
        state.work_items.values().any(|work_info| {
            matches!(work_info.status, WorkStatus::Failed { .. })
                && work_info.status.is_ready_for_retry(self.config.retry_delay)
        })
    }

    /// Check if all work is completed or abandoned
    pub async fn is_finished(&self) -> bool {
        let state = self.state.lock().await;

        // Queue is finished if no work is pending, in progress, or available for retry
        state.pending.is_empty()
            && !state.work_items.values().any(|work_info| {
                work_info.is_in_progress()
                    || (matches!(work_info.status, WorkStatus::Failed { .. })
                        && work_info.status.is_ready_for_retry(self.config.retry_delay))
            })
    }

    /// Get work item by ID (for debugging/monitoring)
    pub async fn get_work_info(&self, work_id: &Md5Hash) -> Option<WorkInfo> {
        let state = self.state.lock().await;
        state.get_work(work_id).cloned()
    }

    /// Get all work items (for debugging/monitoring)
    pub async fn get_all_work(&self) -> Vec<WorkInfo> {
        let state = self.state.lock().await;
        state.work_items.values().cloned().collect()
    }

    /// Clear all completed and abandoned work to free memory
    pub async fn cleanup(&self) -> usize {
        let (removed, stats) = {
            let mut state = self.state.lock().await;
            let initial_count = state.work_items.len();

            // Remove completed and abandoned work items
            state
                .work_items
                .retain(|_, work_info| !work_info.is_completed() && !work_info.is_abandoned());

            let removed = initial_count - state.work_items.len();

            if removed > 0 {
                info!("Cleaned up {} completed/abandoned work items", removed);

                // Update statistics
                state.update_stats();
            }

            (removed, state.stats.clone())
        };

        if removed > 0 {
            self.update_stats_cache(&stats).await;
        }

        removed
    }

    /// Reset the queue (clear all work)
    pub async fn reset(&self) {
        let stats = {
            let mut state = self.state.lock().await;

            state.pending.clear();
            state.work_items.clear();
            state.completed.clear();
            state.stats = QueueStats {
                created_at: Utc::now(),
                ..Default::default()
            };
            state.next_worker_id = 1;

            info!("Reset work queue");

            state.stats.clone()
        };

        // Update statistics cache
        self.update_stats_cache(&stats).await;
    }

    /// Update statistics cache for fast read access
    async fn update_stats_cache(&self, stats: &QueueStats) {
        let mut cache = self.stats_cache.write().await;
        *cache = stats.clone();
    }

    /// Handle timed-out work items (workers that haven't reported progress)
    pub async fn handle_timeouts(&self) -> usize {
        let (timed_out, stats) = {
            let mut state = self.state.lock().await;
            let now = Utc::now();
            let mut timed_out = 0;

            // Find work items that have timed out
            let timeout_candidates: Vec<Md5Hash> = state
                .work_items
                .iter()
                .filter_map(|(work_id, work_info)| {
                    if let WorkStatus::InProgress { started_at, .. } = work_info.status {
                        let elapsed = now
                            .signed_duration_since(started_at)
                            .to_std()
                            .unwrap_or(Duration::ZERO);

                        if elapsed > self.config.work_timeout {
                            Some(*work_id)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            // Mark timed-out work as failed
            for work_id in timeout_candidates {
                if let Some(work_info) = state.get_work_mut(&work_id) {
                    let failure_count = work_info.status.failure_count() + 1;

                    if failure_count >= self.config.max_retries {
                        work_info.status = WorkStatus::Abandoned {
                            failure_count,
                            abandoned_at: now,
                            final_error: "Worker timeout".to_string(),
                        };
                    } else {
                        work_info.status = WorkStatus::Failed {
                            failure_count,
                            last_failure: now,
                            error: "Worker timeout".to_string(),
                        };

                        // Add back to pending queue for retry
                        state.push_pending(work_id);
                    }

                    timed_out += 1;
                    warn!("Work item timed out: {}", work_id);
                }
            }

            if timed_out > 0 {
                // Update statistics
                state.update_stats();
                info!("Handled {} timed-out work items", timed_out);
            }

            (timed_out, state.stats.clone())
        };

        if timed_out > 0 {
            self.update_stats_cache(&stats).await;
        }

        timed_out
    }
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::sleep;

    // Store temp dir to keep it alive for tests
    static TEMP_DIR_STORAGE: std::sync::OnceLock<TempDir> = std::sync::OnceLock::new();

    fn get_temp_dir() -> &'static TempDir {
        TEMP_DIR_STORAGE.get_or_init(|| TempDir::new().unwrap())
    }

    fn create_test_file_info(hash: &str, path: &str) -> FileInfo {
        let temp_dir = get_temp_dir();
        // Generate a consistent valid MD5 hash from the input
        let valid_hash = if hash.len() == 32 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
            hash.to_string()
        } else {
            // Create a deterministic MD5-like hash from the input
            let hash_num = hash
                .chars()
                .fold(0u64, |acc, c| acc.wrapping_mul(31).wrapping_add(c as u64));
            format!("{:032x}", hash_num as u128)
        };

        // Generate a valid MIDAS path if the provided path is simple
        let valid_path = if path.contains("uk-daily-temperature-obs") {
            path.to_string()
        } else {
            // Generate a valid MIDAS path with variation based on hash
            let station_num = if hash == "hash1" {
                "01381"
            } else if hash == "hash2" {
                "01382"
            } else {
                "01383"
            };
            format!(
                "./data/uk-daily-temperature-obs/dataset-version-202407/devon/{}_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_{}_twist_qcv-1_1980.csv",
                station_num, station_num
            )
        };

        let hash_obj = Md5Hash::from_hex(&valid_hash).unwrap();
        FileInfo::new(hash_obj, valid_path, temp_dir.path()).unwrap()
    }

    /// Test basic queue operations: add, get, complete
    #[tokio::test]
    async fn test_basic_queue_operations() {
        let queue = WorkQueue::new();

        // Add work
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        let file_hash = file_info.hash;
        let added = queue.add_work(file_info.clone()).await.unwrap();
        assert!(added);

        // Check stats
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 1);
        assert_eq!(stats.pending_count, 1);

        // Get work
        let work = queue.get_next_work().await.unwrap();
        assert_eq!(work.work_id(), &file_hash);
        assert!(work.is_in_progress());

        // Complete work
        queue.mark_completed(&file_hash).await.unwrap();

        // Check final stats
        let stats = queue.stats().await;
        assert_eq!(stats.completed_count, 1);
        assert_eq!(stats.in_progress_count, 0);
    }

    /// Test duplicate detection
    #[tokio::test]
    async fn test_duplicate_detection() {
        let queue = WorkQueue::new();

        // Add same work twice
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        let added1 = queue.add_work(file_info.clone()).await.unwrap();
        let added2 = queue.add_work(file_info).await.unwrap();

        assert!(added1);
        assert!(!added2); // Duplicate should be rejected

        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 1);
        assert_eq!(stats.duplicate_count, 1);
    }

    /// Test retry mechanism
    #[tokio::test]
    async fn test_retry_mechanism() {
        let config = WorkQueueConfig {
            retry_delay: Duration::from_millis(10),
            max_retries: 2,
            ..Default::default()
        };
        let queue = WorkQueue::with_config(config);

        // Add and get work
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        assert_eq!(work.work_id(), &file_hash);

        // Mark as failed
        queue.mark_failed(&file_hash, "Test error").await.unwrap();

        // Check that work is now failed with count 1
        let failed_work = queue.get_work_info(&file_hash).await.unwrap();
        assert_eq!(failed_work.status.failure_count(), 1);

        // Should not be available immediately
        assert!(queue.get_next_work().await.is_none());

        // Wait for retry delay
        sleep(Duration::from_millis(20)).await;

        // Should be available for retry (becomes InProgress when claimed)
        let retry_work = queue.get_next_work().await.unwrap();
        assert_eq!(retry_work.work_id(), &file_hash);
        assert!(retry_work.is_in_progress());
    }

    /// Test work abandonment after max retries
    #[tokio::test]
    async fn test_work_abandonment() {
        let config = WorkQueueConfig {
            retry_delay: Duration::from_millis(1),
            max_retries: 2,
            ..Default::default()
        };
        let queue = WorkQueue::with_config(config);

        // Add work
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        // Fail it multiple times (need to respect retry delay)
        for i in 0..3 {
            if let Some(work) = queue.get_next_work().await {
                queue
                    .mark_failed(work.work_id(), &format!("Error {}", i + 1))
                    .await
                    .unwrap();

                // Wait for retry delay so work becomes available again
                sleep(Duration::from_millis(2)).await;
            } else {
                // No work available, break
                break;
            }
        }

        // Should be abandoned now (max_retries = 2, so after 2 failures it's abandoned)
        let work_info = queue.get_work_info(&file_hash).await.unwrap();
        assert!(work_info.is_abandoned());

        let stats = queue.stats().await;
        assert_eq!(stats.abandoned_count, 1);
    }

    /// Test worker starvation prevention
    #[tokio::test]
    async fn test_no_worker_starvation() {
        let queue = WorkQueue::new();

        // Add multiple work items
        for i in 1..=5 {
            let file_info =
                create_test_file_info(&format!("hash{}", i), &format!("./data/test{}.csv", i));
            queue.add_work(file_info).await.unwrap();
        }

        // Simulate multiple workers claiming work simultaneously
        let mut claimed_work = Vec::new();
        for _ in 0..5 {
            if let Some(work) = queue.get_next_work().await {
                claimed_work.push(work.work_id().to_string());
            }
        }

        // All work should be claimed by different workers
        assert_eq!(claimed_work.len(), 5);

        // No additional work should be available
        assert!(queue.get_next_work().await.is_none());

        let stats = queue.stats().await;
        assert_eq!(stats.in_progress_count, 5);
    }

    /// Test queue capacity limits
    #[tokio::test]
    async fn test_queue_capacity_limits() {
        let config = WorkQueueConfig {
            max_pending_items: 2,
            ..Default::default()
        };
        let queue = WorkQueue::with_config(config);

        // Add work up to limit
        for i in 1..=2 {
            let file_info =
                create_test_file_info(&format!("hash{}", i), &format!("./data/test{}.csv", i));
            let result = queue.add_work(file_info).await;
            assert!(result.is_ok());
        }

        // Adding beyond limit should fail
        let file_info = create_test_file_info("hash3", "./data/test3.csv");
        let result = queue.add_work(file_info).await;
        assert!(result.is_err());

        if let Err(DownloadError::QueueFull {
            current_size,
            max_size,
        }) = result
        {
            assert_eq!(current_size, 2);
            assert_eq!(max_size, 2);
        } else {
            panic!("Expected QueueFull error");
        }
    }

    /// Test timeout handling
    #[tokio::test]
    async fn test_timeout_handling() {
        let config = WorkQueueConfig {
            work_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let queue = WorkQueue::with_config(config);

        // Add and claim work
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        let file_hash = file_info.hash;
        queue.add_work(file_info).await.unwrap();

        let work = queue.get_next_work().await.unwrap();
        assert!(work.is_in_progress());

        // Wait for timeout
        sleep(Duration::from_millis(20)).await;

        // Handle timeouts
        let timed_out = queue.handle_timeouts().await;
        assert_eq!(timed_out, 1);

        // Work should be failed and available for retry
        let work_info = queue.get_work_info(&file_hash).await.unwrap();
        assert!(matches!(work_info.status, WorkStatus::Failed { .. }));
    }

    /// Test queue cleanup
    #[tokio::test]
    async fn test_queue_cleanup() {
        let queue = WorkQueue::new();

        // Add and complete some work
        for i in 1..=3 {
            let file_info =
                create_test_file_info(&format!("hash{}", i), &format!("./data/test{}.csv", i));
            queue.add_work(file_info).await.unwrap();

            let work = queue.get_next_work().await.unwrap();
            queue.mark_completed(work.work_id()).await.unwrap();
        }

        // Cleanup should remove completed work
        let removed = queue.cleanup().await;
        assert_eq!(removed, 3);

        // Check that work items were actually removed
        let all_work = queue.get_all_work().await;
        assert!(all_work.is_empty());
    }

    /// Test queue reset
    #[tokio::test]
    async fn test_queue_reset() {
        let queue = WorkQueue::new();

        // Add some work
        let file_info = create_test_file_info("hash1", "./data/test1.csv");
        queue.add_work(file_info).await.unwrap();

        // Reset queue
        queue.reset().await;

        // Queue should be empty
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 0);
        assert_eq!(stats.pending_count, 0);

        assert!(queue.get_next_work().await.is_none());
        assert!(queue.is_finished().await);
    }

    /// Test concurrent access (basic thread safety)
    #[tokio::test]
    async fn test_concurrent_access() {
        let queue = Arc::new(WorkQueue::new());

        // Add work concurrently
        let mut handles = Vec::new();
        for i in 1..=10 {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                let file_info =
                    create_test_file_info(&format!("hash{}", i), &format!("./data/test{}.csv", i));
                queue_clone.add_work(file_info).await
            });
            handles.push(handle);
        }

        // Wait for all additions to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Check that all work was added
        let stats = queue.stats().await;
        assert_eq!(stats.total_added, 10);
        assert_eq!(stats.pending_count, 10);
    }
}
