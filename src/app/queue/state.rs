//! Internal state management for the work queue
//!
//! This module handles the low-level state operations for the work queue,
//! including work item storage, pending queue management, and atomic state transitions.

use std::collections::{HashMap, HashSet, VecDeque};

use tracing::debug;

use crate::app::hash::Md5Hash;
use super::types::{QueueStats, WorkInfo, WorkStatus};

/// Internal state of the work queue
#[derive(Debug)]
pub struct QueueState {
    /// Pending work items ordered by priority
    pending: VecDeque<Md5Hash>,
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
    /// Create new empty queue state
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            work_items: HashMap::new(),
            completed: HashSet::new(),
            stats: QueueStats::new(),
            next_worker_id: 1,
        }
    }

    /// Get the next worker ID and increment counter
    pub fn next_worker_id(&mut self) -> u32 {
        let id = self.next_worker_id;
        self.next_worker_id += 1;
        id
    }

    /// Check if work is already completed
    pub fn is_completed(&self, work_id: &Md5Hash) -> bool {
        self.completed.contains(work_id)
    }

    /// Check if work already exists
    pub fn contains_work(&self, work_id: &Md5Hash) -> bool {
        self.work_items.contains_key(work_id)
    }

    /// Add work item to the queue
    pub fn add_work(&mut self, work_info: WorkInfo) -> Result<(), String> {
        let work_id = *work_info.work_id();

        if self.completed.contains(&work_id) {
            return Err("Work already completed".to_string());
        }

        if self.work_items.contains_key(&work_id) {
            return Err("Work already exists".to_string());
        }

        // Insert work item
        self.work_items.insert(work_id, work_info);
        
        // Add to pending queue
        self.add_to_pending(work_id);
        
        // Update statistics
        self.stats.total_added += 1;
        
        debug!("Added work to queue: {}", work_id);
        Ok(())
    }

    /// Add work ID to pending queue
    fn add_to_pending(&mut self, work_id: Md5Hash) {
        self.pending.push_back(work_id);
    }

    /// Get next available work from pending queue
    pub fn get_next_pending(&mut self) -> Option<Md5Hash> {
        self.pending.pop_front()
    }

    /// Get work item by ID
    pub fn get_work(&self, work_id: &Md5Hash) -> Option<&WorkInfo> {
        self.work_items.get(work_id)
    }

    /// Get mutable work item by ID
    pub fn get_work_mut(&mut self, work_id: &Md5Hash) -> Option<&mut WorkInfo> {
        self.work_items.get_mut(work_id)
    }

    /// Get all work items
    pub fn get_all_work(&self) -> Vec<WorkInfo> {
        self.work_items.values().cloned().collect()
    }

    /// Mark work as completed
    pub fn mark_completed(&mut self, work_id: &Md5Hash) -> Result<(), String> {
        if let Some(work_info) = self.work_items.get_mut(work_id) {
            work_info.mark_completed();
            self.completed.insert(*work_id);
            debug!("Marked work completed: {}", work_id);
            Ok(())
        } else {
            Err(format!("Work not found: {}", work_id))
        }
    }

    /// Mark work as failed
    pub fn mark_failed(&mut self, work_id: &Md5Hash, error: &str, max_retries: u32) -> Result<(), String> {
        if let Some(work_info) = self.work_items.get_mut(work_id) {
            work_info.mark_failed(error.to_string(), max_retries);
            
            // If it's still retryable, add back to pending
            if work_info.is_failed() {
                self.add_to_pending(*work_id);
            }
            
            debug!("Marked work failed: {} - {}", work_id, error);
            Ok(())
        } else {
            Err(format!("Work not found: {}", work_id))
        }
    }

    /// Find work items ready for retry
    pub fn find_retry_candidates(&self, retry_delay: std::time::Duration) -> Vec<Md5Hash> {
        self.work_items
            .iter()
            .filter_map(|(work_id, work_info)| {
                if work_info.is_failed() && work_info.status.is_ready_for_retry(retry_delay) {
                    Some(*work_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find work items that have timed out
    pub fn find_timed_out_work(&self, timeout: std::time::Duration) -> Vec<Md5Hash> {
        use chrono::Utc;
        
        let now = Utc::now();
        self.work_items
            .iter()
            .filter_map(|(work_id, work_info)| {
                if let WorkStatus::InProgress { started_at, .. } = work_info.status {
                    let elapsed = now
                        .signed_duration_since(started_at)
                        .to_std()
                        .unwrap_or(std::time::Duration::ZERO);

                    if elapsed > timeout {
                        Some(*work_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove completed and abandoned work items
    pub fn cleanup(&mut self) -> usize {
        let initial_count = self.work_items.len();
        
        self.work_items.retain(|_, work_info| {
            !work_info.is_completed() && !work_info.is_abandoned()
        });
        
        let removed = initial_count - self.work_items.len();
        
        if removed > 0 {
            debug!("Cleaned up {} completed/abandoned work items", removed);
        }
        
        removed
    }

    /// Reset all state (clear everything)
    pub fn reset(&mut self) {
        self.pending.clear();
        self.work_items.clear();
        self.completed.clear();
        self.stats = QueueStats::new();
        self.next_worker_id = 1;
        debug!("Reset queue state");
    }

    /// Update statistics based on current state
    pub fn update_stats(&mut self) {
        self.stats.pending_count = self.pending.len() as u64;
        self.stats.in_progress_count = self.count_by_status(|s| s.is_in_progress());
        self.stats.completed_count = self.completed.len() as u64;
        self.stats.failed_count = self.count_by_status(|s| s.is_failed());
        self.stats.abandoned_count = self.count_by_status(|s| s.is_abandoned());
    }

    /// Get current statistics
    pub fn get_stats(&self) -> &QueueStats {
        &self.stats
    }

    /// Get mutable statistics
    pub fn get_stats_mut(&mut self) -> &mut QueueStats {
        &mut self.stats
    }

    /// Count work items by status predicate
    fn count_by_status<F>(&self, predicate: F) -> u64
    where
        F: Fn(&WorkStatus) -> bool,
    {
        self.work_items
            .values()
            .filter(|work_info| predicate(&work_info.status))
            .count() as u64
    }

    /// Check if queue has any work available (pending or retryable)
    pub fn has_work_available(&self, retry_delay: std::time::Duration) -> bool {
        !self.pending.is_empty() || self.has_retryable_work(retry_delay)
    }

    /// Check if queue has retryable work
    fn has_retryable_work(&self, retry_delay: std::time::Duration) -> bool {
        self.work_items.values().any(|work_info| {
            work_info.is_failed() && work_info.status.is_ready_for_retry(retry_delay)
        })
    }

    /// Check if queue is finished (no active work)
    pub fn is_finished(&self, retry_delay: std::time::Duration) -> bool {
        self.pending.is_empty()
            && !self.work_items.values().any(|work_info| {
                work_info.is_in_progress() || 
                (work_info.is_failed() && work_info.status.is_ready_for_retry(retry_delay))
            })
    }

    /// Get queue capacity info
    pub fn get_capacity_info(&self) -> (usize, usize, usize) {
        (
            self.pending.len(),
            self.work_items.len(),
            self.completed.len(),
        )
    }

    /// Add bulk work items efficiently
    pub fn add_work_bulk(&mut self, work_items: Vec<WorkInfo>) -> (usize, usize) {
        let mut added_count = 0;
        let mut duplicate_count = 0;

        for work_info in work_items {
            let work_id = *work_info.work_id();

            // Skip if already completed or exists
            if self.completed.contains(&work_id) || self.work_items.contains_key(&work_id) {
                duplicate_count += 1;
                continue;
            }

            // Add work item
            self.work_items.insert(work_id, work_info);
            self.add_to_pending(work_id);
            added_count += 1;
        }

        // Update stats
        self.stats.total_added += added_count as u64;
        self.stats.duplicate_count += duplicate_count as u64;

        (added_count, duplicate_count)
    }
}

impl Default for QueueState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::FileInfo;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_file_info(hash_str: &str) -> FileInfo {
        let temp_dir = TempDir::new().unwrap();
        let hash = Md5Hash::from_hex(hash_str).unwrap();
        let path = format!(
            "./data/uk-daily-temperature-obs/dataset-version-202407/devon/01381_twist/qc-version-1/midas-open_uk-daily-temperature-obs_dv-202407_devon_01381_twist_qcv-1_1980_{}.csv",
            &hash_str[..8]
        );
        FileInfo::new(hash, path, temp_dir.path(), None).unwrap()
    }

    #[test]
    fn test_queue_state_creation() {
        let state = QueueState::new();
        assert_eq!(state.next_worker_id, 1);
        assert!(state.pending.is_empty());
        assert!(state.work_items.is_empty());
        assert!(state.completed.is_empty());
    }

    #[test]
    fn test_add_work() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        let result = state.add_work(work_info);
        assert!(result.is_ok());
        assert_eq!(state.work_items.len(), 1);
        assert_eq!(state.pending.len(), 1);
        assert!(state.contains_work(&work_id));
    }

    #[test]
    fn test_duplicate_detection() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info1 = WorkInfo::new(file_info.clone(), 100);
        let work_info2 = WorkInfo::new(file_info, 100);

        // First add should succeed
        assert!(state.add_work(work_info1).is_ok());
        
        // Second add should fail
        assert!(state.add_work(work_info2).is_err());
    }

    #[test]
    fn test_work_completion() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        state.add_work(work_info).unwrap();
        state.mark_completed(&work_id).unwrap();

        assert!(state.is_completed(&work_id));
        assert!(state.get_work(&work_id).unwrap().is_completed());
    }

    #[test]
    fn test_work_failure() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        state.add_work(work_info).unwrap();
        state.mark_failed(&work_id, "Test error", 3).unwrap();

        let work = state.get_work(&work_id).unwrap();
        assert!(work.is_failed());
        assert_eq!(work.status.failure_count(), 1);
    }

    #[test]
    fn test_retry_candidates() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        state.add_work(work_info).unwrap();
        state.mark_failed(&work_id, "Test error", 3).unwrap();

        // Should find retry candidates after delay
        std::thread::sleep(Duration::from_millis(2));
        let candidates = state.find_retry_candidates(Duration::from_millis(1));
        assert!(candidates.contains(&work_id));
    }

    #[test]
    fn test_bulk_add() {
        let mut state = QueueState::new();
        
        let work_items = vec![
            WorkInfo::new(create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e"), 100),
            WorkInfo::new(create_test_file_info("60c9d1c465f3cbff652be1509c2e2a4e"), 100),
            WorkInfo::new(create_test_file_info("70c9d1c465f3cbff652be1509c2e2a4e"), 100),
        ];

        let (added, duplicates) = state.add_work_bulk(work_items);
        assert_eq!(added, 3);
        assert_eq!(duplicates, 0);
        assert_eq!(state.work_items.len(), 3);
        assert_eq!(state.pending.len(), 3);
    }

    #[test]
    fn test_cleanup() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        state.add_work(work_info).unwrap();
        state.mark_completed(&work_id).unwrap();

        let removed = state.cleanup();
        assert_eq!(removed, 1);
        assert!(state.work_items.is_empty());
    }

    #[test]
    fn test_queue_state_transitions() {
        let mut state = QueueState::new();
        let file_info = create_test_file_info("50c9d1c465f3cbff652be1509c2e2a4e");
        let work_info = WorkInfo::new(file_info, 100);
        let work_id = *work_info.work_id();

        // Add work
        state.add_work(work_info).unwrap();
        assert!(state.has_work_available(Duration::from_secs(1)));
        assert!(!state.is_finished(Duration::from_secs(1)));

        // Get next work
        let next_work = state.get_next_pending().unwrap();
        assert_eq!(next_work, work_id);

        // Mark as in progress
        state.get_work_mut(&work_id).unwrap().mark_in_progress(1);
        assert!(!state.has_work_available(Duration::from_secs(1)));
        assert!(!state.is_finished(Duration::from_secs(1)));

        // Complete work
        state.mark_completed(&work_id).unwrap();
        assert!(state.is_finished(Duration::from_secs(1)));
    }
}