//! Background task management for the coordinator
//!
//! This module provides utilities for managing background tasks including
//! cleanup operations, periodic logging, and timeout monitoring with
//! consistent shutdown handling patterns.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::app::cache::CacheManager;
use crate::app::queue::WorkQueue;
use crate::constants::coordinator;

/// Background task manager for coordinating cleanup and monitoring tasks
pub struct BackgroundTaskManager {
    tasks: Vec<JoinHandle<()>>,
}

impl BackgroundTaskManager {
    /// Create a new background task manager
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    /// Start periodic cleanup task for stale reservations and timeouts
    pub fn start_cleanup_task(
        &mut self,
        cache: Arc<CacheManager>,
        queue: Arc<WorkQueue>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let task = tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(coordinator::CLEANUP_INTERVAL);

            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        // Clean up stale cache reservations
                        if let Ok(cleaned_reservations) = cache.cleanup_stale_reservations().await {
                            if cleaned_reservations > 0 {
                                info!("Cleaned up {} stale cache reservations", cleaned_reservations);
                            }
                        }

                        // Handle work timeouts in queue
                        let timed_out_work = queue.handle_timeouts().await;
                        if timed_out_work > 0 {
                            info!("Handled {} timed-out work items", timed_out_work);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Cleanup task received shutdown signal");
                        break;
                    }
                }
            }
        });

        self.tasks.push(task);
    }

    /// Start periodic progress logging task
    pub fn start_periodic_logging_task(
        &mut self,
        queue: Arc<WorkQueue>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(coordinator::PROGRESS_LOG_INTERVAL);
            let mut last_completed = 0u64;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let stats = queue.stats().await;

                        if stats.pending_count > 0 || stats.in_progress_count > 0 {
                            let completed_since_last = stats.completed_count.saturating_sub(last_completed);
                            if completed_since_last > 0 {
                                info!(
                                    "Download progress: {} completed (+{}), {} pending, {} in progress, {} failed",
                                    stats.completed_count,
                                    completed_since_last,
                                    stats.pending_count,
                                    stats.in_progress_count,
                                    stats.failed_count
                                );
                            } else {
                                info!(
                                    "Download progress: pending={}, in_progress={}, completed={}, failed={}",
                                    stats.pending_count,
                                    stats.in_progress_count,
                                    stats.completed_count,
                                    stats.failed_count
                                );
                            }
                            last_completed = stats.completed_count;
                        } else {
                            info!(
                                "All downloads completed: {} successful, {} failed",
                                stats.completed_count, stats.failed_count
                            );
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Periodic logging task received shutdown signal");
                        break;
                    }
                }
            }
        });

        self.tasks.push(task);
    }

    /// Start timeout monitoring task for detecting stalled downloads
    pub fn start_timeout_monitoring_task(
        &mut self,
        queue: Arc<WorkQueue>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let task = tokio::spawn(async move {
            let mut last_progress_check = Instant::now();
            let mut last_completed_count = 0u64;

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(coordinator::TIMEOUT_CHECK_INTERVAL) => {
                        let debug_stats = queue.stats().await;
                        let now = Instant::now();

                        // Calculate progress rate over the last monitoring period
                        let time_since_last_check = now.duration_since(last_progress_check).as_secs_f64();
                        let files_completed_since_last = debug_stats.completed_count.saturating_sub(last_completed_count);
                        let progress_rate = if time_since_last_check > 0.0 {
                            files_completed_since_last as f64 / time_since_last_check
                        } else {
                            0.0
                        };

                        // Update tracking variables
                        last_progress_check = now;
                        last_completed_count = debug_stats.completed_count;

                        if debug_stats.pending_count == 0 && debug_stats.in_progress_count == 0 {
                            debug!("Timeout monitor: Downloads appear complete");
                            break; // Exit monitor when work is done
                        } else {
                            // Only warn if progress rate is abnormally slow
                            if progress_rate < coordinator::MIN_PROGRESS_RATE_THRESHOLD {
                                warn!("Timeout monitor: Download process appears slow - Progress rate: {:.1} files/s", progress_rate);
                                warn!("Queue diagnostics: pending={}, in_progress={}, completed={}, failed={}",
                                       debug_stats.pending_count, debug_stats.in_progress_count,
                                       debug_stats.completed_count, debug_stats.failed_count);
                            } else {
                                debug!("Timeout monitor: Download progressing normally at {:.1} files/s", progress_rate);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Timeout monitoring task received shutdown signal");
                        break;
                    }
                }
            }
        });

        self.tasks.push(task);
    }

    /// Shutdown all background tasks with timeout
    pub async fn shutdown_all(self) {
        debug!("Initiating background task shutdown");

        for task in self.tasks {
            if tokio::time::timeout(coordinator::TASK_SHUTDOWN_TIMEOUT, task)
                .await
                .is_err()
            {
                warn!(
                    "Background task shutdown timed out after {:?}",
                    coordinator::TASK_SHUTDOWN_TIMEOUT
                );
            }
        }

        debug!("All background tasks shutdown complete");
    }

    /// Get the number of active background tasks
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

impl Default for BackgroundTaskManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{CacheConfig, CacheManager, WorkQueue};
    use std::time::Duration;
    use tempfile::TempDir;

    async fn create_test_components() -> (Arc<WorkQueue>, Arc<CacheManager>) {
        let temp_dir = TempDir::new().unwrap();
        let cache_config = CacheConfig {
            cache_root: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };

        let queue = Arc::new(WorkQueue::new());
        let cache = Arc::new(CacheManager::new(cache_config).await.unwrap());

        (queue, cache)
    }

    /// Test background task manager creation
    ///
    /// Verifies that a background task manager can be created
    /// and starts with no active tasks.
    #[test]
    fn test_task_manager_creation() {
        let manager = BackgroundTaskManager::new();
        assert_eq!(manager.task_count(), 0);
    }

    /// Test cleanup task startup
    ///
    /// Ensures that cleanup tasks can be started and registered
    /// with the task manager.
    #[tokio::test]
    async fn test_cleanup_task_startup() {
        let (queue, cache) = create_test_components().await;
        let (_, shutdown_rx) = broadcast::channel(1);

        let mut manager = BackgroundTaskManager::new();
        manager.start_cleanup_task(cache, queue, shutdown_rx);

        assert_eq!(manager.task_count(), 1);

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// Test periodic logging task startup
    ///
    /// Verifies that periodic logging tasks can be started
    /// and properly registered.
    #[tokio::test]
    async fn test_periodic_logging_task_startup() {
        let (queue, _) = create_test_components().await;
        let (_, shutdown_rx) = broadcast::channel(1);

        let mut manager = BackgroundTaskManager::new();
        manager.start_periodic_logging_task(queue, shutdown_rx);

        assert_eq!(manager.task_count(), 1);

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// Test timeout monitoring task startup
    ///
    /// Ensures that timeout monitoring tasks can be started
    /// and tracked correctly.
    #[tokio::test]
    async fn test_timeout_monitoring_task_startup() {
        let (queue, _) = create_test_components().await;
        let (_, shutdown_rx) = broadcast::channel(1);

        let mut manager = BackgroundTaskManager::new();
        manager.start_timeout_monitoring_task(queue, shutdown_rx);

        assert_eq!(manager.task_count(), 1);

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// Test multiple task management
    ///
    /// Verifies that multiple background tasks can be managed
    /// simultaneously by a single task manager.
    #[tokio::test]
    async fn test_multiple_task_management() {
        let (queue, cache) = create_test_components().await;
        let (tx, _) = broadcast::channel(1);

        let mut manager = BackgroundTaskManager::new();
        manager.start_cleanup_task(cache, queue.clone(), tx.subscribe());
        manager.start_periodic_logging_task(queue.clone(), tx.subscribe());
        manager.start_timeout_monitoring_task(queue, tx.subscribe());

        assert_eq!(manager.task_count(), 3);

        // Give tasks a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// Test graceful shutdown
    ///
    /// Ensures that all background tasks can be gracefully shutdown
    /// and that the shutdown completes within the expected timeout.
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let (queue, cache) = create_test_components().await;
        let (tx, _) = broadcast::channel(1);

        let mut manager = BackgroundTaskManager::new();
        manager.start_cleanup_task(cache, queue.clone(), tx.subscribe());
        manager.start_periodic_logging_task(queue, tx.subscribe());

        assert_eq!(manager.task_count(), 2);

        // Give tasks a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger shutdown
        let _ = tx.send(());

        // Shutdown should complete without hanging
        let shutdown_start = Instant::now();
        manager.shutdown_all().await;
        let shutdown_duration = shutdown_start.elapsed();

        // Should complete well before the timeout
        assert!(shutdown_duration < coordinator::TASK_SHUTDOWN_TIMEOUT);
    }
}
