//! Statistics collection and caching for the work queue
//!
//! This module provides efficient statistics calculation and caching mechanisms
//! for the work queue, enabling fast read access to queue metrics.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::debug;

use super::types::QueueStats;

/// Statistics manager with caching capabilities
#[derive(Debug)]
pub struct StatsManager {
    /// Cached statistics for fast read access
    cache: Arc<RwLock<QueueStats>>,
    /// Whether cache is dirty and needs updating
    cache_dirty: Arc<RwLock<bool>>,
}

impl StatsManager {
    /// Create a new statistics manager
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(QueueStats::new())),
            cache_dirty: Arc::new(RwLock::new(false)),
        }
    }

    /// Get cached statistics (fast read)
    pub async fn get_stats(&self) -> QueueStats {
        self.cache.read().await.clone()
    }

    /// Update cached statistics
    pub async fn update_stats(&self, stats: QueueStats) {
        let mut cache = self.cache.write().await;
        *cache = stats;
        
        // Mark cache as clean
        let mut dirty = self.cache_dirty.write().await;
        *dirty = false;
        
        debug!("Updated statistics cache");
    }

    /// Mark cache as dirty (needs updating)
    pub async fn mark_dirty(&self) {
        let mut dirty = self.cache_dirty.write().await;
        *dirty = true;
    }

    /// Check if cache is dirty
    pub async fn is_dirty(&self) -> bool {
        *self.cache_dirty.read().await
    }

    /// Reset statistics
    pub async fn reset(&self) {
        let mut cache = self.cache.write().await;
        *cache = QueueStats::new();
        
        let mut dirty = self.cache_dirty.write().await;
        *dirty = false;
        
        debug!("Reset statistics cache");
    }
}

impl Default for StatsManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics calculator for queue operations
pub struct StatsCalculator;

impl StatsCalculator {
    /// Calculate comprehensive statistics from queue state
    pub fn calculate_stats(
        pending_count: usize,
        in_progress_count: usize,
        completed_count: usize,
        failed_count: usize,
        abandoned_count: usize,
        total_added: u64,
        duplicate_count: u64,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> QueueStats {
        QueueStats {
            total_added,
            pending_count: pending_count as u64,
            in_progress_count: in_progress_count as u64,
            completed_count: completed_count as u64,
            failed_count: failed_count as u64,
            abandoned_count: abandoned_count as u64,
            duplicate_count,
            avg_processing_time: Duration::ZERO, // TODO: Calculate from work history
            created_at,
        }
    }

    /// Calculate success rate
    pub fn calculate_success_rate(completed: u64, abandoned: u64) -> f64 {
        let total_processed = completed + abandoned;
        if total_processed == 0 {
            0.0
        } else {
            (completed as f64 / total_processed as f64) * 100.0
        }
    }

    /// Calculate worker utilization
    pub fn calculate_worker_utilization(in_progress: u64, max_workers: u32) -> f64 {
        if max_workers == 0 {
            0.0
        } else {
            (in_progress as f64 / max_workers as f64).min(1.0) * 100.0
        }
    }

    /// Calculate queue health score (0-100)
    pub fn calculate_health_score(stats: &QueueStats, max_workers: u32) -> f64 {
        let success_rate = stats.success_rate();
        let utilization = stats.worker_utilization(max_workers);
        let active_ratio = if stats.total_added > 0 {
            (stats.active_count() as f64 / stats.total_added as f64) * 100.0
        } else {
            0.0
        };

        // Weight different factors
        let health = (success_rate * 0.5) + (utilization * 0.3) + ((100.0 - active_ratio) * 0.2);
        health.min(100.0).max(0.0)
    }
}

/// Performance metrics tracker
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Items processed per second
    pub items_per_second: f64,
    /// Average processing time per item
    pub avg_processing_time: Duration,
    /// Current queue throughput
    pub throughput: f64,
    /// Memory usage estimate
    pub memory_usage_mb: f64,
}

impl PerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        Self {
            items_per_second: 0.0,
            avg_processing_time: Duration::ZERO,
            throughput: 0.0,
            memory_usage_mb: 0.0,
        }
    }

    /// Calculate performance metrics from statistics
    pub fn calculate_from_stats(stats: &QueueStats, duration: Duration) -> Self {
        let total_processed = stats.completed_count + stats.abandoned_count;
        let items_per_second = if duration.as_secs() > 0 {
            total_processed as f64 / duration.as_secs() as f64
        } else {
            0.0
        };

        let throughput = if stats.in_progress_count > 0 {
            items_per_second
        } else {
            0.0
        };

        // Estimate memory usage (rough approximation)
        let active_items = stats.active_count();
        let memory_usage_mb = (active_items as f64 * 1024.0) / (1024.0 * 1024.0); // ~1KB per item

        Self {
            items_per_second,
            avg_processing_time: stats.avg_processing_time,
            throughput,
            memory_usage_mb,
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics reporter for generating human-readable reports
pub struct StatsReporter;

impl StatsReporter {
    /// Generate a summary report
    pub fn generate_summary_report(stats: &QueueStats, max_workers: u32) -> String {
        let health_score = StatsCalculator::calculate_health_score(stats, max_workers);
        
        format!(
            "Queue Statistics Summary:\n\
            ├─ Total Added: {}\n\
            ├─ Completed: {} ({:.1}%)\n\
            ├─ Failed: {}\n\
            ├─ Abandoned: {}\n\
            ├─ In Progress: {}\n\
            ├─ Pending: {}\n\
            ├─ Duplicates: {}\n\
            ├─ Success Rate: {:.1}%\n\
            ├─ Worker Utilization: {:.1}%\n\
            └─ Health Score: {:.1}/100",
            stats.total_added,
            stats.completed_count,
            if stats.total_added > 0 { 
                (stats.completed_count as f64 / stats.total_added as f64) * 100.0 
            } else { 
                0.0 
            },
            stats.failed_count,
            stats.abandoned_count,
            stats.in_progress_count,
            stats.pending_count,
            stats.duplicate_count,
            stats.success_rate(),
            stats.worker_utilization(max_workers),
            health_score
        )
    }

    /// Generate a detailed report
    pub fn generate_detailed_report(
        stats: &QueueStats, 
        max_workers: u32, 
        performance: &PerformanceMetrics
    ) -> String {
        let summary = Self::generate_summary_report(stats, max_workers);
        
        format!(
            "{}\n\n\
            Performance Metrics:\n\
            ├─ Items/Second: {:.2}\n\
            ├─ Avg Processing Time: {:?}\n\
            ├─ Throughput: {:.2}\n\
            ├─ Memory Usage: {:.2} MB\n\
            └─ Queue Age: {:?}",
            summary,
            performance.items_per_second,
            performance.avg_processing_time,
            performance.throughput,
            performance.memory_usage_mb,
            chrono::Utc::now().signed_duration_since(stats.created_at)
        )
    }

    /// Generate a compact one-line status
    pub fn generate_compact_status(stats: &QueueStats) -> String {
        format!(
            "Queue: {} total, {} completed, {} pending, {} in progress, {} failed",
            stats.total_added,
            stats.completed_count,
            stats.pending_count,
            stats.in_progress_count,
            stats.failed_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn test_stats_manager_creation() {
        let manager = StatsManager::new();
        // Basic creation test - manager should be created successfully
        assert!(format!("{:?}", manager).contains("StatsManager"));
    }

    #[tokio::test]
    async fn test_stats_manager_operations() {
        let manager = StatsManager::new();
        
        // Initial state
        let initial_stats = manager.get_stats().await;
        assert_eq!(initial_stats.total_added, 0);
        assert!(!manager.is_dirty().await);
        
        // Update stats
        let mut new_stats = QueueStats::new();
        new_stats.total_added = 100;
        new_stats.completed_count = 80;
        
        manager.update_stats(new_stats.clone()).await;
        let updated_stats = manager.get_stats().await;
        assert_eq!(updated_stats.total_added, 100);
        assert_eq!(updated_stats.completed_count, 80);
        
        // Mark dirty
        manager.mark_dirty().await;
        assert!(manager.is_dirty().await);
        
        // Reset
        manager.reset().await;
        let reset_stats = manager.get_stats().await;
        assert_eq!(reset_stats.total_added, 0);
        assert!(!manager.is_dirty().await);
    }

    #[test]
    fn test_stats_calculator() {
        let stats = StatsCalculator::calculate_stats(
            10, // pending
            5,  // in_progress
            80, // completed
            3,  // failed
            2,  // abandoned
            100, // total_added
            5,  // duplicate_count
            Utc::now(),
        );
        
        assert_eq!(stats.pending_count, 10);
        assert_eq!(stats.in_progress_count, 5);
        assert_eq!(stats.completed_count, 80);
        assert_eq!(stats.failed_count, 3);
        assert_eq!(stats.abandoned_count, 2);
        assert_eq!(stats.total_added, 100);
        assert_eq!(stats.duplicate_count, 5);
    }

    #[test]
    fn test_success_rate_calculation() {
        assert_eq!(StatsCalculator::calculate_success_rate(80, 20), 80.0);
        assert_eq!(StatsCalculator::calculate_success_rate(0, 0), 0.0);
        assert_eq!(StatsCalculator::calculate_success_rate(100, 0), 100.0);
    }

    #[test]
    fn test_worker_utilization_calculation() {
        assert_eq!(StatsCalculator::calculate_worker_utilization(5, 10), 50.0);
        assert_eq!(StatsCalculator::calculate_worker_utilization(10, 10), 100.0);
        assert_eq!(StatsCalculator::calculate_worker_utilization(15, 10), 100.0);
        assert_eq!(StatsCalculator::calculate_worker_utilization(5, 0), 0.0);
    }

    #[test]
    fn test_health_score_calculation() {
        let mut stats = QueueStats::new();
        stats.total_added = 100;
        stats.completed_count = 80;
        stats.abandoned_count = 10;
        stats.in_progress_count = 5;
        stats.pending_count = 5;
        
        let health = StatsCalculator::calculate_health_score(&stats, 10);
        assert!(health >= 0.0 && health <= 100.0);
    }

    #[test]
    fn test_performance_metrics() {
        let stats = QueueStats {
            total_added: 100,
            completed_count: 80,
            abandoned_count: 10,
            in_progress_count: 5,
            pending_count: 5,
            ..QueueStats::new()
        };
        
        let performance = PerformanceMetrics::calculate_from_stats(&stats, Duration::from_secs(60));
        
        assert!(performance.items_per_second > 0.0);
        assert!(performance.memory_usage_mb >= 0.0);
    }

    #[test]
    fn test_stats_reporter() {
        let stats = QueueStats {
            total_added: 100,
            completed_count: 80,
            abandoned_count: 10,
            in_progress_count: 5,
            pending_count: 5,
            failed_count: 0,
            duplicate_count: 2,
            ..QueueStats::new()
        };
        
        let summary = StatsReporter::generate_summary_report(&stats, 10);
        assert!(summary.contains("Total Added: 100"));
        assert!(summary.contains("Completed: 80"));
        assert!(summary.contains("Success Rate:"));
        
        let compact = StatsReporter::generate_compact_status(&stats);
        assert!(compact.contains("100 total"));
        assert!(compact.contains("80 completed"));
    }
}