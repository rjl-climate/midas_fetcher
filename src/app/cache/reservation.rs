//! File reservation system for preventing duplicate downloads
//!
//! This module provides the reservation system that prevents multiple workers
//! from downloading the same file simultaneously, with timeout handling and
//! status tracking.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::app::models::FileInfo;

/// Status of a file reservation attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationStatus {
    /// File already exists in cache and is verified
    AlreadyExists,
    /// File reservation successful, can proceed with download
    Reserved,
    /// File is currently being downloaded by another worker
    ReservedByOther { worker_id: u32 },
}

/// Information about a file reservation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationInfo {
    /// ID of the worker that reserved this file
    pub worker_id: u32,
    /// When the reservation was created
    pub reserved_at: DateTime<Utc>,
    /// Current status of the reservation
    pub status: ReservationState,
    /// File information
    pub file_info: FileInfo,
    /// Number of retry attempts
    pub retry_count: u32,
}

/// State of a file reservation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationState {
    /// File is reserved for download
    Reserved,
    /// File is currently being downloaded
    Downloading,
    /// Download completed successfully
    Completed,
    /// Download failed, available for retry
    Failed { error: String },
}

impl ReservationInfo {
    /// Create a new reservation
    pub fn new(worker_id: u32, file_info: FileInfo) -> Self {
        Self {
            worker_id,
            reserved_at: Utc::now(),
            status: ReservationState::Reserved,
            file_info,
            retry_count: 0,
        }
    }

    /// Check if reservation has timed out
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        let elapsed = Utc::now()
            .signed_duration_since(self.reserved_at)
            .to_std()
            .unwrap_or(Duration::ZERO);
        elapsed > timeout
    }

    /// Mark reservation as downloading
    pub fn mark_downloading(&mut self) {
        self.status = ReservationState::Downloading;
    }

    /// Mark reservation as completed
    pub fn mark_completed(&mut self) {
        self.status = ReservationState::Completed;
    }

    /// Mark reservation as failed
    pub fn mark_failed(&mut self, error: String) {
        self.status = ReservationState::Failed { error };
        self.retry_count += 1;
    }

    /// Check if reservation is in a failed state
    pub fn is_failed(&self) -> bool {
        matches!(self.status, ReservationState::Failed { .. })
    }

    /// Check if reservation is completed
    pub fn is_completed(&self) -> bool {
        matches!(self.status, ReservationState::Completed)
    }

    /// Check if reservation is currently downloading
    pub fn is_downloading(&self) -> bool {
        matches!(self.status, ReservationState::Downloading)
    }

    /// Get the error message if the reservation failed
    pub fn get_error(&self) -> Option<&str> {
        match &self.status {
            ReservationState::Failed { error } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::hash::Md5Hash;
    use crate::app::models::{DatasetFileInfo, QualityControlVersion};
    use std::path::PathBuf;

    fn create_test_file_info() -> FileInfo {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let dataset_info = DatasetFileInfo {
            dataset_name: "test-dataset".to_string(),
            version: "202407".to_string(),
            county: Some("devon".to_string()),
            station_id: Some("01381".to_string()),
            station_name: Some("twist".to_string()),
            quality_version: Some(QualityControlVersion::V1),
            year: Some("1980".to_string()),
            file_type: Some("data".to_string()),
        };

        FileInfo {
            hash,
            relative_path: "./data/test.csv".to_string(),
            file_name: "test.csv".to_string(),
            dataset_info,
            manifest_version: Some(202507),
            retry_count: 0,
            last_attempt: None,
            estimated_size: None,
            destination_path: PathBuf::from("/tmp/test.csv"),
        }
    }

    #[test]
    fn test_reservation_creation() {
        let file_info = create_test_file_info();
        let reservation = ReservationInfo::new(42, file_info.clone());

        assert_eq!(reservation.worker_id, 42);
        assert_eq!(reservation.status, ReservationState::Reserved);
        assert_eq!(reservation.file_info.hash, file_info.hash);
        assert_eq!(reservation.retry_count, 0);
    }

    #[test]
    fn test_reservation_state_transitions() {
        let file_info = create_test_file_info();
        let mut reservation = ReservationInfo::new(42, file_info);

        // Initial state
        assert_eq!(reservation.status, ReservationState::Reserved);
        assert!(!reservation.is_downloading());
        assert!(!reservation.is_completed());
        assert!(!reservation.is_failed());

        // Mark as downloading
        reservation.mark_downloading();
        assert_eq!(reservation.status, ReservationState::Downloading);
        assert!(reservation.is_downloading());
        assert!(!reservation.is_completed());
        assert!(!reservation.is_failed());

        // Mark as completed
        reservation.mark_completed();
        assert_eq!(reservation.status, ReservationState::Completed);
        assert!(!reservation.is_downloading());
        assert!(reservation.is_completed());
        assert!(!reservation.is_failed());
    }

    #[test]
    fn test_reservation_failure() {
        let file_info = create_test_file_info();
        let mut reservation = ReservationInfo::new(42, file_info);

        reservation.mark_failed("Network error".to_string());

        assert!(reservation.is_failed());
        assert_eq!(reservation.get_error(), Some("Network error"));
        assert_eq!(reservation.retry_count, 1);

        // Mark as failed again
        reservation.mark_failed("Another error".to_string());
        assert_eq!(reservation.retry_count, 2);
        assert_eq!(reservation.get_error(), Some("Another error"));
    }

    #[test]
    fn test_reservation_timeout() {
        let file_info = create_test_file_info();
        let reservation = ReservationInfo::new(42, file_info);

        // Should not be timed out immediately
        assert!(!reservation.is_timed_out(Duration::from_secs(60)));

        // Wait a small amount then test timeout
        std::thread::sleep(Duration::from_millis(2));
        assert!(reservation.is_timed_out(Duration::from_millis(1)));
    }

    #[test]
    fn test_reservation_status_equality() {
        assert_eq!(
            ReservationStatus::AlreadyExists,
            ReservationStatus::AlreadyExists
        );
        assert_eq!(ReservationStatus::Reserved, ReservationStatus::Reserved);
        assert_eq!(
            ReservationStatus::ReservedByOther { worker_id: 42 },
            ReservationStatus::ReservedByOther { worker_id: 42 }
        );
        assert_ne!(
            ReservationStatus::ReservedByOther { worker_id: 42 },
            ReservationStatus::ReservedByOther { worker_id: 43 }
        );
    }
}
