//! Signal handling for graceful shutdown
//!
//! This module provides utilities for handling system signals (CTRL-C, SIGTERM)
//! to ensure graceful shutdown of the download coordinator and its components.

use tokio::signal;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::info;

/// Signal handler for graceful shutdown coordination
pub struct SignalHandler {
    shutdown_tx: broadcast::Sender<()>,
}

impl SignalHandler {
    /// Create a new signal handler with the given shutdown broadcaster
    pub fn new(shutdown_tx: broadcast::Sender<()>) -> Self {
        Self { shutdown_tx }
    }

    /// Setup signal handling for graceful shutdown (CTRL-C, SIGTERM)
    ///
    /// Returns a handle to the background task that monitors for signals.
    /// When a signal is received, it broadcasts shutdown to all subscribers.
    pub fn setup(&self) -> JoinHandle<()> {
        let shutdown_tx = self.shutdown_tx.clone();

        tokio::spawn(async move {
            let ctrl_c = async {
                signal::ctrl_c()
                    .await
                    .expect("Failed to install Ctrl+C handler");
                info!("Ctrl+C signal received");
            };

            #[cfg(unix)]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to install signal handler")
                    .recv()
                    .await;
                info!("SIGTERM signal received");
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {
                    info!("Received Ctrl+C, initiating shutdown");
                },
                _ = terminate => {
                    info!("Received terminate signal, initiating shutdown");
                },
            }

            // Broadcast shutdown signal to all listeners
            let _ = shutdown_tx.send(());
        })
    }
}

/// Create a shutdown signal broadcaster
///
/// Returns a tuple of (sender, receiver) for shutdown coordination.
/// The sender can be used to trigger shutdown, while receivers can
/// subscribe to be notified when shutdown is requested.
pub fn create_shutdown_channel() -> (broadcast::Sender<()>, broadcast::Receiver<()>) {
    broadcast::channel(1)
}

/// Wait for any shutdown signal
///
/// This is a convenience function that waits for either a manual shutdown
/// trigger or a system signal (CTRL-C, SIGTERM).
pub async fn wait_for_shutdown_signal(mut shutdown_rx: broadcast::Receiver<()>) {
    let _ = shutdown_rx.recv().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Test shutdown channel creation
    ///
    /// Verifies that shutdown channels can be created and that
    /// signals can be sent and received correctly.
    #[tokio::test]
    async fn test_shutdown_channel_creation() {
        let (tx, mut rx) = create_shutdown_channel();

        // Test that we can send and receive shutdown signal
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(());
        });

        let result = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(result.is_ok());
    }

    /// Test signal handler creation
    ///
    /// Ensures that signal handlers can be created with valid
    /// shutdown broadcasters and don't panic during setup.
    #[tokio::test]
    async fn test_signal_handler_creation() {
        let (tx, _rx) = create_shutdown_channel();
        let handler = SignalHandler::new(tx);

        // Just test that we can create and setup the handler
        // We can't easily test actual signal handling in unit tests
        let _handle = handler.setup();

        // Let the task start
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// Test wait for shutdown signal
    ///
    /// Verifies that the wait_for_shutdown_signal function correctly
    /// waits for and responds to shutdown signals.
    #[tokio::test]
    async fn test_wait_for_shutdown() {
        let (tx, rx) = create_shutdown_channel();

        // Spawn a task that will trigger shutdown after a delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(());
        });

        // Wait for shutdown signal with timeout
        let result = timeout(Duration::from_millis(200), wait_for_shutdown_signal(rx)).await;

        assert!(result.is_ok());
    }

    /// Test multiple receivers
    ///
    /// Ensures that multiple components can subscribe to the same
    /// shutdown signal and all receive the notification.
    #[tokio::test]
    async fn test_multiple_shutdown_receivers() {
        let (tx, _) = create_shutdown_channel();
        let mut rx1 = tx.subscribe();
        let mut rx2 = tx.subscribe();

        // Trigger shutdown
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(());
        });

        // Both receivers should get the signal
        let result1 = timeout(Duration::from_millis(100), rx1.recv()).await;
        let result2 = timeout(Duration::from_millis(100), rx2.recv()).await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }
}
