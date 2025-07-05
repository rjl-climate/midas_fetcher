#!/usr/bin/env rust-script

//! Test script to demonstrate the progress bar formatting
//! Run with: cargo run --bin test_progress

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

fn main() {
    println!("Testing progress bar formatting...\n");

    // Test the exact template used in the application
    let pb = ProgressBar::new(1000);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} (ETA: {eta}) {per_sec:.0}/s")
            .unwrap()
            .progress_chars("##-")
    );

    // Enable steady tick for rate calculation
    pb.enable_steady_tick(Duration::from_millis(100));

    println!("Progress bar template: {{per_sec:.0}}/s");
    println!("Expected format: integer files/s (e.g., '87/s' not '87.5004/s')");
    println!("Expected color: white text\n");

    // Simulate rapid progress updates
    for i in 1..=100 {
        pb.set_position(i);
        std::thread::sleep(Duration::from_millis(20));
        
        if i % 25 == 0 {
            println!("Position: {}/1000", i);
        }
    }

    pb.finish_with_message("Test completed");
    println!("\nIf the rate shows as integers (e.g., '4/s') instead of decimals (e.g., '4.2345/s'), the fix works!");
}