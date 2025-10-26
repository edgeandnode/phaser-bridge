//! Prometheus metrics for phaser-query sync service
//!
//! Tracks sync job progress, errors, retries, and worker activity

use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramVec,
    IntCounterVec, IntGaugeVec,
};

lazy_static! {
    /// Queue depths for work tracking
    pub static ref SYNC_QUEUE_DEPTH: IntGaugeVec = register_int_gauge_vec!(
        "phaser_sync_queue_depth",
        "Number of segments in each queue",
        &["queue"]  // "pending", "retrying"
    ).unwrap();

    /// Segment processing attempts
    pub static ref SEGMENT_ATTEMPTS: IntCounterVec = register_int_counter_vec!(
        "phaser_sync_segment_attempts_total",
        "Total segment processing attempts",
        &["result"]  // "success", "retry"
    ).unwrap();

    /// Errors by category
    pub static ref SYNC_ERRORS: IntCounterVec = register_int_counter_vec!(
        "phaser_sync_errors_total",
        "Sync errors by type and data category",
        &["error_type", "data_type"]  // error_type: "connection", "timeout", "no_data", etc.
    ).unwrap();

    /// Segment processing duration
    pub static ref SEGMENT_DURATION: HistogramVec = register_histogram_vec!(
        "phaser_sync_segment_duration_seconds",
        "Time to complete a segment (including retries)",
        &["data_type"],
        vec![10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]
    ).unwrap();

    /// Active workers
    pub static ref ACTIVE_WORKERS: IntGaugeVec = register_int_gauge_vec!(
        "phaser_sync_active_workers",
        "Number of workers actively processing",
        &["phase"]  // "blocks", "transactions", "logs"
    ).unwrap();

    /// Segment retry count gauge (tracks current retry count for in-progress segments)
    pub static ref SEGMENT_RETRY_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "phaser_sync_segment_retry_count",
        "Current retry count for segments",
        &["segment_num"]
    ).unwrap();
}

/// Categorize errors for metrics
pub fn categorize_error(err: &anyhow::Error) -> &'static str {
    let err_str = err.to_string().to_lowercase();

    if err_str.contains("connection") || err_str.contains("connect") {
        "connection"
    } else if err_str.contains("timeout") || err_str.contains("timed out") {
        "timeout"
    } else if err_str.contains("no data") || err_str.contains("empty") {
        "no_data"
    } else if err_str.contains("failed to make progress") {
        "stuck_worker"
    } else if err_str.contains("validation") || err_str.contains("invalid") {
        "validation"
    } else if err_str.contains("io error") || err_str.contains("file") {
        "disk_io"
    } else if err_str.contains("cancelled") {
        "cancelled"
    } else {
        "unknown"
    }
}
