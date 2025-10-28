//! Prometheus metrics for erigon-bridge
//!
//! Tracks worker progress, stage transitions, and performance metrics

use lazy_static::lazy_static;
use prometheus::{
    register_gauge_vec, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    Encoder, GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, TextEncoder,
};

lazy_static! {
    /// Worker stage tracking - shows which stage each worker is currently in
    /// Labels: worker_id, segment_id, stage=[blocks|transactions|logs|idle]
    pub static ref WORKER_STAGE: IntGaugeVec = register_int_gauge_vec!(
        "erigon_bridge_worker_stage",
        "Current stage for each worker (0=idle, 1=blocks, 2=transactions, 3=logs)",
        &["worker_id", "segment_id", "stage"]
    ).unwrap();

    /// Active workers by processing phase
    /// Labels: phase=[blocks|transactions|logs]
    pub static ref WORKERS_ACTIVE: IntGaugeVec = register_int_gauge_vec!(
        "erigon_bridge_workers_active",
        "Number of workers currently active in each processing phase",
        &["phase"]
    ).unwrap();

    /// Blocks processed per worker
    /// Labels: worker_id, segment_id
    pub static ref BLOCKS_PROCESSED: IntCounterVec = register_int_counter_vec!(
        "erigon_bridge_blocks_processed_total",
        "Total blocks processed by each worker",
        &["worker_id", "segment_id"]
    ).unwrap();

    /// Transactions processed per worker
    /// Labels: worker_id, segment_id
    pub static ref TRANSACTIONS_PROCESSED: IntCounterVec = register_int_counter_vec!(
        "erigon_bridge_transactions_processed_total",
        "Total transactions processed by each worker",
        &["worker_id", "segment_id"]
    ).unwrap();

    /// Logs processed per worker
    /// Labels: worker_id, segment_id
    pub static ref LOGS_PROCESSED: IntCounterVec = register_int_counter_vec!(
        "erigon_bridge_logs_processed_total",
        "Total logs processed by each worker",
        &["worker_id", "segment_id"]
    ).unwrap();

    /// Segment completion tracking
    /// Labels: phase=[blocks|transactions|logs], result=[success|failure]
    pub static ref SEGMENTS_COMPLETED: IntCounterVec = register_int_counter_vec!(
        "erigon_bridge_segments_completed_total",
        "Total segments completed by phase and result",
        &["phase", "result"]
    ).unwrap();

    /// Segment processing duration
    /// Labels: phase=[blocks|transactions|logs]
    pub static ref SEGMENT_DURATION: HistogramVec = register_histogram_vec!(
        "erigon_bridge_segment_duration_seconds",
        "Time to process a segment by phase",
        &["phase"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0] // 1s to 30min
    ).unwrap();

    /// Worker progress percentage (0.0 - 100.0)
    /// Labels: worker_id, segment_id, phase
    pub static ref WORKER_PROGRESS: GaugeVec = register_gauge_vec!(
        "erigon_bridge_worker_progress_percent",
        "Progress percentage for each worker in current phase",
        &["worker_id", "segment_id", "phase"]
    ).unwrap();

    /// gRPC stream retry attempts
    /// Labels: operation=[blocks|transactions|logs], result=[success|failure]
    pub static ref STREAM_RETRIES: IntCounterVec = register_int_counter_vec!(
        "erigon_bridge_stream_retries_total",
        "Number of gRPC stream retry attempts",
        &["operation", "result"]
    ).unwrap();

    /// Active gRPC streams
    pub static ref GRPC_STREAMS_ACTIVE: IntGaugeVec = register_int_gauge_vec!(
        "erigon_bridge_grpc_streams_active",
        "Number of active gRPC streams",
        &["stream_type"]
    ).unwrap();

    /// Memory usage (heap bytes)
    pub static ref MEMORY_HEAP_BYTES: IntGaugeVec = register_int_gauge_vec!(
        "erigon_bridge_memory_heap_bytes",
        "Current heap memory usage in bytes",
        &["type"]
    ).unwrap();
}

/// Helper to set worker stage
pub fn set_worker_stage(worker_id: usize, segment_id: u64, stage: WorkerStage) {
    let worker_label = worker_id.to_string();
    let segment_label = segment_id.to_string();

    // Clear all stages for this worker
    for s in &["idle", "blocks", "transactions", "logs"] {
        WORKER_STAGE
            .with_label_values(&[&worker_label, &segment_label, s])
            .set(0);
    }

    // Set current stage
    let (stage_name, stage_value) = match stage {
        WorkerStage::Idle => ("idle", 0),
        WorkerStage::Blocks => ("blocks", 1),
        WorkerStage::Transactions => ("transactions", 2),
    };

    WORKER_STAGE
        .with_label_values(&[&worker_label, &segment_label, stage_name])
        .set(stage_value);
}

/// Helper to set worker progress
pub fn set_worker_progress(worker_id: usize, segment_id: u64, phase: &str, progress_pct: f64) {
    WORKER_PROGRESS
        .with_label_values(&[&worker_id.to_string(), &segment_id.to_string(), phase])
        .set(progress_pct);
}

/// Helper to record segment completion
pub fn record_segment_complete(phase: &str, success: bool) {
    let result = if success { "success" } else { "failure" };
    SEGMENTS_COMPLETED.with_label_values(&[phase, result]).inc();
}

/// Worker processing stages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStage {
    Idle,
    Blocks,
    Transactions,
}

/// Get metrics in Prometheus text format
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
