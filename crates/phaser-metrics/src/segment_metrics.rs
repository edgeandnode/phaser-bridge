//! Standard metrics for bridge implementations
//!
//! Provides a consistent metrics interface for all bridges that implement the FlightBridge trait.
//! These metrics track worker progress, segment processing, and performance across phases.

use prometheus::{
    register_gauge_vec, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec,
};
use std::sync::Arc;

/// Common segment worker metrics shared by bridges and query services.
/// Wrapped in Arc for cheap cloning across workers without copying strings/state.
pub struct SegmentWorkerMetrics {
    pub service_name: String,
    pub chain_id: String,
    pub bridge_name: String,
    worker_stage: IntGaugeVec,
    active_workers: IntGaugeVec,
    items_processed: IntCounterVec,
    segment_attempts: IntCounterVec,
    segment_attempts_by_segment: IntCounterVec,
    segment_failures_by_segment: IntCounterVec,
    segment_duration: HistogramVec,
    worker_progress: GaugeVec,
    stream_retries: IntCounterVec,
    segment_retry_count: IntGaugeVec,
    sync_errors: IntCounterVec,
}

/// Trait providing common segment worker metrics with default implementations.
/// Types only need to implement `base()` to get all the delegation automatically.
pub trait SegmentMetrics {
    /// Access to the underlying SegmentWorkerMetrics
    fn base(&self) -> &SegmentWorkerMetrics;

    /// Set the current stage for a worker
    fn set_worker_stage(&self, worker_id: usize, segment_id: u64, stage: WorkerStage) {
        self.base().set_worker_stage(worker_id, segment_id, stage);
    }

    /// Increment active workers for a phase
    fn active_workers_inc(&self, phase: &str) {
        self.base().active_workers_inc(phase);
    }

    /// Decrement active workers for a phase
    fn active_workers_dec(&self, phase: &str) {
        self.base().active_workers_dec(phase);
    }

    /// Increment items processed counter
    fn items_processed_inc(&self, worker_id: usize, segment_id: u64, data_type: &str, count: u64) {
        self.base()
            .items_processed_inc(worker_id, segment_id, data_type, count);
    }

    /// Record segment attempt
    fn segment_attempt(&self, success: bool) {
        self.base().segment_attempt(success);
    }

    /// Record segment attempt for a specific segment
    fn segment_attempt_by_segment(&self, segment_num: u64, success: bool) {
        self.base().segment_attempt_by_segment(segment_num, success);
    }

    /// Record segment failure for a specific segment and bounded error type
    fn segment_failure_by_segment(&self, segment_num: u64, error_type: &str) {
        self.base()
            .segment_failure_by_segment(segment_num, error_type);
    }

    /// Observe segment processing duration
    fn segment_duration(&self, phase: &str, duration_secs: f64) {
        self.base().segment_duration(phase, duration_secs);
    }

    /// Set worker progress percentage
    fn set_worker_progress(
        &self,
        worker_id: usize,
        segment_id: u64,
        phase: &str,
        progress_pct: f64,
    ) {
        self.base()
            .set_worker_progress(worker_id, segment_id, phase, progress_pct);
    }

    /// Record stream retry attempt
    fn stream_retry(&self, operation: &str, success: bool) {
        self.base().stream_retry(operation, success);
    }

    /// Set segment retry count
    fn segment_retry_count(&self, segment_num: u64, count: i64) {
        self.base().segment_retry_count(segment_num, count);
    }

    /// Remove segment retry count metric
    fn segment_retry_count_remove(&self, segment_num: u64) {
        self.base().segment_retry_count_remove(segment_num);
    }

    /// Record an error
    fn error(&self, error_type: &str, data_type: &str) {
        self.base().error(error_type, data_type);
    }
}

/// Bridge-specific metrics that add to the common segment worker metrics.
/// Cheap to clone via Arc wrapper on base.
#[derive(Clone)]
pub struct BridgeMetrics {
    pub base: Arc<SegmentWorkerMetrics>,
    grpc_streams_active: IntGaugeVec,
    grpc_request_duration_blocks: HistogramVec,
    grpc_request_duration_transactions: HistogramVec,
    grpc_request_duration_logs: HistogramVec,
    grpc_message_size_bytes: HistogramVec,
}

impl SegmentMetrics for BridgeMetrics {
    fn base(&self) -> &SegmentWorkerMetrics {
        &self.base
    }
}

impl SegmentWorkerMetrics {
    pub fn new(
        service_name: impl Into<String>,
        chain_id: u64,
        bridge_name: impl Into<String>,
    ) -> Self {
        let service_name = service_name.into();
        let chain_id_str = chain_id.to_string();
        let bridge_name = bridge_name.into();

        Self {
            service_name: service_name.clone(),
            chain_id: chain_id_str,
            bridge_name: bridge_name.clone(),
            worker_stage: register_int_gauge_vec!(
                format!("{}_worker_stage", service_name),
                "Current stage for each worker (0=idle, 1=blocks, 2=transactions, 3=logs)",
                &[
                    "chain_id",
                    "bridge_name",
                    "worker_id",
                    "segment_id",
                    "stage"
                ]
            )
            .unwrap(),
            active_workers: register_int_gauge_vec!(
                format!("{}_active_workers", service_name),
                "Number of workers currently active in each processing phase",
                &["chain_id", "bridge_name", "phase"]
            )
            .unwrap(),
            items_processed: register_int_counter_vec!(
                format!("{}_items_processed_total", service_name),
                "Total items processed by data type",
                &[
                    "chain_id",
                    "bridge_name",
                    "worker_id",
                    "segment_id",
                    "data_type"
                ]
            )
            .unwrap(),
            segment_attempts: register_int_counter_vec!(
                format!("{}_segment_attempts_total", service_name),
                "Total segment processing attempts by result",
                &["chain_id", "bridge_name", "result"] // result: "success", "failure"
            )
            .unwrap(),
            segment_attempts_by_segment: register_int_counter_vec!(
                format!("{}_segment_attempts_by_segment_total", service_name),
                "Total segment processing attempts by segment and result",
                &["chain_id", "bridge_name", "segment_num", "result"]
            )
            .unwrap(),
            segment_failures_by_segment: register_int_counter_vec!(
                format!("{}_segment_failures_by_segment_total", service_name),
                "Total segment failures by segment and bounded error type",
                &["chain_id", "bridge_name", "segment_num", "error_type"]
            )
            .unwrap(),
            segment_duration: register_histogram_vec!(
                format!("{}_segment_duration_seconds", service_name),
                "Time to process a segment by phase",
                &["chain_id", "bridge_name", "phase"],
                vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0] // 1s to 30min
            )
            .unwrap(),
            worker_progress: register_gauge_vec!(
                format!("{}_worker_progress_percent", service_name),
                "Progress percentage for each worker in current phase",
                &[
                    "chain_id",
                    "bridge_name",
                    "worker_id",
                    "segment_id",
                    "phase"
                ]
            )
            .unwrap(),
            stream_retries: register_int_counter_vec!(
                format!("{}_stream_retries_total", service_name),
                "Number of stream retry attempts",
                &["chain_id", "bridge_name", "operation", "result"]
            )
            .unwrap(),
            segment_retry_count: register_int_gauge_vec!(
                format!("{}_segment_retry_count", service_name),
                "Current retry count for segments",
                &["chain_id", "bridge_name", "segment_num"]
            )
            .unwrap(),
            sync_errors: register_int_counter_vec!(
                format!("{}_errors_total", service_name),
                "Errors by type and data category",
                &["chain_id", "bridge_name", "error_type", "data_type"]
            )
            .unwrap(),
        }
    }

    /// Set the current stage for a worker
    pub fn set_worker_stage(&self, worker_id: usize, segment_id: u64, stage: WorkerStage) {
        let worker_label = worker_id.to_string();
        let segment_label = segment_id.to_string();

        // Clear all stages for this worker
        for s in &["idle", "blocks", "transactions", "logs"] {
            self.worker_stage
                .with_label_values(&[
                    &self.chain_id,
                    &self.bridge_name,
                    &worker_label,
                    &segment_label,
                    s,
                ])
                .set(0);
        }

        // Set current stage
        let (stage_name, stage_value) = match stage {
            WorkerStage::Idle => ("idle", 0),
            WorkerStage::Blocks => ("blocks", 1),
            WorkerStage::Transactions => ("transactions", 2),
            WorkerStage::Logs => ("logs", 3),
        };

        self.worker_stage
            .with_label_values(&[
                &self.chain_id,
                &self.bridge_name,
                &worker_label,
                &segment_label,
                stage_name,
            ])
            .set(stage_value);
    }

    /// Increment active workers for a phase
    pub fn active_workers_inc(&self, phase: &str) {
        self.active_workers
            .with_label_values(&[&self.chain_id, &self.bridge_name, phase])
            .inc();
    }

    /// Decrement active workers for a phase
    pub fn active_workers_dec(&self, phase: &str) {
        self.active_workers
            .with_label_values(&[&self.chain_id, &self.bridge_name, phase])
            .dec();
    }

    /// Increment items processed counter
    pub fn items_processed_inc(
        &self,
        worker_id: usize,
        segment_id: u64,
        data_type: &str,
        count: u64,
    ) {
        self.items_processed
            .with_label_values(&[
                &self.chain_id,
                &self.bridge_name,
                &worker_id.to_string(),
                &segment_id.to_string(),
                data_type,
            ])
            .inc_by(count);
    }

    /// Record segment attempt
    pub fn segment_attempt(&self, success: bool) {
        let result = if success { "success" } else { "failure" };
        self.segment_attempts
            .with_label_values(&[&self.chain_id, &self.bridge_name, result])
            .inc();
    }

    /// Record segment attempt for a specific segment
    pub fn segment_attempt_by_segment(&self, segment_num: u64, success: bool) {
        let result = if success { "success" } else { "failure" };
        self.segment_attempts_by_segment
            .with_label_values(&[
                &self.chain_id,
                &self.bridge_name,
                &segment_num.to_string(),
                result,
            ])
            .inc();
    }

    /// Record segment failure for a specific segment and bounded error type
    pub fn segment_failure_by_segment(&self, segment_num: u64, error_type: &str) {
        self.segment_failures_by_segment
            .with_label_values(&[
                &self.chain_id,
                &self.bridge_name,
                &segment_num.to_string(),
                error_type,
            ])
            .inc();
    }

    /// Observe segment processing duration
    pub fn segment_duration(&self, phase: &str, duration_secs: f64) {
        self.segment_duration
            .with_label_values(&[&self.chain_id, &self.bridge_name, phase])
            .observe(duration_secs);
    }

    /// Set worker progress percentage
    pub fn set_worker_progress(
        &self,
        worker_id: usize,
        segment_id: u64,
        phase: &str,
        progress_pct: f64,
    ) {
        self.worker_progress
            .with_label_values(&[
                &self.chain_id,
                &self.bridge_name,
                &worker_id.to_string(),
                &segment_id.to_string(),
                phase,
            ])
            .set(progress_pct);
    }

    /// Record stream retry attempt
    pub fn stream_retry(&self, operation: &str, success: bool) {
        let result = if success { "success" } else { "failure" };
        self.stream_retries
            .with_label_values(&[&self.chain_id, &self.bridge_name, operation, result])
            .inc();
    }

    /// Set segment retry count
    pub fn segment_retry_count(&self, segment_num: u64, count: i64) {
        self.segment_retry_count
            .with_label_values(&[&self.chain_id, &self.bridge_name, &segment_num.to_string()])
            .set(count);
    }

    /// Remove segment retry count metric
    pub fn segment_retry_count_remove(&self, segment_num: u64) {
        self.segment_retry_count
            .remove_label_values(&[&self.chain_id, &self.bridge_name, &segment_num.to_string()])
            .ok();
    }

    /// Record an error
    pub fn error(&self, error_type: &str, data_type: &str) {
        self.sync_errors
            .with_label_values(&[&self.chain_id, &self.bridge_name, error_type, data_type])
            .inc();
    }
}

impl BridgeMetrics {
    pub fn new(
        service_name: impl Into<String>,
        chain_id: u64,
        bridge_name: impl Into<String>,
    ) -> Self {
        let service_name_str = service_name.into();
        let base = SegmentWorkerMetrics::new(service_name_str.clone(), chain_id, bridge_name);

        Self {
            base: Arc::new(base),
            grpc_streams_active: register_int_gauge_vec!(
                format!("{}_grpc_streams_active", service_name_str),
                "Number of active gRPC streams",
                &["chain_id", "bridge_name", "stream_type"]
            )
            .unwrap(),
            grpc_request_duration_blocks: register_histogram_vec!(
                format!(
                    "{}_grpc_request_duration_blocks_milliseconds",
                    service_name_str
                ),
                "Duration of individual block fetch requests in milliseconds",
                &["chain_id", "bridge_name", "segment_num", "method"],
                // 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 2m, 5m
                vec![
                    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
                    10000.0, 30000.0, 60000.0, 120000.0, 300000.0
                ]
            )
            .unwrap(),
            grpc_request_duration_transactions: register_histogram_vec!(
                format!(
                    "{}_grpc_request_duration_transactions_milliseconds",
                    service_name_str
                ),
                "Duration of individual transaction fetch requests in milliseconds",
                &["chain_id", "bridge_name", "segment_num", "method"],
                // 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 2m, 5m, 10m
                vec![
                    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
                    10000.0, 30000.0, 60000.0, 120000.0, 300000.0, 600000.0
                ]
            )
            .unwrap(),
            grpc_request_duration_logs: register_histogram_vec!(
                format!(
                    "{}_grpc_request_duration_logs_milliseconds",
                    service_name_str
                ),
                "Duration of individual log fetch requests in milliseconds",
                &["chain_id", "bridge_name", "segment_num", "method"],
                // 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 2m, 5m, 10m, 20m, 30m, 1h
                vec![
                    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
                    10000.0, 30000.0, 60000.0, 120000.0, 300000.0, 600000.0, 1200000.0, 1800000.0,
                    3600000.0
                ]
            )
            .unwrap(),
            grpc_message_size_bytes: register_histogram_vec!(
                format!("{}_grpc_message_size_bytes", service_name_str),
                "Size of gRPC messages received from Erigon in bytes",
                &["chain_id", "bridge_name", "data_type"],
                // 1KB, 10KB, 100KB, 1MB, 5MB, 10MB, 25MB, 50MB, 100MB, 150MB, 200MB, 256MB, 384MB, 512MB
                vec![
                    1024.0,
                    10240.0,
                    102400.0,
                    1048576.0,
                    5242880.0,
                    10485760.0,
                    26214400.0,
                    52428800.0,
                    104857600.0,
                    157286400.0,
                    209715200.0,
                    268435456.0,
                    402653184.0,
                    536870912.0
                ]
            )
            .unwrap(),
        }
    }

    /// Increment active gRPC stream count
    pub fn grpc_stream_inc(&self, stream_type: &str) {
        self.grpc_streams_active
            .with_label_values(&[&self.base.chain_id, &self.base.bridge_name, stream_type])
            .inc();
    }

    /// Decrement active gRPC stream count
    pub fn grpc_stream_dec(&self, stream_type: &str) {
        self.grpc_streams_active
            .with_label_values(&[&self.base.chain_id, &self.base.bridge_name, stream_type])
            .dec();
    }

    /// Record gRPC request duration for blocks in milliseconds
    pub fn grpc_request_duration_blocks(
        &self,
        segment_num: u64,
        method: &str,
        duration_millis: f64,
    ) {
        self.grpc_request_duration_blocks
            .with_label_values(&[
                &self.base.chain_id,
                &self.base.bridge_name,
                &segment_num.to_string(),
                method,
            ])
            .observe(duration_millis);
    }

    /// Record gRPC request duration for transactions in milliseconds
    pub fn grpc_request_duration_transactions(
        &self,
        segment_num: u64,
        method: &str,
        duration_millis: f64,
    ) {
        self.grpc_request_duration_transactions
            .with_label_values(&[
                &self.base.chain_id,
                &self.base.bridge_name,
                &segment_num.to_string(),
                method,
            ])
            .observe(duration_millis);
    }

    /// Record gRPC request duration for logs in milliseconds
    pub fn grpc_request_duration_logs(&self, segment_num: u64, method: &str, duration_millis: f64) {
        self.grpc_request_duration_logs
            .with_label_values(&[
                &self.base.chain_id,
                &self.base.bridge_name,
                &segment_num.to_string(),
                method,
            ])
            .observe(duration_millis);
    }

    /// Record gRPC message size in bytes
    pub fn grpc_message_size(&self, data_type: &str, size_bytes: usize) {
        self.grpc_message_size_bytes
            .with_label_values(&[&self.base.chain_id, &self.base.bridge_name, data_type])
            .observe(size_bytes as f64);
    }
}

/// Worker processing stages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStage {
    Idle,
    Blocks,
    Transactions,
    Logs,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_worker_metrics_registration() {
        // First registration should succeed
        let metrics1 = SegmentWorkerMetrics::new("test_service", 1, "test_bridge");

        // Verify the metrics struct was created
        assert_eq!(metrics1.service_name, "test_service");
        assert_eq!(metrics1.chain_id, "1");
        assert_eq!(metrics1.bridge_name, "test_bridge");
    }

    #[test]
    #[should_panic(expected = "AlreadyReg")]
    fn test_duplicate_segment_worker_metrics_registration_panics() {
        // Register metrics with the same service name twice - should panic
        let _metrics1 = SegmentWorkerMetrics::new("duplicate_test", 1, "bridge1");
        let _metrics2 = SegmentWorkerMetrics::new("duplicate_test", 2, "bridge2");
    }

    #[test]
    fn test_bridge_metrics_registration() {
        // Test that BridgeMetrics can be created
        let metrics = BridgeMetrics::new("test_bridge_service", 1, "erigon");

        // Verify base metrics are accessible
        assert_eq!(metrics.base.service_name, "test_bridge_service");
        assert_eq!(metrics.base.chain_id, "1");
        assert_eq!(metrics.base.bridge_name, "erigon");
    }

    #[test]
    #[should_panic(expected = "AlreadyReg")]
    fn test_duplicate_bridge_metrics_registration_panics() {
        // Register BridgeMetrics with the same service name twice - should panic
        let _metrics1 = BridgeMetrics::new("duplicate_bridge", 1, "bridge1");
        let _metrics2 = BridgeMetrics::new("duplicate_bridge", 2, "bridge2");
    }

    #[test]
    fn test_different_service_names_coexist() {
        // Different service names should be able to coexist
        let _metrics1 = SegmentWorkerMetrics::new("service_a", 1, "bridge1");
        let _metrics2 = SegmentWorkerMetrics::new("service_b", 1, "bridge2");

        // If we got here without panic, test passes
    }
}
