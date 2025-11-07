//! Metrics infrastructure for phaser bridges and query services
//!
//! Provides composable metrics using a trait-based pattern. Base metrics are defined
//! in SegmentWorkerMetrics, and specialized metrics (BridgeMetrics, QueryMetrics) add
//! service-specific metrics while inheriting common functionality via the SegmentMetrics trait.
//!
//! ## Log Metrics
//!
//! The `MetricsLayer` provides automatic tracking of log events as Prometheus metrics.
//! By default, it tracks ERROR, WARN, and INFO levels, skipping high-volume DEBUG and TRACE.

mod segment_metrics;
mod tracing_layer;

pub use segment_metrics::{BridgeMetrics, SegmentMetrics, SegmentWorkerMetrics, WorkerStage};
pub use tracing_layer::{MetricsLayer, MetricsLayerConfig};

use prometheus::{register_int_gauge_vec, IntGaugeVec};
use std::sync::Arc;

/// Query service metrics for sync operations.
/// Composes SegmentWorkerMetrics and adds query-specific metrics.
#[derive(Clone)]
pub struct QueryMetrics {
    pub base: Arc<SegmentWorkerMetrics>,
    sync_queue_depth: IntGaugeVec,
    segment_total_duration_seconds: IntGaugeVec,
}

impl SegmentMetrics for QueryMetrics {
    fn base(&self) -> &SegmentWorkerMetrics {
        &self.base
    }
}

impl QueryMetrics {
    pub fn new(
        service_name: impl Into<String>,
        chain_id: u64,
        bridge_name: impl Into<String>,
    ) -> Self {
        let service_name_str = service_name.into();
        let base =
            SegmentWorkerMetrics::new(service_name_str.clone(), chain_id, bridge_name.into());

        Self {
            base: Arc::new(base),
            sync_queue_depth: register_int_gauge_vec!(
                format!("{}_sync_queue_depth", service_name_str),
                "Number of segments in each queue",
                &["chain_id", "bridge_name", "queue"] // queue: "pending", "retrying"
            )
            .unwrap(),
            segment_total_duration_seconds: register_int_gauge_vec!(
                format!("{}_segment_total_duration_seconds", service_name_str),
                "Total seconds spent on this specific segment across all retry attempts",
                &["chain_id", "bridge_name", "segment_num"]
            )
            .unwrap(),
        }
    }

    /// Set sync queue depth
    pub fn sync_queue_depth(&self, queue: &str, depth: i64) {
        self.sync_queue_depth
            .with_label_values(&[&self.base.chain_id, &self.base.bridge_name, queue])
            .set(depth);
    }

    /// Convenience wrapper: record segment attempts (maps to base.segment_attempt)
    pub fn segment_attempts(&self, result: &str) {
        self.segment_attempt(result == "success");
    }

    /// Convenience wrapper: record sync errors (maps to base.error)
    pub fn sync_errors(&self, error_type: &str, data_type: &str) {
        self.error(error_type, data_type);
    }

    /// Set segment sync retries for current sync run
    /// Note: This delegates to segment_retry_count for compatibility
    pub fn segment_sync_retries(&self, segment_num: u64, retries: i64) {
        self.segment_retry_count(segment_num, retries);
    }

    /// Set total duration for this segment in seconds
    pub fn segment_total_duration(&self, segment_num: u64, seconds: i64) {
        self.segment_total_duration_seconds
            .with_label_values(&[
                &self.base.chain_id,
                &self.base.bridge_name,
                &segment_num.to_string(),
            ])
            .set(seconds);
    }
}

/// Gather all registered metrics in Prometheus text format
pub fn gather_metrics() -> Result<String, String> {
    use prometheus::{Encoder, TextEncoder};

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| format!("Failed to encode metrics: {}", e))?;

    String::from_utf8(buffer).map_err(|e| format!("Failed to convert metrics to UTF-8: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_metrics_registration() {
        // First registration should succeed
        let metrics = QueryMetrics::new("test_query", 1, "test_bridge");

        // Verify the metrics struct was created
        assert_eq!(metrics.base.service_name, "test_query");
        assert_eq!(metrics.base.chain_id, "1");
        assert_eq!(metrics.base.bridge_name, "test_bridge");
    }

    #[test]
    #[should_panic(expected = "AlreadyReg")]
    fn test_duplicate_query_metrics_registration_panics() {
        // Register metrics with the same service name twice - should panic
        let _metrics1 = QueryMetrics::new("duplicate_query", 1, "bridge1");
        let _metrics2 = QueryMetrics::new("duplicate_query", 2, "bridge2");
    }

    #[test]
    fn test_query_metrics_with_different_names_coexist() {
        // Different service names should be able to coexist
        let _metrics1 = QueryMetrics::new("query_a", 1, "bridge1");
        let _metrics2 = QueryMetrics::new("query_b", 1, "bridge2");

        // If we got here without panic, test passes
    }

    #[test]
    fn test_segment_metrics_trait_delegation() {
        // Test that the trait delegation works correctly
        let metrics = QueryMetrics::new("trait_test", 1, "test_bridge");

        // These should compile and not panic - they're delegating to base()
        metrics.segment_attempt(true);
        metrics.segment_retry_count(100, 5);
        metrics.error("network", "blocks");

        // Success if we got here
    }
}
