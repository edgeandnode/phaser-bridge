//! Prometheus metrics for phaser-query sync service
//!
//! Re-exports QueryMetrics and MetricsLayer from phaser-metrics crate

pub use phaser_metrics::{gather_metrics, MetricsLayer, QueryMetrics as SyncMetrics};
