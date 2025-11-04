//! Prometheus metrics for phaser-query sync service
//!
//! Re-exports QueryMetrics from phaser-metrics crate

pub use phaser_metrics::{gather_metrics, QueryMetrics as SyncMetrics};
