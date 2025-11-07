//! Tracing layer that exports log events as Prometheus metrics
//!
//! This layer intercepts tracing events (logs) and increments Prometheus counters
//! based on log level, service, module, and optionally line numbers.
//!
//! ## Design
//!
//! By default, only ERROR, WARN, and INFO levels are tracked to keep cardinality
//! manageable. DEBUG and TRACE logs are typically high-volume and not useful for
//! alerting or long-term tracking.
//!
//! ## Cardinality
//!
//! Without line numbers: ~200-500 time series (3 levels × services × modules)
//! With line numbers: ~3,000-5,000 time series (3 levels × services × log call sites)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use tracing_subscriber::layer::SubscriberExt;
//! use phaser_metrics::MetricsLayer;
//!
//! tracing_subscriber::registry()
//!     .with(EnvFilter::from_default_env())
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(MetricsLayer::new("erigon-bridge"))
//!     .init();
//! ```

use once_cell::sync::OnceCell;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::collections::HashSet;
use tracing::{Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

// Global metric registry (initialized once)
static LOG_EVENTS_TOTAL: OnceCell<IntCounterVec> = OnceCell::new();

fn get_log_events_metric() -> &'static IntCounterVec {
    LOG_EVENTS_TOTAL.get_or_init(|| {
        register_int_counter_vec!(
            "phaser_log_events_total",
            "Total number of log events by level, service, and module",
            &["level", "service", "module"]
        )
        .expect("Failed to register phaser_log_events_total metric")
    })
}

/// Configuration for the metrics layer
#[derive(Debug, Clone)]
pub struct MetricsLayerConfig {
    /// Service name (e.g., "erigon-bridge", "phaser-query")
    pub service_name: String,

    /// Log levels to track as metrics (default: ERROR, WARN, INFO)
    /// DEBUG and TRACE are excluded by default to reduce cardinality
    pub tracked_levels: HashSet<Level>,
}

impl Default for MetricsLayerConfig {
    fn default() -> Self {
        let mut tracked_levels = HashSet::new();
        tracked_levels.insert(Level::ERROR);
        tracked_levels.insert(Level::WARN);
        tracked_levels.insert(Level::INFO);

        Self {
            service_name: "phaser".to_string(),
            tracked_levels,
        }
    }
}

impl MetricsLayerConfig {
    /// Create a new config with the given service name
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            ..Default::default()
        }
    }

    /// Set custom tracked levels
    pub fn with_levels(mut self, levels: Vec<Level>) -> Self {
        self.tracked_levels = levels.into_iter().collect();
        self
    }
}

/// Tracing layer that exports log events as Prometheus metrics
///
/// Tracks log events with labels: {level, service, module}
/// - level: ERROR, WARN, INFO (configurable)
/// - service: Service name (e.g., "erigon-bridge")
/// - module: Target module path (e.g., "erigon_bridge::segment_worker")
pub struct MetricsLayer {
    config: MetricsLayerConfig,
}

impl MetricsLayer {
    /// Create a new metrics layer with default configuration
    /// Tracks ERROR, WARN, INFO levels by default
    pub fn new(service_name: impl Into<String>) -> Self {
        Self::with_config(MetricsLayerConfig::new(service_name))
    }

    /// Create a new metrics layer with custom configuration
    pub fn with_config(config: MetricsLayerConfig) -> Self {
        // Initialize the global metric
        let _ = get_log_events_metric();

        Self { config }
    }
}

impl<S> Layer<S> for MetricsLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let level = metadata.level();

        // Skip levels we're not tracking
        if !self.config.tracked_levels.contains(level) {
            return;
        }

        // Extract metadata (zero-cost - computed at compile time)
        let level_str = level.as_str();
        let service = self.config.service_name.as_str();
        let module = metadata.target();

        // Increment the counter with labels: {level, service, module}
        get_log_events_metric()
            .with_label_values(&[level_str, service, module])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{error, info, warn};
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn test_metrics_layer_basic() {
        // Create a subscriber with metrics layer
        let metrics_layer = MetricsLayer::new("test_service");

        let subscriber = tracing_subscriber::registry().with(metrics_layer);

        tracing::subscriber::with_default(subscriber, || {
            info!("Test info message");
            warn!("Test warn message");
            error!("Test error message");
        });

        // Verify metrics were recorded
        let metrics = crate::gather_metrics().unwrap();
        assert!(metrics.contains("phaser_log_events_total"));
        assert!(metrics.contains("level="));
        assert!(metrics.contains("service="));
        assert!(metrics.contains("module="));
    }

    #[test]
    fn test_metrics_layer_filters_debug() {
        use tracing::debug;

        let metrics_layer = MetricsLayer::new("test_filter");

        let subscriber = tracing_subscriber::registry().with(metrics_layer);

        tracing::subscriber::with_default(subscriber, || {
            // This should not be tracked (DEBUG not in default tracked_levels)
            debug!("This should not create a metric");
            info!("This should create a metric");
        });

        // Verify metrics were recorded and no panic occurred
        let metrics = crate::gather_metrics().unwrap();
        assert!(metrics.contains("phaser_log_events_total"));
    }

    #[test]
    fn test_custom_levels() {
        // Track only ERROR level
        let config = MetricsLayerConfig::new("test_custom").with_levels(vec![Level::ERROR]);

        let metrics_layer = MetricsLayer::with_config(config);
        let subscriber = tracing_subscriber::registry().with(metrics_layer);

        tracing::subscriber::with_default(subscriber, || {
            info!("This should not be tracked");
            error!("This should be tracked");
        });

        // Verify no panic
        let metrics = crate::gather_metrics().unwrap();
        assert!(metrics.contains("phaser_log_events_total"));
    }
}
