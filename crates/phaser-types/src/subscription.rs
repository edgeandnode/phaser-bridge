//! Subscription types for streaming data from bridges

use serde::{Deserialize, Serialize};

/// How to handle backpressure when consumer is slow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackpressureStrategy {
    /// Drop oldest undelivered messages
    Drop,
    /// Buffer up to specified limit
    Buffer { max_size: usize },
    /// Pause upstream subscription
    Pause,
}

/// Options for creating a subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionOptions {
    /// Client-provided subscription ID
    pub id: String,
    /// Number of records per batch
    pub batch_size: u32,
    /// Maximum time to wait for batch to fill (ms)
    pub batch_timeout_ms: u64,
    /// How to handle slow consumers
    pub backpressure_strategy: BackpressureStrategy,
    /// Optional filters to apply server-side
    pub filters: Option<FilterSpec>,
}

/// Filter specification for server-side filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Filter by contract addresses
    pub addresses: Option<Vec<String>>,
    /// Filter by transaction hash
    pub tx_hashes: Option<Vec<String>>,
    /// Filter by block range
    pub block_range: Option<(u64, u64)>,
    /// Filter by topics (for logs)
    pub topics: Option<Vec<Option<String>>>,
}

/// Handle to an active subscription
///
/// Note: This type contains a Stream which is not Send+Sync by default.
/// It's defined here but typically constructed in the client crate.
pub struct SubscriptionHandle {
    /// Unique subscription ID
    pub id: String,
    /// Stream of record batches (boxed to avoid generic complexity)
    pub stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<arrow_array::RecordBatch, anyhow::Error>> + Send>,
    >,
    /// Last delivered block number (for resumption)
    pub checkpoint: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// Information about an active subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    pub id: String,
    pub created_at: u64,
    pub last_checkpoint: u64,
    pub pending_batches: usize,
    pub is_paused: bool,
    pub backpressure_triggered: bool,
}

/// Control actions for subscription management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlAction {
    Pause,
    Resume,
    Cancel,
    UpdateFilter(FilterSpec),
}

/// Query mode for data requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryMode {
    /// Query historical data between start and end blocks
    Historical { start: u64, end: u64 },
    /// Subscribe to live data from current head
    Live,
}
