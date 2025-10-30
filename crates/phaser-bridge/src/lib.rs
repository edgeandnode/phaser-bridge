pub mod bridge;
pub mod client;
pub mod descriptors;
pub mod error;
pub mod server;
pub mod subscription;

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

pub use bridge::{BridgeCapabilities, FlightBridge};
pub use client::FlightBridgeClient;
pub use descriptors::{BlockchainDescriptor, StreamType, ValidationStage};
pub use error::StreamError;
pub use server::FlightBridgeServer;
pub use subscription::{
    BackpressureStrategy, BlockRange, ControlAction, DataAvailability, DataSource, FilterSpec,
    QueryMode, SubscriptionHandle, SubscriptionInfo, SubscriptionOptions,
};

/// Responsibility range metadata for a batch
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResponsibilityRange {
    pub start_block: u64,
    pub end_block: u64,
}

/// Wrapper for RecordBatch with responsibility range metadata
///
/// The responsibility range indicates which blocks this batch was responsible for processing,
/// even if the batch contains 0 rows. This is critical for tracking progress when processing
/// block ranges that have no transactions/logs.
///
/// When a bridge processes blocks X to Y, all resulting RecordBatches (even if split due to
/// size limits) will carry the same responsibility range (X, Y). This allows the client to:
/// - Track which block ranges have been fully processed
/// - Mark ranges as complete even when they contain 0 rows
/// - Detect gaps or out-of-order delivery
#[derive(Debug, Clone)]
pub struct BatchWithRange {
    pub batch: RecordBatch,
    pub start_block: u64,
    pub end_block: u64,
}

impl BatchWithRange {
    pub fn new(batch: RecordBatch, start_block: u64, end_block: u64) -> Self {
        Self {
            batch,
            start_block,
            end_block,
        }
    }

    /// Encode the block range as bytes for app_metadata using bincode
    pub fn encode_range_metadata(&self) -> Result<Vec<u8>, bincode::Error> {
        let range = ResponsibilityRange {
            start_block: self.start_block,
            end_block: self.end_block,
        };
        bincode::serialize(&range)
    }

    /// Decode block range from app_metadata bytes using bincode
    ///
    /// Returns None if metadata is invalid or empty
    pub fn decode_range_metadata(metadata: &[u8]) -> Option<(u64, u64)> {
        if metadata.is_empty() {
            return None;
        }

        let range: ResponsibilityRange = bincode::deserialize(metadata).ok()?;
        Some((range.start_block, range.end_block))
    }
}
