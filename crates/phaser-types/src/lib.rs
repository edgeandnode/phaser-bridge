//! Shared protocol types for phaser
//!
//! This crate contains protocol types shared between phaser-client and phaser-server:
//! - Discovery types (DiscoveryCapabilities, GenericQuery, TableDescriptor)
//! - Descriptors (BlockchainDescriptor, StreamType)
//! - Subscription types (QueryMode, SubscriptionOptions)
//! - Batch metadata (BatchMetadata, ResponsibilityRange)

pub mod descriptors;
pub mod discovery;
pub mod subscription;

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

pub use descriptors::{
    BlockchainDescriptor, BridgeInfo, Compression, EndpointInfo, StreamPreferences, StreamType,
    ValidationStage,
};
pub use discovery::{
    DiscoveryCapabilities, FilterDescriptor, GenericQuery, GenericQueryMode, TableDescriptor,
    ACTION_DESCRIBE,
};
pub use subscription::{
    BackpressureStrategy, ControlAction, FilterSpec, QueryMode, SubscriptionHandle,
    SubscriptionInfo, SubscriptionOptions,
};

/// Responsibility range metadata for a batch
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResponsibilityRange {
    pub start_block: u64,
    pub end_block: u64,
}

/// Metadata attached to each batch via FlightData.app_metadata
///
/// This struct is encoded/decoded from FlightData.app_metadata and can be extended
/// over time with new fields. When adding fields:
/// - Use Option<T> for truly optional metadata
/// - Add required fields carefully (breaks compatibility)
/// - Document the field's purpose and when it was added
/// - Update BatchMetadata::decode() to handle the new field
///
/// Current usage:
/// - Bridge encodes this into FlightData.app_metadata via BatchMetadata::encode()
/// - Client decodes from FlightData.app_metadata via BatchMetadata::decode()
/// - Returned as part of subscribe_with_metadata() stream items
///
/// Historical context:
/// - v1: Added responsibility_range (required) for gap detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMetadata {
    /// Block range this batch is responsible for (which blocks were checked)
    ///
    /// This range indicates what blocks the bridge processed, even if the batch
    /// contains 0 rows. Critical for distinguishing between "unchecked blocks"
    /// and "checked but empty blocks" in gap detection.
    ///
    /// When a bridge processes blocks X to Y, all resulting batches will carry
    /// the same responsibility range (X, Y), even if split across multiple
    /// FlightData messages due to size limits.
    pub responsibility_range: ResponsibilityRange,
    // Future fields can be added here:
    // /// Compression ratio achieved for this batch (added v2)
    // pub compression_ratio: Option<f32>,
    //
    // /// If batch was split, index of this piece (added v2)
    // pub split_index: Option<u32>,
}

impl BatchMetadata {
    /// Create new metadata with responsibility range
    pub fn new(start_block: u64, end_block: u64) -> Self {
        Self {
            responsibility_range: ResponsibilityRange {
                start_block,
                end_block,
            },
        }
    }

    /// Encode metadata to bytes for FlightData.app_metadata
    pub fn encode(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Decode metadata from FlightData.app_metadata bytes
    ///
    /// Returns an error if metadata is missing, empty, or invalid.
    /// This enforces that all batches from subscribe_with_metadata() must
    /// include proper metadata.
    pub fn decode(metadata: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if metadata.is_empty() {
            return Err("FlightData.app_metadata is empty - bridge must send BatchMetadata".into());
        }

        bincode::deserialize(metadata)
            .map_err(|e| format!("Failed to decode BatchMetadata from app_metadata: {e}").into())
    }
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

    /// Encode as BatchMetadata for app_metadata
    pub fn encode_metadata(&self) -> Result<Vec<u8>, bincode::Error> {
        let metadata = BatchMetadata::new(self.start_block, self.end_block);
        metadata.encode()
    }

    /// Legacy method - use BatchMetadata::encode() instead
    #[deprecated(note = "Use encode_metadata() or BatchMetadata::encode() directly")]
    pub fn encode_range_metadata(&self) -> Result<Vec<u8>, bincode::Error> {
        self.encode_metadata()
    }

    /// Legacy method - use BatchMetadata::decode() instead
    #[deprecated(note = "Use BatchMetadata::decode() directly")]
    pub fn decode_range_metadata(metadata: &[u8]) -> Option<(u64, u64)> {
        BatchMetadata::decode(metadata).ok().map(|m| {
            (
                m.responsibility_range.start_block,
                m.responsibility_range.end_block,
            )
        })
    }
}
