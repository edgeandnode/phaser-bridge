//! Flight client for connecting to phaser bridges
//!
//! This crate provides a client for connecting to Phaser bridges, with both
//! low-level Flight protocol access and high-level sync orchestration.
//!
//! # Low-Level API
//!
//! The `PhaserClient` provides direct access to bridge streams:
//!
//! ```ignore
//! use phaser_client::{PhaserClient, BridgeError, GenericQuery};
//!
//! async fn example() -> Result<(), BridgeError> {
//!     let mut client = PhaserClient::connect("http://localhost:50051".into()).await?;
//!
//!     // Discover available tables
//!     let capabilities = client.discover().await?;
//!     println!("Bridge: {} with {} tables", capabilities.name, capabilities.tables.len());
//!
//!     // Query data
//!     let query = GenericQuery::historical("blocks", 0, 100);
//!     let stream = client.query(query).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # High-Level Sync API
//!
//! The `sync` module provides orchestration for syncing large amounts of data
//! with automatic retry, work queues, and progress tracking:
//!
//! ```ignore
//! use phaser_client::sync::{PhaserSyncer, SyncConfig, BatchWriter, DataType};
//!
//! // Implement BatchWriter for your storage backend
//! struct MyWriter { /* ... */ }
//!
//! impl BatchWriter for MyWriter {
//!     fn data_type(&self) -> DataType { DataType::Blocks }
//!     async fn write_batch(&mut self, batch: RecordBatch) -> Result<u64, SyncError> {
//!         // Write to your storage
//!         Ok(bytes_written)
//!     }
//!     // ...
//! }
//!
//! // Use the syncer
//! let config = SyncConfig::default();
//! let syncer = PhaserSyncer::new(config, writer_factory);
//! let result = syncer.sync_range("http://bridge:50051", 0, 1_000_000, work).await?;
//! ```

mod client;
mod error;
pub mod sync;

pub use client::PhaserClient;
pub use error::BridgeError;

// Re-export core types for convenience
pub use phaser_types::{
    // Subscription
    BackpressureStrategy,
    // Batch metadata
    BatchMetadata,
    BatchWithRange,
    // Descriptors
    BlockchainDescriptor,
    BridgeInfo,
    Compression,
    ControlAction,
    // Discovery
    DiscoveryCapabilities,
    FilterDescriptor,
    FilterSpec,
    GenericQuery,
    GenericQueryMode,
    QueryMode,
    ResponsibilityRange,
    StreamPreferences,
    StreamType,
    SubscriptionInfo,
    SubscriptionOptions,
    TableDescriptor,
    ValidationStage,
    ACTION_DESCRIBE,
};
