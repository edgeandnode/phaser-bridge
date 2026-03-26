//! Smart client for synchronizing blockchain data from Phaser bridges
//!
//! This module provides high-level orchestration for syncing data from bridges,
//! including:
//! - Work queue management with parallel workers
//! - Automatic retry with exponential backoff
//! - Error categorization for intelligent retry decisions
//! - Progress tracking and gap detection
//!
//! # Architecture
//!
//! The sync module is designed around the `BatchWriter` trait, which abstracts
//! the storage layer. Consumers implement `BatchWriter` to handle Arrow RecordBatches,
//! while the `PhaserSyncer` handles orchestration, retries, and error recovery.
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                      PhaserSyncer                             │
//! │  - Work queue management                                      │
//! │  - Worker pool coordination                                   │
//! │  - Retry logic with backoff                                   │
//! │  - Progress tracking                                          │
//! └──────────────────────────────────────────────────────────────┘
//!                              │
//!                              │ calls
//!                              ▼
//! ┌──────────────────────────────────────────────────────────────┐
//! │                      BatchWriter                              │
//! │  - write_batch(RecordBatch) -> Result<u64>                    │
//! │  - finalize() -> Result<()>                                   │
//! │  - last_written_block() -> Option<u64>                        │
//! └──────────────────────────────────────────────────────────────┘
//!                              │
//!                              │ implemented by
//!                              ▼
//! ┌─────────────────────┐  ┌─────────────────────┐
//! │   ParquetWriter     │  │   ArrowFlightSink   │
//! │   (phaser-query)    │  │   (AMP)             │
//! └─────────────────────┘  └─────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use phaser_client::sync::{PhaserSyncer, SyncConfig, BatchWriter};
//!
//! // Implement BatchWriter for your storage backend
//! struct MyWriter { /* ... */ }
//!
//! impl BatchWriter for MyWriter {
//!     async fn write_batch(&mut self, batch: RecordBatch) -> Result<u64, SyncError> {
//!         // Write to your storage
//!         Ok(bytes_written)
//!     }
//!     // ...
//! }
//!
//! // Create syncer with config
//! let config = SyncConfig::default();
//! let syncer = PhaserSyncer::new(config);
//!
//! // Sync a range with your writer
//! syncer.sync_range("http://bridge:50051", 0, 1_000_000, writer).await?;
//! ```

mod config;
mod error;
mod progress;
mod syncer;
mod writer;

pub use config::{RetryPolicy, SyncConfig};
pub use error::{
    categorize_error_message, DataType, ErrorCategory, MultipleDataTypeErrors, SyncError,
};
pub use progress::{Range, SegmentWork, SyncProgress, WorkerProgress};
pub use syncer::{PhaserSyncer, ProgressReceiver, ProgressSender, ProgressUpdate, SyncResult};
pub use writer::{BatchWriter, WriterFactory};
