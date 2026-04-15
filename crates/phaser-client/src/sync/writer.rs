//! BatchWriter trait for abstracting storage backends
//!
//! The BatchWriter trait is the primary abstraction point between the sync
//! orchestration (PhaserSyncer) and the storage layer. Consumers implement
//! this trait to handle Arrow RecordBatches however they need.

use arrow_array::RecordBatch;

use super::error::{DataType, SyncError};

/// Trait for writing batches of blockchain data to a storage backend
///
/// Implementors receive Arrow RecordBatches and are responsible for:
/// - Writing data to their storage backend (Parquet files, Arrow Flight, etc.)
/// - Tracking progress (last written block)
/// - Handling rotation/finalization when segments complete
///
/// # Example Implementation
///
/// ```ignore
/// use phaser_client::sync::{BatchWriter, SyncError, DataType};
/// use arrow_array::RecordBatch;
///
/// struct ParquetWriter {
///     // ...
/// }
///
/// #[async_trait::async_trait]
/// impl BatchWriter for ParquetWriter {
///     fn data_type(&self) -> DataType {
///         DataType::Blocks
///     }
///
///     async fn write_batch(&mut self, batch: RecordBatch) -> Result<u64, SyncError> {
///         // Write to parquet file
///         let bytes = self.inner_write(batch)?;
///         Ok(bytes)
///     }
///
///     fn last_written_block(&self) -> Option<u64> {
///         self.last_block
///     }
///
///     fn update_responsibility_end(&mut self, block: u64) {
///         self.responsibility_end = self.responsibility_end.max(block);
///     }
///
///     async fn finalize(&mut self) -> Result<(), SyncError> {
///         // Flush and close the file
///         self.flush()?;
///         Ok(())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait BatchWriter: Send {
    /// The type of data this writer handles (blocks, transactions, logs)
    fn data_type(&self) -> DataType;

    /// Write a batch of records
    ///
    /// Returns the number of bytes written (for progress tracking).
    /// The batch contains Arrow data from the bridge.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<u64, SyncError>;

    /// Get the last block number that was successfully written
    ///
    /// Used for resume logic - if a stream fails, we can resume from
    /// the next block after the last successfully written one.
    fn last_written_block(&self) -> Option<u64>;

    /// Update the responsibility end block from batch metadata
    ///
    /// The bridge sends metadata indicating the range it processed.
    /// This is important for empty batches - we need to know the range
    /// was covered even if no rows were returned.
    fn update_responsibility_end(&mut self, block: u64);

    /// Finalize the current output (flush, close file, etc.)
    ///
    /// Called when a segment is complete. Implementors should ensure
    /// all data is persisted and the file is properly closed.
    async fn finalize(&mut self) -> Result<(), SyncError>;

    /// Reset the writer for a new range
    ///
    /// Called when starting a new segment or resuming from a different point.
    /// Implementors should prepare for a fresh write.
    fn reset(&mut self) -> Result<(), SyncError> {
        Ok(()) // Default no-op
    }

    /// Set the block range this writer is responsible for
    ///
    /// segment_start/segment_end: The logical segment boundaries (for filenames)
    /// responsibility_start/responsibility_end: The actual blocks we're syncing
    fn set_ranges(
        &mut self,
        segment_start: u64,
        segment_end: u64,
        responsibility_start: u64,
        responsibility_end: u64,
    );
}

/// Factory for creating writers for different data types
///
/// Implementors provide this to create appropriately configured writers
/// for each data type (blocks, transactions, logs).
pub trait WriterFactory: Send + Sync {
    /// The writer type produced by this factory
    type Writer: BatchWriter;

    /// Create a writer for the given data type and segment
    ///
    /// Parameters:
    /// - data_type: The type of data to write
    /// - segment_start: Start of the segment (for filename)
    /// - segment_end: End of the segment (for filename)
    /// - responsibility_start: First block we're responsible for
    /// - responsibility_end: Last block we're responsible for
    fn create_writer(
        &self,
        data_type: DataType,
        segment_start: u64,
        segment_end: u64,
        responsibility_start: u64,
        responsibility_end: u64,
    ) -> Result<Self::Writer, SyncError>;
}
