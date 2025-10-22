/// Core traits for building pluggable parquet indexes
///
/// This crate defines the traits that enable indexing any typed-arrow Record
/// into a key-value store (like RocksDB). The indexing logic is schema-agnostic.
use std::sync::Arc;
use typed_arrow::prelude::Record;

/// Reference to a specific file in the index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u32);

/// Points to a specific page within a Parquet file
///
/// Combined with the OffsetIndex from parquet metadata, this enables
/// precise byte-range reads of individual pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagePointer {
    pub file_id: FileId,
    pub row_group: u16,
    pub column_chunk: u16,
    pub page_index: u16,
    pub row_in_page: u16,
}

impl PagePointer {
    /// Serialize to fixed 12-byte representation
    pub fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&self.file_id.0.to_be_bytes());
        bytes[4..6].copy_from_slice(&self.row_group.to_be_bytes());
        bytes[6..8].copy_from_slice(&self.column_chunk.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.page_index.to_be_bytes());
        bytes[10..12].copy_from_slice(&self.row_in_page.to_be_bytes());
        bytes
    }

    /// Deserialize from 12-byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 12 {
            return Err(IndexError::InvalidPointerSize {
                expected: 12,
                got: bytes.len(),
            });
        }

        Ok(Self {
            file_id: FileId(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
            row_group: u16::from_be_bytes([bytes[4], bytes[5]]),
            column_chunk: u16::from_be_bytes([bytes[6], bytes[7]]),
            page_index: u16::from_be_bytes([bytes[8], bytes[9]]),
            row_in_page: u16::from_be_bytes([bytes[10], bytes[11]]),
        })
    }
}

/// Defines how to index a specific blockchain schema
///
/// Implement this trait to enable indexing for your Record type.
pub trait IndexableSchema: Send + Sync + 'static {
    /// The typed-arrow Record type for this schema
    type Record: Record;

    /// Name of this schema (e.g., "evm_transactions", "solana_transactions")
    fn schema_name() -> &'static str;

    /// Specifications for what to index
    fn index_specs() -> Vec<IndexSpec>;
}

/// Specification for a single index
pub struct IndexSpec {
    /// Column family name in the key-value store
    pub column_family: String,

    /// How to extract the key from a RecordBatch row
    pub key_extractor: Arc<dyn KeyExtractor>,

    /// The column index used for page lookup in OffsetIndex
    /// This is the column that the key extractor primarily reads from
    pub column_index: usize,
}

/// Extracts simple keys with zero-copy references into Arrow arrays
///
/// Use this for single-field keys (like tx_hash) where you can return a direct
/// reference to the underlying Arrow array data with no allocation.
pub trait ZeroCopyKeyExtractor: Send + Sync {
    /// Extract key reference from a specific row in a RecordBatch
    ///
    /// Returns None if this record shouldn't be indexed for this key
    /// (e.g., when an optional field is None).
    ///
    /// The returned reference points directly into the Arrow array - zero allocation.
    fn extract_ref<'a>(
        &self,
        batch: &'a arrow::record_batch::RecordBatch,
        row_idx: usize,
    ) -> Option<&'a [u8]>;
}

/// Extracts composite keys by building them into a buffer
///
/// Use this for multi-field keys (like address || block_num || tx_index) that need
/// to be constructed by concatenating multiple fields.
pub trait CompositeKeyExtractor: Send + Sync {
    /// Extract composite key into a caller-provided buffer
    ///
    /// The buffer is reused across rows to minimize allocations.
    /// Clear the buffer before building the key.
    ///
    /// Returns true if a key was extracted, false if this row should be skipped
    /// (e.g., when an optional field is None).
    fn extract_into(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        row_idx: usize,
        buf: &mut Vec<u8>,
    ) -> bool;
}

/// Unified trait for key extraction used by IndexBuilder
///
/// All key extractors must provide this interface. Implement either
/// ZeroCopyKeyExtractor or CompositeKeyExtractor, then manually implement
/// this trait to bridge to the IndexBuilder.
pub trait KeyExtractor: Send + Sync {
    /// Extract key into a caller-provided buffer
    ///
    /// Returns true if a key was extracted, false if this row should be skipped.
    fn extract_into(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        row_idx: usize,
        buf: &mut Vec<u8>,
    ) -> bool;
}

/// Extracts multiple keys from a single row
///
/// Use this for one-to-many indexes (e.g., indexing all account keys in a Solana transaction).
/// Uses a callback to avoid allocating a Vec of Vecs.
pub trait MultiKeyExtractor: Send + Sync {
    /// Extract all keys from a specific row in a RecordBatch
    ///
    /// Calls `key_callback` for each key found in this row.
    /// The callback receives a reference to avoid allocation - it should copy if needed.
    fn extract_keys<F>(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        row_idx: usize,
        key_callback: F,
    ) where
        F: FnMut(&[u8]);
}

/// Index-related errors
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Invalid pointer size: expected {expected} bytes, got {got}")]
    InvalidPointerSize { expected: usize, got: usize },

    #[error("Column not found in schema: {0}")]
    ColumnNotFound(String),

    #[error("Failed to convert RecordBatch: {0}")]
    RecordBatchConversion(String),

    #[error("Page not found for row index {0}")]
    PageNotFound(usize),

    #[error("Other error: {0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_pointer_serialization() {
        let pointer = PagePointer {
            file_id: FileId(42),
            row_group: 1,
            column_chunk: 2,
            page_index: 3,
            row_in_page: 100,
        };

        let bytes = pointer.to_bytes();
        let deserialized = PagePointer::from_bytes(&bytes).unwrap();

        assert_eq!(pointer, deserialized);
    }

    #[test]
    fn test_page_pointer_invalid_size() {
        let bytes = vec![0u8; 10]; // Wrong size
        let result = PagePointer::from_bytes(&bytes);

        assert!(matches!(
            result,
            Err(IndexError::InvalidPointerSize {
                expected: 12,
                got: 10
            })
        ));
    }

    #[test]
    fn test_file_id_equality() {
        let id1 = FileId(100);
        let id2 = FileId(100);
        let id3 = FileId(200);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
