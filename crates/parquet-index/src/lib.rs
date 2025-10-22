/// Generic parquet indexing engine
///
/// This crate provides the generic indexing infrastructure that works with
/// any `IndexableSchema` implementation. It handles:
/// - Building indexes from parquet files (IndexBuilder)
/// - Reading specific pages using fusio async I/O (PageReader)
/// - Abstracting over storage backends (IndexStorage trait)
pub mod builder;
pub mod error;
pub mod reader;
pub mod storage;

pub use builder::IndexBuilder;
pub use error::{BuilderError, ReaderError, StorageError};
pub use reader::PageReader;
pub use storage::{FileRegistry, IndexStorage, WriteBatch, WriteOp};

// Re-export core types from schema crate
pub use parquet_index_schema::{
    FileId, IndexError, IndexSpec, IndexableSchema, KeyExtractor, MultiKeyExtractor, PagePointer,
};
