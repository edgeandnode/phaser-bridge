/// Error types for parquet indexing operations
use parquet_index_schema::IndexError;
use std::path::PathBuf;

/// Errors that can occur during storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),

    #[error("Failed to read from storage: {0}")]
    ReadError(String),

    #[error("Failed to write to storage: {0}")]
    WriteError(String),

    #[error("File not found in registry: {0:?}")]
    FileNotFound(parquet_index_schema::FileId),

    #[error("Invalid file path: {0}")]
    InvalidFilePath(String),

    #[error("Storage backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Errors that can occur during index building
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("Failed to open parquet file: {path}")]
    FileOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to read parquet metadata: {0}")]
    MetadataRead(String),

    #[error("Failed to read record batch: {0}")]
    RecordBatchRead(String),

    #[error("Index error: {0}")]
    Index(#[from] IndexError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("File registry error: {0}")]
    FileRegistry(String),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Errors that can occur during page reading
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("File not found in registry: {0:?}")]
    FileNotFound(parquet_index_schema::FileId),

    #[error("Failed to open file: {path}")]
    FileOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to read parquet data: {0}")]
    ParquetRead(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Fusio error: {0}")]
    Fusio(#[from] fusio::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}
