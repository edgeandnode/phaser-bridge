/// Storage abstraction for indexes
///
/// This trait allows the indexing engine to work with any key-value store,
/// not just RocksDB. Implementations can use RocksDB, LMDB, Sled, or even
/// in-memory storage for testing.
use anyhow::Result;
use parquet_index_schema::FileId;
use std::path::{Path, PathBuf};

/// Abstraction over index storage backend
///
/// Allows swapping RocksDB for other KV stores or in-memory storage for tests
pub trait IndexStorage: Send + Sync {
    /// Get value for a key in a column family
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put key-value in a column family
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Write a batch of operations atomically
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Iterate with prefix (for range scans)
    fn prefix_iterator<'a>(
        &'a self,
        cf: &str,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;
}

/// Batch of write operations for atomic writes
#[derive(Debug, Default)]
pub struct WriteBatch {
    pub operations: Vec<WriteOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, cf: impl Into<String>, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(WriteOp::Put {
            cf: cf.into(),
            key,
            value,
        });
    }

    pub fn delete(&mut self, cf: impl Into<String>, key: Vec<u8>) {
        self.operations.push(WriteOp::Delete { cf: cf.into(), key });
    }
}

/// Single write operation
#[derive(Debug, Clone)]
pub enum WriteOp {
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
}

/// Registry for mapping FileId to file paths
///
/// This allows the indexing system to assign stable FileIds to parquet files
/// and later resolve them back to paths for reading.
pub trait FileRegistry: Send + Sync {
    /// Register a new file and get a FileId
    ///
    /// If the file is already registered, returns the existing FileId.
    fn register_file(&self, path: &Path) -> Result<FileId>;

    /// Get the file path for a given FileId
    fn get_file_path(&self, file_id: FileId) -> Result<PathBuf>;
}
