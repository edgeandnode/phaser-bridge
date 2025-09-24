use serde::{Deserialize, Serialize};

/// Information about a Parquet file and its block range
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_path: String,
    pub block_start: u64,
    pub block_end: u64,
    pub row_count: u32,
}

/// Points to a specific block within a Parquet file
#[derive(Debug, Clone)]
pub struct BlockPointer {
    pub row_group: u16,
    pub row_offset: u32,
}

impl BlockPointer {
    pub fn to_bytes(&self) -> [u8; 6] {
        let mut bytes = [0u8; 6];
        bytes[0..2].copy_from_slice(&self.row_group.to_be_bytes());
        bytes[2..6].copy_from_slice(&self.row_offset.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 6 {
            return Err(IndexError::InvalidPointerSize {
                expected: 6,
                got: bytes.len(),
            });
        }
        Ok(Self {
            row_group: u16::from_be_bytes([bytes[0], bytes[1]]),
            row_offset: u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]),
        })
    }
}

/// Points to a transaction within a Parquet file
#[derive(Debug, Clone)]
pub struct TransactionPointer {
    pub block_number: u64,
    pub row_group: u16,
    pub row_offset: u32,
}

impl TransactionPointer {
    pub fn to_bytes(&self) -> [u8; 14] {
        let mut bytes = [0u8; 14];
        bytes[0..8].copy_from_slice(&self.block_number.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.row_group.to_be_bytes());
        bytes[10..14].copy_from_slice(&self.row_offset.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 14 {
            return Err(IndexError::InvalidPointerSize {
                expected: 14,
                got: bytes.len(),
            });
        }
        Ok(Self {
            block_number: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            row_group: u16::from_be_bytes(bytes[8..10].try_into().unwrap()),
            row_offset: u32::from_be_bytes(bytes[10..14].try_into().unwrap()),
        })
    }
}

/// Points to a log entry within a Parquet file
#[derive(Debug, Clone)]
pub struct LogPointer {
    pub row_group: u16,
    pub row_offset: u32,
}

impl LogPointer {
    pub fn to_bytes(&self) -> [u8; 6] {
        let mut bytes = [0u8; 6];
        bytes[0..2].copy_from_slice(&self.row_group.to_be_bytes());
        bytes[2..6].copy_from_slice(&self.row_offset.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() != 6 {
            return Err(IndexError::InvalidPointerSize {
                expected: 6,
                got: bytes.len(),
            });
        }
        Ok(Self {
            row_group: u16::from_be_bytes([bytes[0], bytes[1]]),
            row_offset: u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]),
        })
    }
}

/// Column family names
pub mod cf {
    // File registry indexes
    pub const BLOCK_FILES: &str = "block_files";
    pub const TX_FILES: &str = "tx_files";
    pub const LOG_FILES: &str = "log_files";

    // Data indexes
    pub const BLOCKS: &str = "blocks";
    pub const TRANSACTIONS: &str = "transactions";
    pub const LOGS: &str = "logs";

    // Buffer and streaming indexes
    pub const STREAMING_BUFFER: &str = "streaming_buffer";
    pub const STREAMING_INDEX: &str = "streaming_index";
    pub const HISTORICAL_INDEX: &str = "historical_index";
}

/// Index-related errors
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Invalid pointer size: expected {expected} bytes, got {got}")]
    InvalidPointerSize { expected: usize, got: usize },

    #[error("Block not indexed: {0}")]
    BlockNotIndexed(u64),

    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
}

/// Key builders for consistent key formatting
pub mod keys {
    pub fn block_key(block_number: u64) -> [u8; 8] {
        block_number.to_be_bytes()
    }

    pub fn tx_key(tx_hash: &[u8; 32]) -> [u8; 32] {
        *tx_hash
    }

    pub fn log_key(block_number: u64, log_index: u32) -> [u8; 12] {
        let mut key = [0u8; 12];
        key[0..8].copy_from_slice(&block_number.to_be_bytes());
        key[8..12].copy_from_slice(&log_index.to_be_bytes());
        key
    }

    pub fn file_range_key(block_start: u64) -> [u8; 8] {
        block_start.to_be_bytes()
    }
}
