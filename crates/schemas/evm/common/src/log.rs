use crate::types::{Address20, Hash32};
use typed_arrow::Record;

/// EVM Log schema - common across all EVM chains
#[derive(Record, Clone, Debug)]
pub struct LogRecord {
    /// Partition key for efficient storage
    pub _block_num: u64,

    /// Block number
    pub block_num: u64,

    /// Block hash
    pub block_hash: Hash32,

    /// Block timestamp in nanoseconds
    pub timestamp: i64,

    /// Transaction index within the block
    pub tx_index: u32,

    /// Transaction hash
    pub tx_hash: Hash32,

    /// Log index within the transaction
    pub log_index: u32,

    /// Contract address that emitted the log
    pub address: Address20,

    /// Log data
    pub data: Vec<u8>,

    /// Log topics (up to 4)
    pub topic0: Option<Hash32>,
    pub topic1: Option<Hash32>,
    pub topic2: Option<Hash32>,
    pub topic3: Option<Hash32>,

    /// Whether the log was removed (chain reorg)
    pub removed: bool,
}

/// Context for converting logs
pub struct LogContext {
    pub address: Address20,
    pub block_hash: Hash32,
    pub block_num: u64,
    pub timestamp: i64, // nanos
    pub data: Vec<u8>,
    pub log_index: u32,
    pub topics: Vec<Hash32>,
    pub tx_hash: Hash32,
    pub tx_index: u32,
    pub removed: bool,
}

impl From<LogContext> for LogRecord {
    fn from(ctx: LogContext) -> Self {
        // Extract topics (up to 4)
        let topic0 = ctx.topics.first().cloned();
        let topic1 = ctx.topics.get(1).cloned();
        let topic2 = ctx.topics.get(2).cloned();
        let topic3 = ctx.topics.get(3).cloned();

        LogRecord {
            _block_num: ctx.block_num,
            block_num: ctx.block_num,
            block_hash: ctx.block_hash,
            timestamp: ctx.timestamp,
            tx_index: ctx.tx_index,
            tx_hash: ctx.tx_hash,
            log_index: ctx.log_index,
            address: ctx.address,
            data: ctx.data,
            topic0,
            topic1,
            topic2,
            topic3,
            removed: ctx.removed,
        }
    }
}
