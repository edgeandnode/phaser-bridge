use alloy::network::AnyRpcBlock;
use alloy_rpc_types_eth::{Header as RpcHeader, Log};
use anyhow::Result;
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use evm_common::rpc_conversions;
use std::sync::Arc;

/// Converter for JSON-RPC data to Arrow format
pub struct JsonRpcConverter;

impl JsonRpcConverter {
    /// Get the Arrow schema for blocks
    pub fn block_schema() -> Arc<Schema> {
        evm_common::block_arrow_schema()
    }

    /// Get the Arrow schema for transactions
    pub fn transaction_schema() -> Arc<Schema> {
        evm_common::transaction_arrow_schema()
    }

    /// Get the Arrow schema for logs
    pub fn log_schema() -> Arc<Schema> {
        evm_common::log_arrow_schema()
    }

    /// Convert an RPC header to a block RecordBatch
    pub fn convert_header(header: &RpcHeader) -> Result<RecordBatch> {
        Ok(rpc_conversions::convert_rpc_header(header)?)
    }

    /// Convert an AnyRpcBlock to RecordBatches
    pub fn convert_block(block: &AnyRpcBlock) -> Result<(RecordBatch, Option<RecordBatch>)> {
        Ok(rpc_conversions::convert_rpc_block(block)?)
    }

    /// Convert transactions from an AnyRpcBlock
    pub fn convert_transactions(block: &AnyRpcBlock) -> Result<RecordBatch> {
        Ok(rpc_conversions::convert_rpc_transactions(block)?)
    }

    /// Convert logs to RecordBatch
    pub fn convert_logs(
        logs: &[Log],
        block_num: u64,
        block_hash: alloy_primitives::B256,
        timestamp: u64,
    ) -> Result<RecordBatch> {
        Ok(rpc_conversions::convert_rpc_logs(
            logs, block_num, block_hash, timestamp,
        )?)
    }
}
