//! Conversions from JSON-RPC types to Arrow schemas

use crate::block::BlockRecord;
use crate::error::{EvmCommonError, Result};
use crate::log::LogRecord;
use crate::transaction::{TransactionContext, TransactionRecord};
use alloy_consensus::Header as ConsensusHeader;
use alloy_network::{AnyHeader, AnyRpcBlock, AnyTxEnvelope, TransactionResponse};
use alloy_rpc_types_eth::{Header as RpcHeader, Log as RpcLog};
use arrow_array::RecordBatch;
use typed_arrow::prelude::BuildRows;

/// Convert an RPC header to a block RecordBatch
pub fn convert_rpc_header(header: &RpcHeader) -> Result<RecordBatch> {
    // RPC Header contains an inner consensus header we can access via Deref
    // The inner header is already a ConsensusHeader
    let consensus_header = ConsensusHeader {
        parent_hash: header.parent_hash,
        ommers_hash: header.ommers_hash,
        beneficiary: header.beneficiary,
        state_root: header.state_root,
        transactions_root: header.transactions_root,
        receipts_root: header.receipts_root,
        withdrawals_root: header.withdrawals_root,
        logs_bloom: header.logs_bloom,
        difficulty: header.difficulty,
        number: header.number,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        timestamp: header.timestamp,
        mix_hash: header.mix_hash,
        nonce: header.nonce,
        base_fee_per_gas: header.base_fee_per_gas,
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_block_root: header.parent_beacon_block_root,
        requests_hash: header.requests_hash,
        extra_data: header.extra_data.clone(),
    };

    // Use hash from RPC response (already computed) instead of recomputing
    let block_record = BlockRecord::from_header_with_hash(header.hash, &consensus_header);

    // Build RecordBatch
    let mut builders = BlockRecord::new_builders(1);
    builders.append_row(block_record);
    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}

/// Convert an AnyRpcBlock to RecordBatches (header and transactions)
pub fn convert_rpc_block(block: &AnyRpcBlock) -> Result<(RecordBatch, Option<RecordBatch>)> {
    let header = &block.header;
    let _block_num = header.number;

    // Convert header - handle AnyHeader type
    let header_batch = convert_any_header(header)?;

    // Convert transactions if present
    let tx_batch = if !block.transactions.is_empty() {
        Some(convert_rpc_transactions(block)?)
    } else {
        None
    };

    Ok((header_batch, tx_batch))
}

/// Extract transaction records from an AnyRpcBlock
pub fn extract_transaction_records(block: &AnyRpcBlock) -> Result<Vec<TransactionRecord>> {
    let header = &block.header;
    let block_num = header.number;
    let block_hash = header.hash;
    let timestamp = header.timestamp;

    let transactions = block.transactions.as_transactions().ok_or_else(|| {
        EvmCommonError::InvalidBlock("Block does not contain full transactions".to_string())
    })?;

    let mut records = Vec::new();

    for (idx, any_rpc_tx) in transactions.iter().enumerate() {
        // Try to extract TxEnvelope from AnyRpcTransaction
        let any_tx_envelope: AnyTxEnvelope = any_rpc_tx.clone().into();

        // Check if it's an Ethereum transaction
        if let AnyTxEnvelope::Ethereum(tx_envelope) = any_tx_envelope {
            // Use existing conversion with TransactionContext
            let context = TransactionContext {
                tx: &tx_envelope,
                block_hash: block_hash.into(),
                block_num,
                timestamp: timestamp as i64 * 1_000_000_000,
                tx_index: idx as u32,
                from: any_rpc_tx.from().into(),
                gas_used: 0,  // Would need receipt data
                status: true, // Would need receipt data
            };

            let record = TransactionRecord::from(context);
            records.push(record);
        } else {
            // Handle unknown transaction types
            // For now, skip them
            continue;
        }
    }

    Ok(records)
}

/// Convert RPC transactions from an AnyRpcBlock
pub fn convert_rpc_transactions(block: &AnyRpcBlock) -> Result<RecordBatch> {
    let records = extract_transaction_records(block)?;

    let mut builders = TransactionRecord::new_builders(records.len());
    for record in records {
        builders.append_row(record);
    }

    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}

/// Convert RPC logs to RecordBatch (single block version)
///
/// For logs from a single block where you already know the block context.
/// For logs from multiple blocks (e.g., eth_getLogs with block range), use
/// `convert_rpc_logs_multi_block` instead.
pub fn convert_rpc_logs(
    logs: &[RpcLog],
    block_num: u64,
    block_hash: alloy_primitives::B256,
    timestamp: u64,
) -> Result<RecordBatch> {
    let mut builders = LogRecord::new_builders(logs.len());

    for log in logs {
        // Extract topics (up to 4)
        let topics = log.topics();
        let topic0 = topics.first().map(|t| (*t).into());
        let topic1 = topics.get(1).map(|t| (*t).into());
        let topic2 = topics.get(2).map(|t| (*t).into());
        let topic3 = topics.get(3).map(|t| (*t).into());

        let record = LogRecord {
            _block_num: block_num,
            block_num,
            block_hash: block_hash.into(),
            timestamp: timestamp as i64 * 1_000_000_000,
            tx_index: log.transaction_index.unwrap_or(0) as u32,
            tx_hash: log.transaction_hash.unwrap_or_default().into(),
            log_index: log.log_index.unwrap_or(0) as u32,
            address: log.address().into(),
            data: log.data().data.to_vec(),
            topic0,
            topic1,
            topic2,
            topic3,
            removed: log.removed,
        };

        builders.append_row(record);
    }

    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}

/// Convert RPC logs to RecordBatch (multi-block version)
///
/// For logs from eth_getLogs spanning multiple blocks. Extracts block_num,
/// block_hash, and timestamp from each log individually. This is more efficient
/// than calling `convert_rpc_logs` per-block because it reduces RPC round-trips.
pub fn convert_rpc_logs_multi_block(logs: &[RpcLog]) -> Result<RecordBatch> {
    let mut builders = LogRecord::new_builders(logs.len());

    for log in logs {
        // Extract block context from the log itself
        let block_num = log.block_number.unwrap_or(0);
        let block_hash = log.block_hash.unwrap_or_default();
        // Note: block_timestamp is not always present in all nodes
        // If missing, use 0 and let downstream handle it
        let timestamp = log.block_timestamp.unwrap_or(0);

        // Extract topics (up to 4)
        let topics = log.topics();
        let topic0 = topics.first().map(|t| (*t).into());
        let topic1 = topics.get(1).map(|t| (*t).into());
        let topic2 = topics.get(2).map(|t| (*t).into());
        let topic3 = topics.get(3).map(|t| (*t).into());

        let record = LogRecord {
            _block_num: block_num,
            block_num,
            block_hash: block_hash.into(),
            timestamp: timestamp as i64 * 1_000_000_000,
            tx_index: log.transaction_index.unwrap_or(0) as u32,
            tx_hash: log.transaction_hash.unwrap_or_default().into(),
            log_index: log.log_index.unwrap_or(0) as u32,
            address: log.address().into(),
            data: log.data().data.to_vec(),
            topic0,
            topic1,
            topic2,
            topic3,
            removed: log.removed,
        };

        builders.append_row(record);
    }

    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}

/// Convert an AnyHeader to a BlockRecord
pub fn convert_any_header_to_record(header: &AnyHeader) -> BlockRecord {
    // AnyHeader derefs to the inner consensus header
    let consensus_header = ConsensusHeader {
        parent_hash: header.parent_hash,
        ommers_hash: header.ommers_hash,
        beneficiary: header.beneficiary,
        state_root: header.state_root,
        transactions_root: header.transactions_root,
        receipts_root: header.receipts_root,
        withdrawals_root: header.withdrawals_root,
        logs_bloom: header.logs_bloom,
        difficulty: header.difficulty,
        number: header.number,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        timestamp: header.timestamp,
        mix_hash: header.mix_hash.unwrap_or_default(),
        nonce: header.nonce.unwrap_or_default(),
        base_fee_per_gas: header.base_fee_per_gas,
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_block_root: header.parent_beacon_block_root,
        requests_hash: header.requests_hash,
        extra_data: header.extra_data.clone(),
    };

    // AnyHeader doesn't expose hash field, so compute it from consensus header
    let hash = consensus_header.hash_slow();
    BlockRecord::from_header_with_hash(hash, &consensus_header)
}

/// Convert an AnyHeader to a block RecordBatch
pub fn convert_any_header(header: &AnyHeader) -> Result<RecordBatch> {
    let block_record = convert_any_header_to_record(header);

    // Build RecordBatch
    let mut builders = BlockRecord::new_builders(1);
    builders.append_row(block_record);
    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}
