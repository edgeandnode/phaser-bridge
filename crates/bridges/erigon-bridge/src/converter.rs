use crate::proto::remote::{BlockReply, SubscribeReply, SubscribeLogsReply};
use alloy_consensus::{Block, Header, TxEnvelope};
use anyhow::{anyhow, Result};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
type EthBlock = Block<TxEnvelope>;
use alloy_rlp::Decodable;
use std::sync::Arc;
use tracing::{debug, error};
use evm_common::block::BlockRecord;
use evm_common::transaction::{TransactionRecord, TransactionContext};
use evm_common::log::{LogRecord, LogContext};
use evm_common::types::{Hash32, Address20};
use typed_arrow::prelude::BuildRows;

/// Converter for Erigon data to Arrow format
pub struct ErigonDataConverter;

impl ErigonDataConverter {
    /// Get the Arrow schema for blocks
    pub fn block_schema() -> Arc<Schema> {
        evm_common::block_arrow_schema()
    }

    /// Convert SubscribeReply to RecordBatch using typed-arrow
    pub fn convert_subscribe_reply(reply: &SubscribeReply) -> Result<RecordBatch> {
        if reply.data.is_empty() {
            return Err(anyhow!("No data in reply"));
        }

        // Decode the RLP-encoded block header using Alloy
        let header = Header::decode(&mut &reply.data[..])?;

        debug!(
            "Successfully decoded block #{} at timestamp {} with hash 0x{}",
            header.number,
            header.timestamp,
            hex::encode(header.hash_slow().as_slice())
        );

        // Convert header to BlockRecord using From trait
        let block = BlockRecord::from(&header);

        // Build RecordBatch using typed-arrow
        let mut builders = BlockRecord::new_builders(1);
        builders.append_row(block);
        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Get the Arrow schema for transactions
    pub fn transaction_schema() -> Arc<Schema> {
        evm_common::transaction_arrow_schema()
    }

    /// Get the Arrow schema for logs
    pub fn log_schema() -> Arc<Schema> {
        evm_common::log_arrow_schema()
    }

    /// Convert a full block from Erigon to RecordBatches (header + transactions)
    pub fn convert_full_block(
        block_reply: &BlockReply,
    ) -> Result<(RecordBatch, Option<RecordBatch>)> {
        debug!(
            "Starting conversion - RLP size: {} bytes, senders: {} bytes",
            block_reply.block_rlp.len(),
            block_reply.senders.len()
        );

        // Decode the full block from RLP
        let block = match EthBlock::decode(&mut &block_reply.block_rlp[..]) {
            Ok(b) => {
                debug!(
                    "Successfully decoded block #{} with {} transactions",
                    b.header.number,
                    b.body.transactions.len()
                );
                b
            }
            Err(e) => {
                error!(
                    "Failed to decode RLP block (size: {} bytes): {}",
                    block_reply.block_rlp.len(),
                    e
                );
                error!(
                    "First 100 bytes of RLP: {:?}",
                    &block_reply.block_rlp[..block_reply.block_rlp.len().min(100)]
                );
                return Err(e.into());
            }
        };

        // Convert header to RecordBatch
        debug!(
            "Converting header for block #{}",
            block.header.number
        );
        let header_batch = Self::convert_header_to_batch(&block.header)?;
        debug!(
            "Header batch created with {} rows, {} columns",
            header_batch.num_rows(),
            header_batch.num_columns()
        );

        // If there are transactions, convert them
        let tx_batch = if !block.body.transactions.is_empty() {
            debug!(
                "Converting {} transactions for block #{}",
                block.body.transactions.len(),
                block.header.number
            );
            let batch = Self::convert_transactions_to_batch(&block, &block_reply.senders)?;
            debug!(
                "Transaction batch created with {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            Some(batch)
        } else {
            debug!("DEBUG: Block #{} has no transactions", block.header.number);
            None
        };

        debug!(
            "Successfully converted full block #{} - header: ✓, transactions: {}",
            block.header.number,
            if tx_batch.is_some() { "✓" } else { "none" }
        );

        Ok((header_batch, tx_batch))
    }

    /// Convert a header to RecordBatch using typed-arrow
    fn convert_header_to_batch(header: &Header) -> Result<RecordBatch> {
        debug!(
            "Processing block #{} at timestamp {} with hash 0x{}",
            header.number,
            header.timestamp,
            hex::encode(header.hash_slow().as_slice())
        );

        // Convert header to BlockRecord using From trait
        let block = BlockRecord::from(header);

        // Build RecordBatch using typed-arrow
        let mut builders = BlockRecord::new_builders(1);
        builders.append_row(block);
        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Convert logs to RecordBatch using typed-arrow
    pub fn convert_logs_to_batch(
        logs: &[SubscribeLogsReply],
        block_timestamp: i64, // nanos
    ) -> Result<RecordBatch> {
        let mut builders = LogRecord::new_builders(logs.len());

        for log in logs {
            // Convert H160 address to Address20
            let address = if let Some(addr) = &log.address {
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(&addr.hash);
                Address20 { bytes }
            } else {
                return Err(anyhow!("Log missing address"));
            };

            // Convert H256 hashes to Hash32
            let block_hash = if let Some(hash) = &log.block_hash {
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&hash.hash);
                Hash32 { bytes }
            } else {
                return Err(anyhow!("Log missing block_hash"));
            };

            let tx_hash = if let Some(hash) = &log.transaction_hash {
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&hash.hash);
                Hash32 { bytes }
            } else {
                return Err(anyhow!("Log missing transaction_hash"));
            };

            // Convert topics
            let topics: Vec<Hash32> = log.topics.iter()
                .map(|t| {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&t.hash);
                    Hash32 { bytes }
                })
                .collect();

            let ctx = LogContext {
                address,
                block_hash,
                block_num: log.block_number,
                timestamp: block_timestamp,
                data: log.data.clone(),
                log_index: log.log_index as u32,
                topics,
                tx_hash,
                tx_index: log.transaction_index as u32,
                removed: log.removed,
            };

            let record = LogRecord::from(ctx);
            builders.append_row(record);
        }

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Convert transactions from a block to RecordBatch using typed-arrow
    fn convert_transactions_to_batch(
        block: &EthBlock,
        senders_bytes: &[u8],
    ) -> Result<RecordBatch> {
        let num_txs = block.body.transactions.len();
        debug!(
            "Processing {} transactions for block #{}",
            num_txs, block.header.number
        );

        // Parse senders (20 bytes each)
        if senders_bytes.len() != num_txs * 20 {
            return Err(anyhow!(
                "Senders array size mismatch: expected {} bytes, got {}",
                num_txs * 20,
                senders_bytes.len()
            ));
        }

        let block_hash = Hash32::from(block.header.hash_slow());
        let timestamp = block.header.timestamp as i64 * 1_000_000_000;

        // Build transaction records
        let mut builders = TransactionRecord::new_builders(num_txs);

        for (idx, tx_env) in block.body.transactions.iter().enumerate() {
            // Extract sender from senders array
            let sender_bytes = &senders_bytes[idx * 20..(idx + 1) * 20];
            let mut sender_array = [0u8; 20];
            sender_array.copy_from_slice(sender_bytes);
            let sender = Address20 { bytes: sender_array };

            // Create transaction context
            let ctx = TransactionContext {
                tx: tx_env,
                block_hash: block_hash.clone(),
                block_num: block.header.number,
                timestamp,
                tx_index: idx as u32,
                sender,
                gas_used: 0,  // Would need receipts
                status: true, // Would need receipts
            };

            // Convert to record and append
            let record = TransactionRecord::from(ctx);
            builders.append_row(record);
        }

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }
}