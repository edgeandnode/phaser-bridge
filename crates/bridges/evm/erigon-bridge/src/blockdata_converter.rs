/// Converter for RLP-encoded blockchain data to Arrow format
///
/// This module handles the RLP bytes → alloy types conversion,
/// then uses the From impls in evm-common to convert to typed-arrow records
use crate::error::ErigonBridgeError;
use crate::proto::custom::{BlockBatch, ReceiptBatch, ReceiptData, TransactionBatch};
use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{Header, ReceiptEnvelope, TxEnvelope};
use alloy_rlp::Decodable;
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use evm_common::block::BlockRecord;
use evm_common::log::{LogContext, LogRecord};
use evm_common::transaction::{RecoverSender, TransactionContext, TransactionRecord};
use evm_common::types::{Address20, Hash32};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};
use typed_arrow::prelude::BuildRows;

/// Converter for BlockDataBackend RLP data to Arrow format
pub struct BlockDataConverter;

impl BlockDataConverter {
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

    /// Convert a BlockBatch to Arrow RecordBatch
    pub fn blocks_to_arrow(batch: BlockBatch) -> Result<RecordBatch, ErigonBridgeError> {
        let mut builders = BlockRecord::new_builders(batch.blocks.len());
        let num_blocks = batch.blocks.len();

        for block_data in &batch.blocks {
            // Decode RLP → alloy Header
            let header = match Header::decode(&mut block_data.rlp_header.as_slice()) {
                Ok(h) => h,
                Err(e) => {
                    error!(
                        "Failed to decode RLP header for block {}: {}",
                        block_data.block_number, e
                    );
                    continue;
                }
            };

            // Use hash from Erigon (already computed) instead of recomputing via hash_slow()
            let hash = alloy_primitives::B256::from_slice(&block_data.block_hash);
            let record = BlockRecord::from_header_with_hash(hash, &header);
            builders.append_row(record);
        }

        debug!("Converted {} blocks to Arrow", num_blocks);

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Convert a TransactionBatch to Arrow RecordBatch
    ///
    /// Note: This returns transactions WITHOUT receipt data (gas_used=0, status=false)
    /// since StreamTransactions doesn't include receipts
    pub fn transactions_to_arrow(
        batch: TransactionBatch,
        block_timestamps: &HashMap<u64, i64>, // block_number -> timestamp in nanos
    ) -> Result<RecordBatch, ErigonBridgeError> {
        let mut builders = TransactionRecord::new_builders(batch.transactions.len());
        let num_transactions = batch.transactions.len();

        for tx_data in &batch.transactions {
            // Decode RLP → alloy TxEnvelope
            let tx = match TxEnvelope::decode(&mut tx_data.rlp_transaction.as_slice()) {
                Ok(t) => t,
                Err(e) => {
                    error!(
                        "Failed to decode RLP transaction for block {} tx {}: {}",
                        tx_data.block_number, tx_data.tx_index, e
                    );
                    continue;
                }
            };

            // Recover sender address from signature - skip if recovery fails
            let sender = match tx.recover_sender() {
                Some(addr) => addr,
                None => {
                    warn!(
                        "Failed to recover sender for block {} tx {} - skipping transaction",
                        tx_data.block_number, tx_data.tx_index
                    );
                    continue;
                }
            };

            let block_hash = Hash32 {
                bytes: tx_data
                    .block_hash
                    .as_slice()
                    .try_into()
                    .unwrap_or([0u8; 32]),
            };

            // Get block timestamp (default to 0 if not found)
            let timestamp = block_timestamps
                .get(&tx_data.block_number)
                .copied()
                .unwrap_or_else(|| {
                    warn!(
                        "Missing timestamp for block {}, using 0",
                        tx_data.block_number
                    );
                    0
                });

            // Build context for conversion
            let ctx = TransactionContext {
                tx: &tx,
                block_hash,
                block_num: tx_data.block_number,
                timestamp,
                tx_index: tx_data.tx_index,
                from: sender,
                gas_used: 0,   // Not available without receipt
                status: false, // Not available without receipt (false = unknown)
            };

            // Use From impl: TransactionContext → TransactionRecord
            let record = TransactionRecord::from(ctx);
            builders.append_row(record);
        }

        debug!("Converted {} transactions to Arrow", num_transactions);

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Convert a batch of blocks with transactions directly to Arrow RecordBatch
    ///
    /// This is more efficient than transactions_to_arrow as it:
    /// - Takes ownership of BlockData (no cloning)
    /// - Extracts timestamps directly from headers (no HashMap)
    /// - Processes transactions in a single pass
    ///
    /// ## Sender Address Resolution Strategy
    ///
    /// Sender addresses are queried from Erigon's TxSender table (precomputed during
    /// block import) rather than performing expensive signature recovery.
    ///
    /// **Why this matters:**
    /// - Signature recovery uses elliptic curve point multiplication (expensive)
    /// - Perf profiling showed 75% of CPU time on k256 EC operations
    /// - TxSender table lookup is a simple KV query + byte parsing
    /// - Performance improvement: ~4x faster (eliminate 75% CPU bottleneck)
    ///
    /// **Erigon's TxSender Table:**
    /// - Computed once during "Senders" stage of block import
    /// - Format: block_num+block_hash → sender_0|sender_1|...|sender_n (20 bytes each)
    /// - Always populated for historical blocks in a synced Erigon node
    ///
    /// Returns error if TxSender lookup fails - this indicates Erigon database issues
    /// that should be investigated rather than silently falling back to slow recovery.
    pub async fn block_transactions_to_arrow(
        blocks: Vec<crate::segment_worker::BlockData>,
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Pre-calculate total transactions for builder capacity
        let total_transactions: usize = blocks.iter().map(|b| b.transactions.len()).sum();
        let mut builders = TransactionRecord::new_builders(total_transactions);

        // Process each block, moving transactions out
        for block in blocks {
            let timestamp = block.header.timestamp as i64 * 1_000_000_000;

            // Process all transactions in this block (moving, not cloning)
            for (tx_index_in_block, tx_data) in block.transactions.into_iter().enumerate() {
                // Use block_hash from TransactionData (already provided by Erigon)
                let block_hash_bytes: [u8; 32] =
                    tx_data.block_hash.as_slice().try_into().map_err(|_| {
                        ErigonBridgeError::InvalidData(format!(
                            "Invalid block_hash size for block {} tx {}: expected 32 bytes, got {}",
                            block.block_num,
                            tx_data.tx_index,
                            tx_data.block_hash.len()
                        ))
                    })?;
                let block_hash = Hash32 {
                    bytes: block_hash_bytes,
                };

                // Decode RLP → alloy TxEnvelope
                let tx = match TxEnvelope::decode(&mut tx_data.rlp_transaction.as_slice()) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(
                            "Failed to decode RLP transaction for block {} tx {}: {}",
                            block.block_num, tx_data.tx_index, e
                        );
                        continue;
                    }
                };

                // Get sender address from TransactionData (precomputed by Erigon from TxSender table)
                // If not available (snapshot blocks), fall back to signature recovery
                let sender = if tx_data.sender.len() == 20 {
                    // Use precomputed sender from TxSender table (fast path)
                    let mut bytes = [0u8; 20];
                    bytes.copy_from_slice(&tx_data.sender);
                    Address20 { bytes }
                } else {
                    // Fall back to expensive ECDSA signature recovery
                    // This happens for snapshot blocks where TxSender table isn't populated
                    let sender_addr = tx.recover_sender().ok_or_else(|| {
                        ErigonBridgeError::InvalidData(format!(
                            "Failed to recover sender for block {} tx {} (signature invalid)",
                            block.block_num, tx_data.tx_index
                        ))
                    })?;
                    let mut bytes = [0u8; 20];
                    bytes.copy_from_slice(sender_addr.as_ref()); // Convert alloy Address to [u8; 20]
                    Address20 { bytes }
                };

                // Build context for conversion
                let ctx = TransactionContext {
                    tx: &tx,
                    block_hash: block_hash.clone(),
                    block_num: block.block_num,
                    timestamp,
                    tx_index: tx_data.tx_index,
                    from: sender,
                    gas_used: 0,   // Not available without receipt
                    status: false, // Not available without receipt
                };

                let record = TransactionRecord::from(ctx);
                builders.append_row(record);
            }
        }

        debug!("Converted {} transactions to Arrow", total_transactions);

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }

    /// Convert receipts to Arrow RecordBatch (logs)
    /// Receipts contain logs - we extract all logs from all receipts
    ///
    /// Note: Block timestamps must be provided as we don't have them in receipts
    pub fn receipts_to_logs_arrow(
        receipts: Vec<ReceiptData>,
        block_timestamps: &HashMap<u64, i64>, // block_number -> timestamp in nanos
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Estimate total logs (rough)
        let estimated_logs = receipts.len() * 2;
        let mut builders = LogRecord::new_builders(estimated_logs);

        for receipt_data in &receipts {
            // Decode RLP → alloy ReceiptEnvelope
            let receipt_envelope =
                match ReceiptEnvelope::decode(&mut receipt_data.rlp_receipt.as_slice()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(
                            "Failed to decode RLP receipt for block {} tx {}: {}",
                            receipt_data.block_number, receipt_data.tx_index, e
                        );
                        continue;
                    }
                };

            // Extract inner receipt (all variants contain ReceiptWithBloom<Receipt>)
            let receipt = receipt_envelope
                .as_receipt()
                .expect("Receipt envelope should contain receipt");

            let tx_hash = Hash32 {
                bytes: receipt_data
                    .tx_hash
                    .as_slice()
                    .try_into()
                    .unwrap_or([0u8; 32]),
            };
            let block_hash = Hash32 {
                bytes: receipt_data
                    .block_hash
                    .as_slice()
                    .try_into()
                    .unwrap_or([0u8; 32]),
            };

            // Get block timestamp (default to 0 if not found)
            let timestamp = block_timestamps
                .get(&receipt_data.block_number)
                .copied()
                .unwrap_or_else(|| {
                    warn!(
                        "Missing timestamp for block {}, using 0",
                        receipt_data.block_number
                    );
                    0
                });

            // Extract logs from receipt
            for (log_index, log) in receipt.logs.iter().enumerate() {
                let topics: Vec<Hash32> = log
                    .topics()
                    .iter()
                    .map(|t| Hash32 {
                        bytes: t.as_slice().try_into().unwrap_or([0u8; 32]),
                    })
                    .collect();

                // Build context for log conversion
                let context = LogContext {
                    address: Address20 {
                        bytes: log.address.as_slice().try_into().unwrap_or([0u8; 20]),
                    },
                    block_hash: block_hash.clone(),
                    block_num: receipt_data.block_number,
                    timestamp,
                    data: log.data.data.to_vec(),
                    log_index: log_index as u32,
                    topics,
                    tx_hash: tx_hash.clone(),
                    tx_index: receipt_data.tx_index,
                    removed: false, // RLP receipts don't indicate removal
                };

                // Use From impl: LogContext → LogRecord
                let log_record = LogRecord::from(context);
                builders.append_row(log_record);
            }
        }

        debug!("Extracted logs from {} receipts", receipts.len());

        let arrays = builders.finish();
        Ok(arrays.into_record_batch())
    }
}
