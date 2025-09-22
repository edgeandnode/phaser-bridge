use crate::proto::remote::{BlockReply, SubscribeReply};
use alloy_consensus::{Block, Header, TxEnvelope};
use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::{
    BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int32Array, RecordBatch,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
type EthBlock = Block<TxEnvelope>;
use alloy_primitives::{Address, B256, U256};
use alloy_rlp::Decodable;
use std::sync::Arc;

/// Converter for Erigon data to Arrow format
pub struct ErigonDataConverter;

impl ErigonDataConverter {
    /// Get the Arrow schema for blocks
    pub fn block_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
            Field::new("hash", DataType::FixedSizeBinary(32), false),
            Field::new("parent_hash", DataType::FixedSizeBinary(32), false),
            Field::new("ommers_hash", DataType::FixedSizeBinary(32), false),
            Field::new("miner", DataType::FixedSizeBinary(20), false),
            Field::new("state_root", DataType::FixedSizeBinary(32), false),
            Field::new("transactions_root", DataType::FixedSizeBinary(32), false),
            Field::new("receipts_root", DataType::FixedSizeBinary(32), false),
            Field::new("logs_bloom", DataType::Binary, false),
            Field::new("difficulty", DataType::Decimal128(38, 0), false),
            Field::new("gas_limit", DataType::UInt64, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("extra_data", DataType::Binary, false),
            Field::new("mix_hash", DataType::FixedSizeBinary(32), false),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("base_fee_per_gas", DataType::Decimal128(38, 0), true),
            Field::new("withdrawals_root", DataType::FixedSizeBinary(32), true),
            Field::new("blob_gas_used", DataType::UInt64, true),
            Field::new("excess_blob_gas", DataType::UInt64, true),
            Field::new("parent_beacon_root", DataType::FixedSizeBinary(32), true),
        ]))
    }

    /// Get the Arrow schema for transactions
    pub fn transaction_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
            Field::new("tx_index", DataType::UInt32, false),
            Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
            Field::new("from", DataType::FixedSizeBinary(20), false),
            Field::new("to", DataType::FixedSizeBinary(20), true),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("gas_price", DataType::Decimal128(38, 0), true),
            Field::new("gas_limit", DataType::UInt64, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("value", DataType::Decimal128(38, 0), false),
            Field::new("input", DataType::Binary, false),
            Field::new("v", DataType::Binary, false),
            Field::new("r", DataType::Binary, false),
            Field::new("s", DataType::Binary, false),
            Field::new("type", DataType::Int32, false),
            Field::new("status", DataType::Boolean, false),
            Field::new("max_fee_per_gas", DataType::Decimal128(38, 0), true),
            Field::new(
                "max_priority_fee_per_gas",
                DataType::Decimal128(38, 0),
                true,
            ),
            Field::new("max_fee_per_blob_gas", DataType::Decimal128(38, 0), true),
        ]))
    }

    /// Get the Arrow schema for logs
    pub fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                false,
            ),
            Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
            Field::new("tx_index", DataType::UInt32, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("address", DataType::FixedSizeBinary(20), false),
            Field::new("topic0", DataType::FixedSizeBinary(32), true),
            Field::new("topic1", DataType::FixedSizeBinary(32), true),
            Field::new("topic2", DataType::FixedSizeBinary(32), true),
            Field::new("topic3", DataType::FixedSizeBinary(32), true),
            Field::new("data", DataType::Binary, false),
        ]))
    }
    /// Convert SubscribeReply containing block header to RecordBatch
    pub fn convert_subscribe_reply(reply: &SubscribeReply) -> Result<RecordBatch> {
        // For HEADER events (type 0), the data field contains raw RLP-encoded block header
        // Not wrapped in BlockReply protobuf

        if reply.data.is_empty() {
            return Err(anyhow!("No data in reply"));
        }

        // Decode the RLP-encoded block header using Alloy
        let header = Header::decode(&mut &reply.data[..])?;

        // Extract fields from the decoded header
        let block_num = header.number;
        let timestamp = header.timestamp;
        let hash = header.hash_slow();

        println!(
            "DEBUG: Successfully decoded block #{} at timestamp {} with hash 0x{}",
            block_num,
            timestamp,
            hex::encode(hash.as_slice())
        );

        // Create Arrow arrays from the header fields
        let schema = Self::block_schema();
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> =
            Vec::with_capacity(schema.fields().len());

        // Field 0: _block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 1: block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 2: timestamp
        let timestamp_nanos = timestamp as i64 * 1_000_000_000;
        arrays.push(Arc::new(
            TimestampNanosecondArray::from(vec![timestamp_nanos]).with_timezone("+00:00"),
        ));

        // Field 3: hash (32 bytes)
        let hash_bytes: &[u8] = hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![hash_bytes].into_iter(),
        )?));

        // Field 4: parent_hash (32 bytes)
        let parent_hash: &[u8] = header.parent_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![parent_hash].into_iter(),
        )?));

        // Field 5: ommers_hash (32 bytes)
        let ommers_hash: &[u8] = header.ommers_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![ommers_hash].into_iter(),
        )?));

        // Field 6: miner/beneficiary (20 bytes)
        let miner: &[u8] = header.beneficiary.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![miner].into_iter(),
        )?));

        // Field 7: state_root (32 bytes)
        let state_root: &[u8] = header.state_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![state_root].into_iter(),
        )?));

        // Field 8: transactions_root (32 bytes)
        let transactions_root: &[u8] = header.transactions_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![transactions_root].into_iter(),
        )?));

        // Field 9: receipts_root (32 bytes)
        let receipts_root: &[u8] = header.receipts_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![receipts_root].into_iter(),
        )?));

        // Field 10: logs_bloom (256 bytes)
        let bloom_bytes: Vec<u8> = header.logs_bloom.0.to_vec();
        arrays.push(Arc::new(BinaryArray::from(vec![Some(
            bloom_bytes.as_slice(),
        )])));

        // Field 11: difficulty (as Decimal128 with precision 38, scale 0)
        // Convert U256 difficulty to i128 (may overflow for very large values)
        let difficulty = header.difficulty.to::<u128>() as i128;
        arrays.push(Arc::new(
            Decimal128Array::from(vec![Some(difficulty)]).with_precision_and_scale(38, 0)?,
        ));

        // Field 12: gas_limit
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_limit])));

        // Field 13: gas_used
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_used])));

        // Field 14: extra_data
        arrays.push(Arc::new(BinaryArray::from(vec![Some(
            header.extra_data.as_ref(),
        )])));

        // Field 15: mix_hash (32 bytes)
        let mix_hash: &[u8] = header.mix_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![mix_hash].into_iter(),
        )?));

        // Field 16: nonce
        arrays.push(Arc::new(UInt64Array::from(vec![u64::from_be_bytes(
            header.nonce.0,
        )])));

        // Field 17: base_fee_per_gas (optional, as Decimal128 with precision 38, scale 0)
        let base_fee = header.base_fee_per_gas.map(|fee| fee as u128 as i128);
        arrays.push(Arc::new(
            Decimal128Array::from(vec![base_fee]).with_precision_and_scale(38, 0)?,
        ));

        // Field 18: withdrawals_root (optional, 32 bytes)
        if let Some(ref root) = header.withdrawals_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
                vec![root_bytes].into_iter(),
            )?));
        } else {
            arrays.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![None::<&[u8]>].into_iter(),
                    32,
                )?,
            ));
        }

        // Field 19: blob_gas_used (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.blob_gas_used])));

        // Field 20: excess_blob_gas (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.excess_blob_gas])));

        // Field 21: parent_beacon_root (optional, 32 bytes)
        if let Some(ref root) = header.parent_beacon_block_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
                vec![root_bytes].into_iter(),
            )?));
        } else {
            arrays.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![None::<&[u8]>].into_iter(),
                    32,
                )?,
            ));
        }

        let batch = RecordBatch::try_new(schema, arrays)?;

        println!(
            "Created RecordBatch with {} rows, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        Ok(batch)
    }

    /// Convert a full block from Erigon to RecordBatches (header + transactions)
    pub fn convert_full_block(
        block_reply: &BlockReply,
    ) -> Result<(RecordBatch, Option<RecordBatch>)> {
        println!(
            "DEBUG convert_full_block: Starting conversion - RLP size: {} bytes, senders: {} bytes",
            block_reply.block_rlp.len(),
            block_reply.senders.len()
        );

        // Decode the full block from RLP
        let block = match EthBlock::decode(&mut &block_reply.block_rlp[..]) {
            Ok(b) => {
                println!(
                    "DEBUG: Successfully decoded block #{} with {} transactions",
                    b.header.number,
                    b.body.transactions.len()
                );
                b
            }
            Err(e) => {
                eprintln!(
                    "ERROR: Failed to decode RLP block (size: {} bytes): {}",
                    block_reply.block_rlp.len(),
                    e
                );
                eprintln!(
                    "ERROR: First 100 bytes of RLP: {:?}",
                    &block_reply.block_rlp[..block_reply.block_rlp.len().min(100)]
                );
                return Err(e.into());
            }
        };

        // Convert header to RecordBatch
        println!(
            "DEBUG: Converting header for block #{}",
            block.header.number
        );
        let header_batch = Self::convert_header_to_batch(&block.header)?;
        println!(
            "DEBUG: Header batch created with {} rows, {} columns",
            header_batch.num_rows(),
            header_batch.num_columns()
        );

        // If there are transactions, convert them
        let tx_batch = if !block.body.transactions.is_empty() {
            println!(
                "DEBUG: Converting {} transactions for block #{}",
                block.body.transactions.len(),
                block.header.number
            );
            let batch = Self::convert_transactions_to_batch(&block, &block_reply.senders)?;
            println!(
                "DEBUG: Transaction batch created with {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            Some(batch)
        } else {
            println!("DEBUG: Block #{} has no transactions", block.header.number);
            None
        };

        println!(
            "DEBUG: Successfully converted full block #{} - header: ✓, transactions: {}",
            block.header.number,
            if tx_batch.is_some() { "✓" } else { "none" }
        );

        Ok((header_batch, tx_batch))
    }

    /// Convert a header to RecordBatch (extracted from convert_subscribe_reply for reuse)
    fn convert_header_to_batch(header: &Header) -> Result<RecordBatch> {
        let block_num = header.number;
        let timestamp = header.timestamp;
        let hash = header.hash_slow();

        println!(
            "DEBUG: Processing block #{} at timestamp {} with hash 0x{}",
            block_num,
            timestamp,
            hex::encode(hash.as_slice())
        );

        // Create Arrow arrays from the header fields
        let schema = Self::block_schema();
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> =
            Vec::with_capacity(schema.fields().len());

        // Field 0: _block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 1: block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 2: timestamp
        let timestamp_nanos = timestamp as i64 * 1_000_000_000;
        arrays.push(Arc::new(
            TimestampNanosecondArray::from(vec![timestamp_nanos]).with_timezone("+00:00"),
        ));

        // Field 3: hash (32 bytes)
        let hash_bytes: &[u8] = hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![hash_bytes].into_iter(),
        )?));

        // Field 4: parent_hash (32 bytes)
        let parent_hash: &[u8] = header.parent_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![parent_hash].into_iter(),
        )?));

        // Field 5: ommers_hash (32 bytes)
        let ommers_hash: &[u8] = header.ommers_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![ommers_hash].into_iter(),
        )?));

        // Field 6: miner/beneficiary (20 bytes)
        let miner: &[u8] = header.beneficiary.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![miner].into_iter(),
        )?));

        // Field 7: state_root (32 bytes)
        let state_root: &[u8] = header.state_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![state_root].into_iter(),
        )?));

        // Field 8: transactions_root (32 bytes)
        let transactions_root: &[u8] = header.transactions_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![transactions_root].into_iter(),
        )?));

        // Field 9: receipts_root (32 bytes)
        let receipts_root: &[u8] = header.receipts_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![receipts_root].into_iter(),
        )?));

        // Field 10: logs_bloom (256 bytes)
        let bloom_bytes: Vec<u8> = header.logs_bloom.0.to_vec();
        arrays.push(Arc::new(BinaryArray::from(vec![Some(
            bloom_bytes.as_slice(),
        )])));

        // Field 11: difficulty (as Decimal128 with precision 38, scale 0)
        let difficulty = header.difficulty.to::<u128>() as i128;
        arrays.push(Arc::new(
            Decimal128Array::from(vec![Some(difficulty)]).with_precision_and_scale(38, 0)?,
        ));

        // Field 12: gas_limit
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_limit])));

        // Field 13: gas_used
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_used])));

        // Field 14: extra_data
        arrays.push(Arc::new(BinaryArray::from(vec![Some(
            header.extra_data.as_ref(),
        )])));

        // Field 15: mix_hash (32 bytes)
        let mix_hash: &[u8] = header.mix_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            vec![mix_hash].into_iter(),
        )?));

        // Field 16: nonce
        arrays.push(Arc::new(UInt64Array::from(vec![u64::from_be_bytes(
            header.nonce.0,
        )])));

        // Field 17: base_fee_per_gas (optional)
        let base_fee = header.base_fee_per_gas.map(|fee| fee as u128 as i128);
        arrays.push(Arc::new(
            Decimal128Array::from(vec![base_fee]).with_precision_and_scale(38, 0)?,
        ));

        // Field 18: withdrawals_root (optional, 32 bytes)
        if let Some(ref root) = header.withdrawals_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
                vec![root_bytes].into_iter(),
            )?));
        } else {
            arrays.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![None::<&[u8]>].into_iter(),
                    32,
                )?,
            ));
        }

        // Field 19: blob_gas_used (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.blob_gas_used])));

        // Field 20: excess_blob_gas (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.excess_blob_gas])));

        // Field 21: parent_beacon_root (optional, 32 bytes)
        if let Some(ref root) = header.parent_beacon_block_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
                vec![root_bytes].into_iter(),
            )?));
        } else {
            arrays.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![None::<&[u8]>].into_iter(),
                    32,
                )?,
            ));
        }

        let batch = RecordBatch::try_new(schema, arrays)?;
        Ok(batch)
    }

    /// Convert transactions from a block to RecordBatch
    fn convert_transactions_to_batch(
        block: &EthBlock,
        senders_bytes: &[u8],
    ) -> Result<RecordBatch> {
        let num_txs = block.body.transactions.len();
        println!(
            "DEBUG convert_transactions_to_batch: Processing {} transactions for block #{}",
            num_txs, block.header.number
        );

        // Parse senders (20 bytes each)
        if senders_bytes.len() != num_txs * 20 {
            eprintln!(
                "ERROR: Senders array size mismatch: expected {} bytes, got {}",
                num_txs * 20,
                senders_bytes.len()
            );
            return Err(anyhow!(
                "Senders array size mismatch: expected {} bytes, got {}",
                num_txs * 20,
                senders_bytes.len()
            ));
        }

        let block_num = block.header.number;
        let block_hash = block.header.hash_slow();
        let timestamp = block.header.timestamp;

        println!(
            "DEBUG: Block info - num: {}, hash: 0x{}, timestamp: {}",
            block_num,
            hex::encode(block_hash.as_slice()),
            timestamp
        );

        // Prepare arrays for all fields
        let mut block_nums = Vec::with_capacity(num_txs);
        let mut block_hashes: Vec<&[u8]> = Vec::with_capacity(num_txs);
        let mut block_nums2 = Vec::with_capacity(num_txs);
        let mut timestamps = Vec::with_capacity(num_txs);
        let mut tx_indices = Vec::with_capacity(num_txs);
        let mut tx_hashes: Vec<&[u8]> = Vec::with_capacity(num_txs);
        let mut from_addrs: Vec<&[u8]> = Vec::with_capacity(num_txs);
        let mut to_addrs: Vec<Option<&[u8]>> = Vec::with_capacity(num_txs);
        let mut nonces = Vec::with_capacity(num_txs);
        let mut gas_prices = Vec::with_capacity(num_txs);
        let mut gas_limits = Vec::with_capacity(num_txs);
        let mut gas_used = Vec::with_capacity(num_txs); // Will be 0 for now, needs receipts
        let mut values = Vec::with_capacity(num_txs);
        let mut inputs = Vec::with_capacity(num_txs);
        let mut v_values = Vec::with_capacity(num_txs);
        let mut r_values = Vec::with_capacity(num_txs);
        let mut s_values = Vec::with_capacity(num_txs);
        let mut tx_types = Vec::with_capacity(num_txs);
        let mut statuses = Vec::with_capacity(num_txs); // Will be true for now, needs receipts
        let mut max_fees = Vec::with_capacity(num_txs);
        let mut max_priority_fees = Vec::with_capacity(num_txs);
        let mut max_blob_fees = Vec::with_capacity(num_txs);

        for (idx, tx_env) in block.body.transactions.iter().enumerate() {
            // Extract sender from senders array
            let sender_bytes = &senders_bytes[idx * 20..(idx + 1) * 20];

            // Common fields for all transactions
            block_nums.push(block_num);
            block_hashes.push(block_hash.as_ref());
            block_nums2.push(block_num);
            timestamps.push(timestamp as i64 * 1_000_000_000);
            tx_indices.push(idx as u32);

            // Get transaction hash
            let tx_hash = tx_env.tx_hash();
            tx_hashes.push(tx_hash.as_ref());

            from_addrs.push(sender_bytes);

            // Debug transaction type
            let tx_type_name = match tx_env {
                TxEnvelope::Legacy(_) => "Legacy",
                TxEnvelope::Eip2930(_) => "EIP-2930",
                TxEnvelope::Eip1559(_) => "EIP-1559",
                TxEnvelope::Eip4844(_) => "EIP-4844",
                TxEnvelope::Eip7702(_) => "EIP-7702",
            };
            println!(
                "DEBUG: TX[{}] type: {}, hash: 0x{}, sender: 0x{}",
                idx,
                tx_type_name,
                hex::encode(tx_hash.as_slice()),
                hex::encode(sender_bytes)
            );

            // Process based on transaction type
            match tx_env {
                TxEnvelope::Legacy(tx) => {
                    let tx_inner = tx.tx();
                    to_addrs.push(tx_inner.to.to().map(|a| a.as_ref()));
                    nonces.push(tx_inner.nonce);
                    gas_prices.push(Some(tx_inner.gas_price as u128 as i128));
                    gas_limits.push(tx_inner.gas_limit);
                    values.push(tx_inner.value.to::<u128>() as i128);
                    inputs.push(Some(tx_inner.input.as_ref()));

                    // Signature fields
                    let sig = tx.signature();
                    v_values.push(Some(vec![if sig.v() { 1u8 } else { 0u8 }]));
                    r_values.push(Some(sig.r().to_be_bytes::<32>().to_vec()));
                    s_values.push(Some(sig.s().to_be_bytes::<32>().to_vec()));

                    tx_types.push(0); // Legacy = 0
                    max_fees.push(None);
                    max_priority_fees.push(None);
                    max_blob_fees.push(None);
                }
                TxEnvelope::Eip2930(tx) => {
                    let tx_inner = tx.tx();
                    to_addrs.push(tx_inner.to.to().map(|a| a.as_ref()));
                    nonces.push(tx_inner.nonce);
                    gas_prices.push(Some(tx_inner.gas_price as u128 as i128));
                    gas_limits.push(tx_inner.gas_limit);
                    values.push(tx_inner.value.to::<u128>() as i128);
                    inputs.push(Some(tx_inner.input.as_ref()));

                    // Signature fields
                    let sig = tx.signature();
                    v_values.push(Some(vec![if sig.v() { 1u8 } else { 0u8 }]));
                    r_values.push(Some(sig.r().to_be_bytes::<32>().to_vec()));
                    s_values.push(Some(sig.s().to_be_bytes::<32>().to_vec()));

                    tx_types.push(1); // EIP-2930 = 1
                    max_fees.push(None);
                    max_priority_fees.push(None);
                    max_blob_fees.push(None);
                }
                TxEnvelope::Eip1559(tx) => {
                    let tx_inner = tx.tx();
                    to_addrs.push(tx_inner.to.to().map(|a| a.as_ref()));
                    nonces.push(tx_inner.nonce);
                    gas_prices.push(None); // EIP-1559 doesn't have gas_price
                    gas_limits.push(tx_inner.gas_limit);
                    values.push(tx_inner.value.to::<u128>() as i128);
                    inputs.push(Some(tx_inner.input.as_ref()));

                    // Signature fields
                    let sig = tx.signature();
                    v_values.push(Some(vec![if sig.v() { 1u8 } else { 0u8 }]));
                    r_values.push(Some(sig.r().to_be_bytes::<32>().to_vec()));
                    s_values.push(Some(sig.s().to_be_bytes::<32>().to_vec()));

                    tx_types.push(2); // EIP-1559 = 2
                    max_fees.push(Some(tx_inner.max_fee_per_gas as u128 as i128));
                    max_priority_fees.push(Some(tx_inner.max_priority_fee_per_gas as u128 as i128));
                    max_blob_fees.push(None);
                }
                TxEnvelope::Eip4844(tx) => {
                    // Extract the inner TxEip4844 from the variant
                    let tx_inner = match tx.tx() {
                        alloy_consensus::TxEip4844Variant::TxEip4844(inner) => inner,
                        alloy_consensus::TxEip4844Variant::TxEip4844WithSidecar(with_sidecar) => {
                            &with_sidecar.tx
                        }
                    };

                    to_addrs.push(Some(tx_inner.to.as_ref()));
                    nonces.push(tx_inner.nonce);
                    gas_prices.push(None);
                    gas_limits.push(tx_inner.gas_limit);
                    values.push(tx_inner.value.to::<u128>() as i128);
                    inputs.push(Some(tx_inner.input.as_ref()));

                    // Signature fields
                    let sig = tx.signature();
                    v_values.push(Some(vec![if sig.v() { 1u8 } else { 0u8 }]));
                    r_values.push(Some(sig.r().to_be_bytes::<32>().to_vec()));
                    s_values.push(Some(sig.s().to_be_bytes::<32>().to_vec()));

                    tx_types.push(3); // EIP-4844 = 3
                    max_fees.push(Some(tx_inner.max_fee_per_gas as u128 as i128));
                    max_priority_fees.push(Some(tx_inner.max_priority_fee_per_gas as u128 as i128));
                    max_blob_fees.push(Some(tx_inner.max_fee_per_blob_gas as u128 as i128));
                }
                TxEnvelope::Eip7702(tx) => {
                    let tx_inner = tx.tx();
                    to_addrs.push(Some(tx_inner.to.as_ref()));
                    nonces.push(tx_inner.nonce);
                    gas_prices.push(None);
                    gas_limits.push(tx_inner.gas_limit);
                    values.push(tx_inner.value.to::<u128>() as i128);
                    inputs.push(Some(tx_inner.input.as_ref()));

                    // Signature fields
                    let sig = tx.signature();
                    v_values.push(Some(vec![if sig.v() { 1u8 } else { 0u8 }]));
                    r_values.push(Some(sig.r().to_be_bytes::<32>().to_vec()));
                    s_values.push(Some(sig.s().to_be_bytes::<32>().to_vec()));

                    tx_types.push(4); // EIP-7702 = 4
                    max_fees.push(Some(tx_inner.max_fee_per_gas as u128 as i128));
                    max_priority_fees.push(Some(tx_inner.max_priority_fee_per_gas as u128 as i128));
                    max_blob_fees.push(None);
                }
            }

            // Default values for fields we don't have yet
            gas_used.push(0); // Would need receipts
            statuses.push(true); // Would need receipts
        }

        // Build the RecordBatch
        let schema = Self::transaction_schema();
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> =
            Vec::with_capacity(schema.fields().len());

        // Field 0: _block_num
        arrays.push(Arc::new(UInt64Array::from(block_nums.clone())));

        // Field 1: block_hash
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            block_hashes.into_iter(),
        )?));

        // Field 2: block_num
        arrays.push(Arc::new(UInt64Array::from(block_nums2)));

        // Field 3: timestamp
        arrays.push(Arc::new(
            TimestampNanosecondArray::from(timestamps).with_timezone("+00:00"),
        ));

        // Field 4: tx_index
        arrays.push(Arc::new(UInt32Array::from(tx_indices)));

        // Field 5: tx_hash
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            tx_hashes.into_iter(),
        )?));

        // Field 6: from
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
            from_addrs.into_iter(),
        )?));

        // Field 7: to (nullable)
        arrays.push(Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(to_addrs.into_iter(), 20)?,
        ));

        // Field 8: nonce
        arrays.push(Arc::new(UInt64Array::from(nonces)));

        // Field 9: gas_price (nullable)
        arrays.push(Arc::new(
            Decimal128Array::from(gas_prices).with_precision_and_scale(38, 0)?,
        ));

        // Field 10: gas_limit
        arrays.push(Arc::new(UInt64Array::from(gas_limits)));

        // Field 11: gas_used
        arrays.push(Arc::new(UInt64Array::from(gas_used)));

        // Field 12: value
        arrays.push(Arc::new(
            Decimal128Array::from(values.into_iter().map(Some).collect::<Vec<_>>())
                .with_precision_and_scale(38, 0)?,
        ));

        // Field 13: input
        arrays.push(Arc::new(BinaryArray::from_iter(inputs)));

        // Field 14: v
        arrays.push(Arc::new(BinaryArray::from_iter(v_values)));

        // Field 15: r
        arrays.push(Arc::new(BinaryArray::from_iter(r_values)));

        // Field 16: s
        arrays.push(Arc::new(BinaryArray::from_iter(s_values)));

        // Field 17: type
        arrays.push(Arc::new(Int32Array::from(tx_types)));

        // Field 18: status
        arrays.push(Arc::new(BooleanArray::from(statuses)));

        // Field 19: max_fee_per_gas (nullable)
        arrays.push(Arc::new(
            Decimal128Array::from(max_fees).with_precision_and_scale(38, 0)?,
        ));

        // Field 20: max_priority_fee_per_gas (nullable)
        arrays.push(Arc::new(
            Decimal128Array::from(max_priority_fees).with_precision_and_scale(38, 0)?,
        ));

        // Field 21: max_fee_per_blob_gas (nullable)
        arrays.push(Arc::new(
            Decimal128Array::from(max_blob_fees).with_precision_and_scale(38, 0)?,
        ));

        let batch = RecordBatch::try_new(schema, arrays)?;

        println!(
            "Created transaction RecordBatch with {} rows",
            batch.num_rows()
        );

        Ok(batch)
    }
}
