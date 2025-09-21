use arrow_array::{RecordBatch, UInt64Array, TimestampNanosecondArray, FixedSizeBinaryArray, BinaryArray, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use crate::proto::remote::SubscribeReply;
use anyhow::{Result, anyhow};
use alloy_consensus::Header;
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
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())), false),
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
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())), false),
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
            Field::new("max_priority_fee_per_gas", DataType::Decimal128(38, 0), true),
            Field::new("max_fee_per_blob_gas", DataType::Decimal128(38, 0), true),
        ]))
    }

    /// Get the Arrow schema for logs
    pub fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new("block_num", DataType::UInt64, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())), false),
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

        println!("DEBUG: Successfully decoded block #{} at timestamp {} with hash 0x{}",
                 block_num, timestamp, hex::encode(hash.0));

        // Create Arrow arrays from the header fields
        let schema = Self::block_schema();
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

        // Field 0: _block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 1: block_num
        arrays.push(Arc::new(UInt64Array::from(vec![block_num])));

        // Field 2: timestamp
        let timestamp_nanos = timestamp as i64 * 1_000_000_000;
        arrays.push(Arc::new(TimestampNanosecondArray::from(vec![timestamp_nanos]).with_timezone("+00:00")));

        // Field 3: hash (32 bytes)
        let hash_bytes: &[u8] = hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![hash_bytes].into_iter())?));

        // Field 4: parent_hash (32 bytes)
        let parent_hash: &[u8] = header.parent_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![parent_hash].into_iter())?));

        // Field 5: ommers_hash (32 bytes)
        let ommers_hash: &[u8] = header.ommers_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![ommers_hash].into_iter())?));

        // Field 6: miner/beneficiary (20 bytes)
        let miner: &[u8] = header.beneficiary.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![miner].into_iter())?));

        // Field 7: state_root (32 bytes)
        let state_root: &[u8] = header.state_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![state_root].into_iter())?));

        // Field 8: transactions_root (32 bytes)
        let transactions_root: &[u8] = header.transactions_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![transactions_root].into_iter())?));

        // Field 9: receipts_root (32 bytes)
        let receipts_root: &[u8] = header.receipts_root.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![receipts_root].into_iter())?));

        // Field 10: logs_bloom (256 bytes)
        let bloom_bytes: Vec<u8> = header.logs_bloom.0.to_vec();
        arrays.push(Arc::new(BinaryArray::from(vec![Some(bloom_bytes.as_slice())])));

        // Field 11: difficulty (as Decimal128 with precision 38, scale 0)
        // Convert U256 difficulty to i128 (may overflow for very large values)
        let difficulty = header.difficulty.to::<u128>() as i128;
        arrays.push(Arc::new(Decimal128Array::from(vec![Some(difficulty)])
            .with_precision_and_scale(38, 0)?));

        // Field 12: gas_limit
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_limit])));

        // Field 13: gas_used
        arrays.push(Arc::new(UInt64Array::from(vec![header.gas_used])));

        // Field 14: extra_data
        arrays.push(Arc::new(BinaryArray::from(vec![Some(header.extra_data.as_ref())])));

        // Field 15: mix_hash (32 bytes)
        let mix_hash: &[u8] = header.mix_hash.as_ref();
        arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![mix_hash].into_iter())?));

        // Field 16: nonce
        arrays.push(Arc::new(UInt64Array::from(vec![u64::from_be_bytes(header.nonce.0)])));

        // Field 17: base_fee_per_gas (optional, as Decimal128 with precision 38, scale 0)
        let base_fee = header.base_fee_per_gas.map(|fee| fee as u128 as i128);
        arrays.push(Arc::new(Decimal128Array::from(vec![base_fee])
            .with_precision_and_scale(38, 0)?));

        // Field 18: withdrawals_root (optional, 32 bytes)
        if let Some(ref root) = header.withdrawals_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![root_bytes].into_iter())?));
        } else {
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![None::<&[u8]>].into_iter(), 32
            )?));
        }

        // Field 19: blob_gas_used (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.blob_gas_used])));

        // Field 20: excess_blob_gas (optional)
        arrays.push(Arc::new(UInt64Array::from(vec![header.excess_blob_gas])));

        // Field 21: parent_beacon_root (optional, 32 bytes)
        if let Some(ref root) = header.parent_beacon_block_root {
            let root_bytes: &[u8] = root.as_ref();
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_iter(vec![root_bytes].into_iter())?));
        } else {
            arrays.push(Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![None::<&[u8]>].into_iter(), 32
            )?));
        }

        let batch = RecordBatch::try_new(schema, arrays)?;

        println!("Created RecordBatch with {} rows, {} columns",
                 batch.num_rows(), batch.num_columns());

        Ok(batch)
    }
}