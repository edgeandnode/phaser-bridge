/// EVM-specific indexing implementation
///
/// This crate provides IndexableSchema implementations for EVM transaction data,
/// using typed-arrow's zero-copy Record views for efficient key extraction.
use arrow::array::{Array, AsArray};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use parquet_index_schema::{
    CompositeKeyExtractor, IndexSpec, IndexableSchema, KeyExtractor, ZeroCopyKeyExtractor,
};
use std::sync::Arc;

/// Column indices in TransactionRecord
/// Based on field order in evm_common::transaction::TransactionRecord
mod columns {
    pub const BLOCK_NUM: usize = 2; // block_num field
    pub const TX_HASH: usize = 5; // tx_hash field
    pub const FROM: usize = 6; // from field
    pub const TO: usize = 7; // to field (Option<Address20>)
}

/// Column family names for EVM transaction indexes
///
/// These are the RocksDB column family names used by the EVM transaction indexer.
/// Use these constants when querying indexes to ensure consistency.
pub const CF_TX_BY_HASH: &str = "tx_by_hash";
pub const CF_TX_BY_FROM: &str = "tx_by_from";
pub const CF_TX_BY_TO: &str = "tx_by_to";

/// IndexableSchema implementation for EVM transactions
pub struct EvmTransactionIndexer;

impl IndexableSchema for EvmTransactionIndexer {
    type Record = evm_common::transaction::TransactionRecord;

    fn schema_name() -> &'static str {
        "evm_transactions"
    }

    fn index_specs() -> Vec<IndexSpec> {
        vec![
            IndexSpec {
                column_family: CF_TX_BY_HASH.to_string(),
                key_extractor: Arc::new(TxHashExtractor),
                column_index: columns::TX_HASH,
            },
            IndexSpec {
                column_family: CF_TX_BY_FROM.to_string(),
                key_extractor: Arc::new(FromAddressExtractor),
                column_index: columns::FROM,
            },
            IndexSpec {
                column_family: CF_TX_BY_TO.to_string(),
                key_extractor: Arc::new(ToAddressExtractor),
                column_index: columns::TO,
            },
        ]
    }
}

/// Zero-copy extractor for transaction hash
///
/// Returns direct reference to the 32-byte hash in the Arrow array
struct TxHashExtractor;

impl ZeroCopyKeyExtractor for TxHashExtractor {
    fn extract_ref<'a>(&self, batch: &'a RecordBatch, row_idx: usize) -> Option<&'a [u8]> {
        // tx_hash is a Struct with a "bytes" field that contains FixedSizeBinary(32)
        let column = batch.column(columns::TX_HASH);
        let struct_array = column.as_struct();

        // Get the "bytes" field from the struct
        let bytes_column = struct_array
            .column_by_name("bytes")
            .expect("Hash32 struct should have 'bytes' field");
        let hash_array = bytes_column.as_fixed_size_binary();

        // Get direct reference to the 32-byte hash
        if hash_array.is_null(row_idx) {
            return None;
        }

        Some(hash_array.value(row_idx))
    }
}

impl KeyExtractor for TxHashExtractor {
    fn extract_into(&self, batch: &RecordBatch, row_idx: usize, buf: &mut Vec<u8>) -> bool {
        if let Some(key_ref) = self.extract_ref(batch, row_idx) {
            buf.clear();
            buf.extend_from_slice(key_ref);
            true
        } else {
            false
        }
    }
}

/// Composite key extractor for from_address || block_num || tx_index
///
/// Format: [20 bytes address][8 bytes block_num (big-endian)][4 bytes tx_index (big-endian)]
struct FromAddressExtractor;

impl CompositeKeyExtractor for FromAddressExtractor {
    fn extract_into(&self, batch: &RecordBatch, row_idx: usize, buf: &mut Vec<u8>) -> bool {
        // from is a Struct with a "bytes" field that contains FixedSizeBinary(20)
        let from_column = batch.column(columns::FROM);
        let from_struct = from_column.as_struct();
        let from_bytes = from_struct
            .column_by_name("bytes")
            .expect("Address20 struct should have 'bytes' field");
        let from_array = from_bytes.as_fixed_size_binary();

        if from_array.is_null(row_idx) {
            return false;
        }

        let from_bytes = from_array.value(row_idx);

        // block_num is a UInt64 column
        let block_num_column = batch.column(columns::BLOCK_NUM);
        let block_num_array = block_num_column.as_primitive::<UInt64Type>();
        let block_num = block_num_array.value(row_idx);

        // tx_index is a UInt32 column (column 4)
        let tx_index_column = batch.column(4);
        let tx_index_array = tx_index_column.as_primitive::<arrow::datatypes::UInt32Type>();
        let tx_index = tx_index_array.value(row_idx);

        // Build composite key: address || block_num || tx_index
        buf.clear();
        buf.extend_from_slice(from_bytes); // 20 bytes
        buf.extend_from_slice(&block_num.to_be_bytes()); // 8 bytes
        buf.extend_from_slice(&tx_index.to_be_bytes()); // 4 bytes

        true
    }
}

impl KeyExtractor for FromAddressExtractor {
    fn extract_into(&self, batch: &RecordBatch, row_idx: usize, buf: &mut Vec<u8>) -> bool {
        <Self as CompositeKeyExtractor>::extract_into(self, batch, row_idx, buf)
    }
}

/// Composite key extractor for to_address || block_num || tx_index
///
/// Format: [20 bytes address][8 bytes block_num (big-endian)][4 bytes tx_index (big-endian)]
/// Skips rows where to is None (contract creation)
struct ToAddressExtractor;

impl CompositeKeyExtractor for ToAddressExtractor {
    fn extract_into(&self, batch: &RecordBatch, row_idx: usize, buf: &mut Vec<u8>) -> bool {
        // to is an Option<Address20> - represented as a nullable struct with "bytes" field
        let to_column = batch.column(columns::TO);
        let to_struct = to_column.as_struct();

        // Check if the struct is null (None case - contract creation)
        if to_struct.is_null(row_idx) {
            return false;
        }

        // Get the "bytes" field from the struct
        let bytes_column = to_struct
            .column_by_name("bytes")
            .expect("Address20 struct should have 'bytes' field");
        let to_array = bytes_column.as_fixed_size_binary();

        if to_array.is_null(row_idx) {
            return false;
        }

        let to_bytes = to_array.value(row_idx);

        // block_num is a UInt64 column
        let block_num_column = batch.column(columns::BLOCK_NUM);
        let block_num_array = block_num_column.as_primitive::<UInt64Type>();
        let block_num = block_num_array.value(row_idx);

        // tx_index is a UInt32 column (column 4)
        let tx_index_column = batch.column(4);
        let tx_index_array = tx_index_column.as_primitive::<arrow::datatypes::UInt32Type>();
        let tx_index = tx_index_array.value(row_idx);

        // Build composite key: address || block_num || tx_index
        buf.clear();
        buf.extend_from_slice(to_bytes); // 20 bytes
        buf.extend_from_slice(&block_num.to_be_bytes()); // 8 bytes
        buf.extend_from_slice(&tx_index.to_be_bytes()); // 4 bytes

        true
    }
}

impl KeyExtractor for ToAddressExtractor {
    fn extract_into(&self, batch: &RecordBatch, row_idx: usize, buf: &mut Vec<u8>) -> bool {
        <Self as CompositeKeyExtractor>::extract_into(self, batch, row_idx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_specs() {
        let specs = EvmTransactionIndexer::index_specs();
        assert_eq!(specs.len(), 3);

        assert_eq!(specs[0].column_family, CF_TX_BY_HASH);
        assert_eq!(specs[0].column_index, columns::TX_HASH);

        assert_eq!(specs[1].column_family, CF_TX_BY_FROM);
        assert_eq!(specs[1].column_index, columns::FROM);

        assert_eq!(specs[2].column_family, CF_TX_BY_TO);
        assert_eq!(specs[2].column_index, columns::TO);
    }

    #[test]
    fn test_schema_name() {
        assert_eq!(EvmTransactionIndexer::schema_name(), "evm_transactions");
    }

    // Key extraction tests

    /// Helper to create a minimal RecordBatch for testing key extraction
    fn create_test_batch() -> RecordBatch {
        use evm_common::transaction::TransactionRecord;
        use evm_common::types::{Address20, Hash32, Wei};
        use typed_arrow::prelude::*;

        // Create test data
        let block_nums = [1000u64, 1000, 1001, 1001];
        let tx_indices = [0u32, 1, 0, 1];

        let tx_hashes = [Hash32 { bytes: [1u8; 32] },
            Hash32 { bytes: [2u8; 32] },
            Hash32 { bytes: [3u8; 32] },
            Hash32 { bytes: [4u8; 32] }];

        let from_addresses = [
            Address20 { bytes: [10u8; 20] },
            Address20 { bytes: [10u8; 20] }, // Same address, different tx_index
            Address20 { bytes: [20u8; 20] },
            Address20 { bytes: [30u8; 20] },
        ];

        let to_addresses = [
            Some(Address20 { bytes: [100u8; 20] }),
            None, // Contract creation
            Some(Address20 { bytes: [200u8; 20] }),
            Some(Address20 { bytes: [250u8; 20] }),
        ];

        let records: Vec<TransactionRecord> = (0..4)
            .map(|i| TransactionRecord {
                _block_num: block_nums[i],
                block_hash: Hash32 { bytes: [0u8; 32] },
                block_num: block_nums[i],
                timestamp: 1700000000000000000,
                tx_index: tx_indices[i],
                tx_hash: tx_hashes[i].clone(),
                from: from_addresses[i].clone(),
                to: to_addresses[i].clone(),
                nonce: 0,
                gas_price: None,
                gas_limit: 21000,
                gas_used: 21000,
                value: Wei { bytes: [0u8; 32] },
                input: vec![],
                v: vec![0x1c],
                r: vec![0; 32],
                s: vec![0; 32],
                tx_type: 0,
                status: true,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                chain_id: Some(1),
                access_list: None,
                blob_versioned_hashes: None,
                authorization_list: None,
            })
            .collect();

        let mut builder = <TransactionRecord as BuildRows>::new_builders(records.len());
        builder.append_rows(records);
        let arrays = builder.finish();
        arrays.into_record_batch()
    }

    #[test]
    fn test_tx_hash_extractor() {
        let batch = create_test_batch();
        let extractor = TxHashExtractor;

        // Extract from row 0
        let key = extractor
            .extract_ref(&batch, 0)
            .expect("Should extract key");

        // Should be 32 bytes
        assert_eq!(key.len(), 32, "Hash should be 32 bytes");
        assert_eq!(key, &[1u8; 32]);

        // Test row 1
        let key = extractor
            .extract_ref(&batch, 1)
            .expect("Should extract key");
        assert_eq!(key, &[2u8; 32]);
    }

    #[test]
    fn test_from_address_extractor() {
        let batch = create_test_batch();
        let extractor = FromAddressExtractor;
        let mut buf = Vec::new();

        // Extract from row 0: address=[10u8;20], block_num=1000, tx_index=0
        assert!(
            KeyExtractor::extract_into(&extractor, &batch, 0, &mut buf),
            "Should extract key"
        );

        // Should be 32 bytes total (20 address + 8 block_num + 4 tx_index)
        assert_eq!(buf.len(), 32, "Key should be 32 bytes");

        // First 20 bytes: address
        assert_eq!(&buf[0..20], &[10u8; 20]);

        // Next 8 bytes: block_num (big-endian)
        let block_num_bytes = &buf[20..28];
        let block_num = u64::from_be_bytes(block_num_bytes.try_into().unwrap());
        assert_eq!(block_num, 1000);

        // Last 4 bytes: tx_index (big-endian)
        let tx_index_bytes = &buf[28..32];
        let tx_index = u32::from_be_bytes(tx_index_bytes.try_into().unwrap());
        assert_eq!(tx_index, 0);
    }

    #[test]
    fn test_from_address_extractor_uniqueness() {
        let batch = create_test_batch();
        let extractor = FromAddressExtractor;
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        // Rows 0 and 1 have the same address and block_num but different tx_index
        KeyExtractor::extract_into(&extractor, &batch, 0, &mut buf1);
        KeyExtractor::extract_into(&extractor, &batch, 1, &mut buf2);

        // Keys should be different (because tx_index differs)
        assert_ne!(buf1, buf2, "Keys should differ when tx_index differs");

        // First 28 bytes should be the same (address + block_num)
        assert_eq!(&buf1[0..28], &buf2[0..28]);

        // Last 4 bytes should differ (tx_index)
        assert_ne!(&buf1[28..32], &buf2[28..32]);
    }

    #[test]
    fn test_to_address_extractor_with_value() {
        let batch = create_test_batch();
        let extractor = ToAddressExtractor;
        let mut buf = Vec::new();

        // Extract from row 0: to=Some([100u8;20])
        assert!(
            KeyExtractor::extract_into(&extractor, &batch, 0, &mut buf),
            "Should extract key"
        );

        // Should be 32 bytes total
        assert_eq!(buf.len(), 32, "Key should be 32 bytes");

        // First 20 bytes: to address
        assert_eq!(&buf[0..20], &[100u8; 20]);

        // Verify block_num
        let block_num = u64::from_be_bytes(buf[20..28].try_into().unwrap());
        assert_eq!(block_num, 1000);

        // Verify tx_index
        let tx_index = u32::from_be_bytes(buf[28..32].try_into().unwrap());
        assert_eq!(tx_index, 0);
    }

    #[test]
    fn test_to_address_extractor_with_none() {
        let batch = create_test_batch();
        let extractor = ToAddressExtractor;
        let mut buf = Vec::new();

        // Extract from row 1: to=None (contract creation)
        let result = KeyExtractor::extract_into(&extractor, &batch, 1, &mut buf);

        // Should return false for contract creations
        assert!(!result, "Should skip contract creations");

        // Buffer should be empty (cleared)
        assert_eq!(buf.len(), 0, "Buffer should be empty for skipped rows");
    }

    #[test]
    fn test_composite_key_ordering() {
        let batch = create_test_batch();
        let extractor = FromAddressExtractor;

        let mut keys: Vec<Vec<u8>> = Vec::new();

        // Extract all keys
        for row in 0..batch.num_rows() {
            let mut buf = Vec::new();
            if KeyExtractor::extract_into(&extractor, &batch, row, &mut buf) {
                keys.push(buf);
            }
        }

        // Keys should sort by address first, then block_num, then tx_index
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();

        // Verify the sort order matches our expectations
        // Row 0: addr=10, block=1000, idx=0
        // Row 1: addr=10, block=1000, idx=1
        // Row 2: addr=20, block=1001, idx=0
        // Row 3: addr=30, block=1001, idx=1

        assert_eq!(sorted_keys[0], keys[0]); // addr=10, block=1000, idx=0
        assert_eq!(sorted_keys[1], keys[1]); // addr=10, block=1000, idx=1
        assert_eq!(sorted_keys[2], keys[2]); // addr=20, block=1001, idx=0
        assert_eq!(sorted_keys[3], keys[3]); // addr=30, block=1001, idx=1
    }
}
