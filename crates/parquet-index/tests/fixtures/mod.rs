/// Test data generation utilities for parquet indexing tests
use anyhow::Result;
use evm_common::transaction::TransactionRecord;
use evm_common::types::{Address20, Hash32, Wei};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use tempfile::TempDir;

/// Public helper to create Address20 from u64 for tests
pub fn make_address(n: u64) -> Address20 {
    address20_from_u64(n)
}

/// Test transaction data for verification
#[derive(Debug, Clone)]
pub struct TestTransaction {
    pub block_num: u64,
    pub tx_hash: Hash32,
    pub from: Address20,
    pub to: Option<Address20>,
    pub tx_index: u32,
}

/// Helper functions to create deterministic test data
fn hash32_from_u64(n: u64) -> Hash32 {
    let mut bytes = [0u8; 32];
    bytes[0..8].copy_from_slice(&n.to_be_bytes());
    Hash32 { bytes }
}

fn address20_from_u64(n: u64) -> Address20 {
    let mut bytes = [0u8; 20];
    bytes[0..8].copy_from_slice(&n.to_be_bytes());
    Address20 { bytes }
}

fn wei_from_u64(n: u64) -> Wei {
    let mut bytes = [0u8; 32];
    bytes[24..32].copy_from_slice(&n.to_be_bytes());
    Wei { bytes }
}

impl TestTransaction {
    /// Create a deterministic test transaction from an index
    pub fn from_index(i: usize) -> Self {
        Self {
            block_num: 1000 + (i / 100) as u64,
            tx_hash: hash32_from_u64(i as u64),
            from: address20_from_u64((i % 50) as u64),
            // Every 10th transaction is a contract creation (to=None)
            to: if i.is_multiple_of(10) {
                None
            } else {
                Some(address20_from_u64(((i + 50) % 50) as u64))
            },
            tx_index: (i % 100) as u32,
        }
    }

    /// Convert to full TransactionRecord for writing to parquet
    fn to_record(&self) -> TransactionRecord {
        TransactionRecord {
            _block_num: self.block_num,
            block_hash: hash32_from_u64(self.block_num),
            block_num: self.block_num,
            timestamp: 1700000000000000000 + (self.tx_index as i64) * 1000000000,
            tx_index: self.tx_index,
            tx_hash: self.tx_hash.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            nonce: self.tx_index as u64,
            gas_price: Some(wei_from_u64(20000000000)),
            gas_limit: 21000,
            gas_used: 21000,
            value: wei_from_u64(1000000000000000000),
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
        }
    }
}

/// Generate a parquet file with test transactions
///
/// Returns (temp_dir, parquet_path, expected_transactions)
/// The temp_dir must be kept alive to prevent cleanup.
pub fn generate_test_parquet(count: usize) -> Result<(TempDir, PathBuf, Vec<TestTransaction>)> {
    use typed_arrow::prelude::*;
    use typed_arrow::schema::SchemaMeta;

    let temp_dir = TempDir::new()?;
    let parquet_path = temp_dir.path().join("test_transactions.parquet");

    // Generate test transactions
    let test_txs: Vec<TestTransaction> = (0..count).map(TestTransaction::from_index).collect();

    // Convert to full TransactionRecords
    let records: Vec<TransactionRecord> = test_txs.iter().map(|tx| tx.to_record()).collect();

    // Get the schema from typed-arrow
    let schema = TransactionRecord::schema();

    // Convert Vec<TransactionRecord> to RecordBatch using typed-arrow's BuildRows trait
    let mut builder = <TransactionRecord as BuildRows>::new_builders(records.len());
    builder.append_rows(records);
    let arrays = builder.finish();
    let batch = arrays.into_record_batch();

    // Write to parquet
    let file = File::create(&parquet_path)?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100)) // Small row groups for testing
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok((temp_dir, parquet_path, test_txs))
}
