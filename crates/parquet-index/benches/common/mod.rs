/// Shared utilities for benchmarks
use anyhow::Result;
use evm_common::transaction::TransactionRecord;
use evm_common::types::{Address20, Hash32, Wei};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use tempfile::TempDir;
use typed_arrow::prelude::*;

#[allow(dead_code)]
pub struct TestTransaction {
    pub block_num: u64,
    pub tx_index: u32,
    pub tx_hash: Hash32,
    pub from: Address20,
    pub to: Option<Address20>,
}

impl TestTransaction {
    pub fn from_index(i: usize) -> Self {
        let block_num = (i / 10) as u64; // 10 txs per block
        let tx_index = (i % 10) as u32;

        // Create deterministic addresses and hashes
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0..8].copy_from_slice(&(i as u64).to_be_bytes());

        let address_byte = ((i % 10) as u8) * 5; // Cycle through 10 addresses
        let from = make_address(address_byte);

        // 20% contract creations (to=None)
        let to = if i.is_multiple_of(5) {
            None
        } else {
            Some(make_address(address_byte + 100))
        };

        Self {
            block_num,
            tx_index,
            tx_hash: Hash32 { bytes: hash_bytes },
            from,
            to,
        }
    }

    pub fn to_record(&self) -> TransactionRecord {
        TransactionRecord {
            _block_num: self.block_num,
            block_hash: Hash32 { bytes: [0u8; 32] },
            block_num: self.block_num,
            timestamp: 1700000000000000000,
            tx_index: self.tx_index,
            tx_hash: self.tx_hash.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            nonce: 0,
            gas_price: Some(Wei { bytes: [0u8; 32] }),
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
        }
    }
}

pub fn make_address(byte: u8) -> Address20 {
    let mut bytes = [0u8; 20];
    bytes[0] = byte;
    Address20 { bytes }
}

/// Generate a test parquet file with the specified number of transactions
pub fn generate_test_parquet(count: usize) -> Result<(TempDir, PathBuf, Vec<TestTransaction>)> {
    use typed_arrow::schema::SchemaMeta;

    let temp_dir = TempDir::new()?;
    let parquet_path = temp_dir.path().join("test_transactions.parquet");

    let test_txs: Vec<TestTransaction> = (0..count).map(TestTransaction::from_index).collect();
    let records: Vec<TransactionRecord> = test_txs.iter().map(|tx| tx.to_record()).collect();

    let schema = TransactionRecord::schema();

    // Use typed-arrow's BuildRows trait to convert to RecordBatch
    let mut builder = <TransactionRecord as BuildRows>::new_builders(records.len());
    builder.append_rows(records);
    let arrays = builder.finish();
    let batch = arrays.into_record_batch();

    let file = File::create(&parquet_path)?;
    let props = WriterProperties::builder()
        .set_max_row_group_size(100)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok((temp_dir, parquet_path, test_txs))
}
