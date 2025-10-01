use anyhow::Result;
use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, RecordBatch, StructArray, UInt64Array,
};
use rocksdb::DB;
use std::sync::Arc;
use tracing::debug;

/// Writer for trie data into RocksDB
/// Stores trie nodes as hash -> RLP mapping for state reconstruction
pub struct TrieWriter {
    db: Arc<DB>,
    cf_name: String,
    nodes_written: u64,
}

impl TrieWriter {
    /// Create a new trie writer that writes to RocksDB column family
    pub fn new(db: Arc<DB>, cf_name: String) -> Result<Self> {
        // Ensure column family exists
        let _cf = db
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf_name))?;

        Ok(Self {
            db,
            cf_name,
            nodes_written: 0,
        })
    }

    /// Write a batch of trie nodes to RocksDB
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate columns
        if batch.num_columns() < 3 {
            return Err(anyhow::anyhow!(
                "Invalid trie batch: expected 3 columns, got {}",
                batch.num_columns()
            ));
        }

        // Column 0: hash - Struct with a single field "bytes" of type FixedSizeBinary(32)
        let hash_struct = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow::anyhow!("First column is not a Struct (hash)"))?;

        // Get the "bytes" field from the struct (it should be the first/only field)
        let hash_bytes = hash_struct
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("Hash struct's bytes field is not FixedSizeBinary"))?;

        // Column 1: raw_rlp - Binary
        let rlp_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("Second column is not Binary (raw_rlp)"))?;

        // Column 2: step - UInt64
        let _step_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow::anyhow!("Third column is not UInt64 (step)"))?;

        let cf = self
            .db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", self.cf_name))?;

        // Write each node to RocksDB
        let mut write_batch = rocksdb::WriteBatch::default();
        let mut count = 0;

        for i in 0..batch.num_rows() {
            if !hash_bytes.is_null(i) && !rlp_array.is_null(i) {
                let hash = hash_bytes.value(i);
                let rlp = rlp_array.value(i);

                // Key: hash (32 bytes)
                // Value: RLP-encoded node data
                write_batch.put_cf(cf, hash, rlp);
                count += 1;
            }
        }

        // Write the batch atomically
        self.db.write(write_batch)?;
        self.nodes_written += count;

        debug!(
            "Wrote {} trie nodes to RocksDB (total: {})",
            count, self.nodes_written
        );

        Ok(())
    }

    /// Get the total number of nodes written
    pub fn nodes_written(&self) -> u64 {
        self.nodes_written
    }
}
