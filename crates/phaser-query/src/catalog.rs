use crate::buffer_manager::{CfToParquetBuffer, QueryManager};
use crate::index::cf;
use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;
use std::sync::Arc;

/// RocksDB-backed catalog implementation
/// Stores only indexes pointing to Parquet files, not the data itself
pub struct RocksDbCatalog {
    pub db: Arc<DB>,
    parquet_files: Vec<String>,
    pub streaming_buffer: Option<Arc<CfToParquetBuffer>>,
    pub query_manager: Option<Arc<QueryManager>>,
}

impl RocksDbCatalog {
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = vec![
            // File registries
            ColumnFamilyDescriptor::new(cf::BLOCK_FILES, Options::default()),
            ColumnFamilyDescriptor::new(cf::TX_FILES, Options::default()),
            ColumnFamilyDescriptor::new(cf::LOG_FILES, Options::default()),
            // Data indexes
            ColumnFamilyDescriptor::new(cf::BLOCKS, Options::default()),
            ColumnFamilyDescriptor::new(cf::TRANSACTIONS, Options::default()),
            ColumnFamilyDescriptor::new(cf::LOGS, Options::default()),
            // New buffer and index column families
            ColumnFamilyDescriptor::new(cf::STREAMING_BUFFER, Options::default()),
            ColumnFamilyDescriptor::new(cf::STREAMING_INDEX, Options::default()),
            ColumnFamilyDescriptor::new(cf::HISTORICAL_INDEX, Options::default()),
            // Trie data
            ColumnFamilyDescriptor::new(cf::TRIE, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)?;

        Ok(Self {
            db: Arc::new(db),
            parquet_files: Vec::new(),
            streaming_buffer: None,
            query_manager: None,
        })
    }

    /// Initialize buffer manager and query manager
    pub fn init_buffer_manager(
        &mut self,
        target_dir: std::path::PathBuf,
        row_group_size: usize,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<()> {
        let buffer = Arc::new(CfToParquetBuffer::new(
            self.db.clone(),
            cf::STREAMING_BUFFER,
            cf::STREAMING_INDEX,
            target_dir,
            row_group_size,
            schema,
        )?);

        let query_mgr = Arc::new(QueryManager::new(
            self.db.clone(),
            buffer.clone(),
            cf::HISTORICAL_INDEX,
        ));

        self.streaming_buffer = Some(buffer);
        self.query_manager = Some(query_mgr);

        Ok(())
    }

    pub fn set_parquet_files(&mut self, files: Vec<String>) {
        self.parquet_files = files;
    }

    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    /// Get a block pointer by block number
    pub fn get_block(&self, block_number: u64) -> Result<Option<crate::index::BlockPointer>> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::BLOCKS)
            .ok_or_else(|| anyhow::anyhow!("blocks CF not found"))?;

        let key = crate::index::keys::block_key(block_number);

        match self.db.get_cf(cf, key)? {
            Some(bytes) => Ok(Some(
                crate::index::BlockPointer::from_bytes(&bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to parse block pointer: {}", e))?,
            )),
            None => Ok(None),
        }
    }

    /// Get a transaction pointer by hash
    pub fn get_transaction(
        &self,
        tx_hash: &[u8; 32],
    ) -> Result<Option<crate::index::TransactionPointer>> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::TRANSACTIONS)
            .ok_or_else(|| anyhow::anyhow!("transactions CF not found"))?;

        let key = crate::index::keys::tx_key(tx_hash);

        match self.db.get_cf(cf, key)? {
            Some(bytes) => Ok(Some(
                crate::index::TransactionPointer::from_bytes(&bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to parse tx pointer: {}", e))?,
            )),
            None => Ok(None),
        }
    }

    /// Store a block pointer
    pub fn put_block(&self, block_number: u64, pointer: &crate::index::BlockPointer) -> Result<()> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::BLOCKS)
            .ok_or_else(|| anyhow::anyhow!("blocks CF not found"))?;

        let key = crate::index::keys::block_key(block_number);
        self.db.put_cf(cf, key, pointer.to_bytes())?;
        Ok(())
    }

    /// Store a transaction pointer
    pub fn put_transaction(
        &self,
        tx_hash: &[u8; 32],
        pointer: &crate::index::TransactionPointer,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::TRANSACTIONS)
            .ok_or_else(|| anyhow::anyhow!("transactions CF not found"))?;

        let key = crate::index::keys::tx_key(tx_hash);
        self.db.put_cf(cf, key, pointer.to_bytes())?;
        Ok(())
    }

    /// Get block file info for a given block range
    pub fn get_block_file(&self, block_number: u64) -> Result<Option<crate::index::FileInfo>> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::BLOCK_FILES)
            .ok_or_else(|| anyhow::anyhow!("block_files CF not found"))?;

        // Find the file that contains this block by iterating backwards
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::End);
        for item in iter {
            let (key, value) = item?;
            let block_start = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            let file_info: crate::index::FileInfo = bincode::deserialize(&value)?;

            if block_number >= file_info.block_start && block_number <= file_info.block_end {
                return Ok(Some(file_info));
            }

            if block_start < block_number {
                break; // No file contains this block
            }
        }
        Ok(None)
    }

    /// Store block file info
    pub fn put_block_file(&self, file_info: &crate::index::FileInfo) -> Result<()> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::BLOCK_FILES)
            .ok_or_else(|| anyhow::anyhow!("block_files CF not found"))?;

        let key = crate::index::keys::file_range_key(file_info.block_start);
        let value = bincode::serialize(file_info)?;
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }

    /// Get a log pointer by block and log index
    pub fn get_log(
        &self,
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<crate::index::LogPointer>> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::LOGS)
            .ok_or_else(|| anyhow::anyhow!("logs CF not found"))?;

        let key = crate::index::keys::log_key(block_number, log_index);

        match self.db.get_cf(cf, key)? {
            Some(bytes) => Ok(Some(
                crate::index::LogPointer::from_bytes(&bytes)
                    .map_err(|e| anyhow::anyhow!("Failed to parse log pointer: {}", e))?,
            )),
            None => Ok(None),
        }
    }

    /// Store a log pointer
    pub fn put_log(
        &self,
        block_number: u64,
        log_index: u32,
        pointer: &crate::index::LogPointer,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(crate::index::cf::LOGS)
            .ok_or_else(|| anyhow::anyhow!("logs CF not found"))?;

        let key = crate::index::keys::log_key(block_number, log_index);
        self.db.put_cf(cf, key, pointer.to_bytes())?;
        Ok(())
    }
}

impl std::fmt::Debug for RocksDbCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbCatalog")
            .field("parquet_files", &self.parquet_files.len())
            .finish()
    }
}
