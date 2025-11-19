use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rocksdb::{WriteBatch, DB};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::index::{BlockPointer, FileInfo};

/// Generic buffer manager for accumulating data in RocksDB before writing to Parquet
pub struct CfToParquetBuffer {
    db: Arc<DB>,
    buffer_cf: String,
    index_cf: String,
    target_dir: PathBuf,
    row_group_size: usize,
    schema: Arc<Schema>,
    file_counter: std::sync::atomic::AtomicU64,
}

impl CfToParquetBuffer {
    pub fn new(
        db: Arc<DB>,
        buffer_cf_name: &str,
        index_cf_name: &str,
        target_dir: PathBuf,
        row_group_size: usize,
        schema: Arc<Schema>,
    ) -> Result<Self> {
        // Ensure target directory exists
        std::fs::create_dir_all(&target_dir)?;

        Ok(Self {
            db,
            buffer_cf: buffer_cf_name.to_string(),
            index_cf: index_cf_name.to_string(),
            target_dir,
            row_group_size,
            schema,
            file_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Add a record batch to the buffer
    pub fn add(&self, key: u64, batch: RecordBatch) -> Result<()> {
        let cf = self
            .db
            .cf_handle(&self.buffer_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.buffer_cf))?;

        // Serialize the RecordBatch
        let bytes = self.serialize_batch(&batch)?;

        // Store in buffer CF
        self.db.put_cf(cf, key.to_be_bytes(), bytes)?;

        debug!("Added block {} to buffer", key);

        // Check if we should flush
        if self.should_flush()? {
            info!("Buffer reached row group size, flushing to Parquet");
            self.flush_to_parquet()?;
        }

        Ok(())
    }

    /// Check if buffer has reached flush threshold
    fn should_flush(&self) -> Result<bool> {
        let count = self.buffer_count()?;
        Ok(count >= self.row_group_size)
    }

    /// Count items in buffer
    pub fn buffer_count(&self) -> Result<usize> {
        let cf = self
            .db
            .cf_handle(&self.buffer_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.buffer_cf))?;

        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    /// Flush buffer to Parquet file atomically
    pub fn flush_to_parquet(&self) -> Result<()> {
        let cf = self
            .db
            .cf_handle(&self.buffer_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.buffer_cf))?;

        // 1. Scan all items from buffer
        let mut items = Vec::new();
        let mut min_key = u64::MAX;
        let mut max_key = 0u64;

        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key_bytes, value_bytes) = item?;
            let key = u64::from_be_bytes(key_bytes.as_ref().try_into()?);
            let batch = self.deserialize_batch(&value_bytes)?;

            min_key = min_key.min(key);
            max_key = max_key.max(key);

            items.push((key, batch));
        }

        if items.is_empty() {
            debug!("No items to flush");
            return Ok(());
        }

        info!(
            "Flushing {} items to Parquet (blocks {} to {})",
            items.len(),
            min_key,
            max_key
        );

        // Sort by key for optimal Parquet layout
        items.sort_by_key(|(k, _)| *k);

        // 2. Write to Parquet file
        let file_path = self.write_parquet_file(&items, min_key, max_key)?;

        // 3. Create atomic batch for index update and buffer cleanup
        let mut batch = WriteBatch::default();

        let index_cf = self
            .db
            .cf_handle(&self.index_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.index_cf))?;

        // Add file info to index
        let file_info = FileInfo {
            file_path: file_path.to_string_lossy().to_string(),
            block_start: min_key,
            block_end: max_key,
            row_count: items.len() as u32,
        };

        // Store file info for the range
        let file_info_key = format!("file_{min_key:016x}_{max_key:016x}");
        batch.put_cf(
            index_cf,
            file_info_key.as_bytes(),
            bincode::serialize(&file_info)?,
        );

        // Add index entries for each block
        for (i, (key, _)) in items.iter().enumerate() {
            let pointer = BlockPointer {
                row_group: 0, // Single row group for now
                row_offset: i as u32,
            };
            batch.put_cf(index_cf, key.to_be_bytes(), pointer.to_bytes());
        }

        // Delete items from buffer
        for (key, _) in &items {
            batch.delete_cf(cf, key.to_be_bytes());
        }

        // 4. Atomic commit
        self.db.write(batch)?;

        info!(
            "Successfully flushed {} blocks to {}",
            items.len(),
            file_path.display()
        );

        Ok(())
    }

    /// Write items to a Parquet file
    fn write_parquet_file(
        &self,
        items: &[(u64, RecordBatch)],
        min_key: u64,
        max_key: u64,
    ) -> Result<PathBuf> {
        // Generate filename with timestamp and range
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let counter = self
            .file_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let filename = format!("blocks_{timestamp}_{counter}_{min_key:08}_to_{max_key:08}.parquet");

        let file_path = self.target_dir.join(&filename);
        let temp_path = self.target_dir.join(format!(".{filename}.tmp"));

        // Write to temp file first
        let file = File::create(&temp_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_max_row_group_size(self.row_group_size)
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;

        // Write all batches
        for (_, batch) in items {
            writer.write(batch)?;
        }

        writer.close()?;

        // Atomic rename
        std::fs::rename(&temp_path, &file_path)?;

        Ok(file_path)
    }

    /// Get a block from the buffer (if present)
    pub fn get_from_buffer(&self, key: u64) -> Result<Option<RecordBatch>> {
        let cf = self
            .db
            .cf_handle(&self.buffer_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.buffer_cf))?;

        match self.db.get_cf(cf, key.to_be_bytes())? {
            Some(bytes) => Ok(Some(self.deserialize_batch(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Get a block pointer from the index
    pub fn get_index_pointer(&self, key: u64) -> Result<Option<BlockPointer>> {
        let cf = self
            .db
            .cf_handle(&self.index_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.index_cf))?;

        match self.db.get_cf(cf, key.to_be_bytes())? {
            Some(bytes) => Ok(Some(BlockPointer::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Serialize a RecordBatch to bytes
    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        // For now, use Arrow IPC format for serialization
        use arrow::ipc::writer::StreamWriter;
        use std::io::Cursor;

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &self.schema)?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buffer.into_inner())
    }

    /// Deserialize bytes to a RecordBatch
    fn deserialize_batch(&self, bytes: &[u8]) -> Result<RecordBatch> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None)?;

        // Read the first (and only) batch
        if let Some(batch_result) = reader.next() {
            Ok(batch_result?)
        } else {
            Err(anyhow!("No batch found in serialized data"))
        }
    }

    /// Force flush any remaining items (for shutdown)
    pub fn force_flush(&self) -> Result<()> {
        let count = self.buffer_count()?;
        if count > 0 {
            info!("Force flushing {} remaining items", count);
            self.flush_to_parquet()?;
        }
        Ok(())
    }

    /// Get file info for a block range
    pub fn get_file_info(&self, block_num: u64) -> Result<Option<FileInfo>> {
        let cf = self
            .db
            .cf_handle(&self.index_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.index_cf))?;

        // Scan for file info entries that might contain this block
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);

            // File info keys start with "file_"
            if key_str.starts_with("file_") {
                let file_info: FileInfo = bincode::deserialize(&value)?;
                if block_num >= file_info.block_start && block_num <= file_info.block_end {
                    return Ok(Some(file_info));
                }
            }
        }

        Ok(None)
    }
}

/// Query manager that handles three-tier lookup across buffer, streaming, and historical
pub struct QueryManager {
    db: Arc<DB>,
    streaming_buffer: Arc<CfToParquetBuffer>,
    historical_index_cf: String,
}

impl QueryManager {
    pub fn new(
        db: Arc<DB>,
        streaming_buffer: Arc<CfToParquetBuffer>,
        historical_index_cf: &str,
    ) -> Self {
        Self {
            db,
            streaming_buffer,
            historical_index_cf: historical_index_cf.to_string(),
        }
    }

    /// Three-tier query for a block
    /// 1. Check streaming buffer (most recent, not yet in Parquet)
    /// 2. Check streaming index (points to streaming/*.parquet)
    /// 3. Check historical index (points to historical/*.parquet)
    pub fn get_block(&self, block_num: u64) -> Result<Option<RecordBatch>> {
        // 1. Check streaming buffer first
        if let Some(batch) = self.streaming_buffer.get_from_buffer(block_num)? {
            debug!("Found block {} in streaming buffer", block_num);
            return Ok(Some(batch));
        }

        // 2. Check streaming index
        if let Some(pointer) = self.streaming_buffer.get_index_pointer(block_num)? {
            debug!("Found block {} in streaming index", block_num);
            if let Some(file_info) = self.streaming_buffer.get_file_info(block_num)? {
                return self.read_from_parquet(&file_info, &pointer, block_num);
            }
        }

        // 3. Check historical index
        if let Some((file_info, pointer)) = self.get_historical_pointer(block_num)? {
            debug!("Found block {} in historical index", block_num);
            return self.read_from_parquet(&file_info, &pointer, block_num);
        }

        Ok(None)
    }

    /// Get pointer from historical index
    fn get_historical_pointer(&self, block_num: u64) -> Result<Option<(FileInfo, BlockPointer)>> {
        let cf = self
            .db
            .cf_handle(&self.historical_index_cf)
            .ok_or_else(|| anyhow!("{} CF not found", self.historical_index_cf))?;

        // Get block pointer
        let pointer = match self.db.get_cf(cf, block_num.to_be_bytes())? {
            Some(bytes) => BlockPointer::from_bytes(&bytes)?,
            None => return Ok(None),
        };

        // Get file info - scan for matching range
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);

            if key_str.starts_with("file_") {
                let file_info: FileInfo = bincode::deserialize(&value)?;
                if block_num >= file_info.block_start && block_num <= file_info.block_end {
                    return Ok(Some((file_info, pointer)));
                }
            }
        }

        Ok(None)
    }

    /// Read a block from a Parquet file using pointer
    fn read_from_parquet(
        &self,
        file_info: &FileInfo,
        pointer: &BlockPointer,
        block_num: u64,
    ) -> Result<Option<RecordBatch>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(&file_info.file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Set row group and create reader
        let reader = builder
            .with_row_groups(vec![pointer.row_group as usize])
            .build()?;

        // Read batches and find the specific row
        let mut row_count = 0;
        for batch_result in reader {
            let batch = batch_result?;

            // Check if our row is in this batch
            if row_count + batch.num_rows() > pointer.row_offset as usize {
                // Extract the specific row
                let row_in_batch = pointer.row_offset as usize - row_count;

                // Create a new batch with just this row
                let single_row = batch.slice(row_in_batch, 1);
                return Ok(Some(single_row));
            }

            row_count += batch.num_rows();
        }

        error!(
            "Block {} not found at expected position in Parquet file",
            block_num
        );
        Ok(None)
    }

    /// Get statistics about buffer and index states
    pub fn get_stats(&self) -> Result<BufferStats> {
        let buffer_count = self.streaming_buffer.buffer_count()?;

        let streaming_cf = self
            .db
            .cf_handle(&self.streaming_buffer.index_cf)
            .ok_or_else(|| anyhow!("Streaming index CF not found"))?;

        let historical_cf = self
            .db
            .cf_handle(&self.historical_index_cf)
            .ok_or_else(|| anyhow!("Historical index CF not found"))?;

        let streaming_indexed = self
            .db
            .iterator_cf(streaming_cf, rocksdb::IteratorMode::Start)
            .filter(|item| {
                if let Ok((key, _)) = item {
                    !String::from_utf8_lossy(key).starts_with("file_")
                } else {
                    false
                }
            })
            .count();

        let historical_indexed = self
            .db
            .iterator_cf(historical_cf, rocksdb::IteratorMode::Start)
            .filter(|item| {
                if let Ok((key, _)) = item {
                    !String::from_utf8_lossy(key).starts_with("file_")
                } else {
                    false
                }
            })
            .count();

        Ok(BufferStats {
            buffer_count,
            streaming_indexed,
            historical_indexed,
        })
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub buffer_count: usize,
    pub streaming_indexed: usize,
    pub historical_indexed: usize,
}
