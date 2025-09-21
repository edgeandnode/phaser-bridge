use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rocksdb::{DB, WriteBatch};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::index::{BlockPointer, FileInfo};

/// Configurable flush policy for Parquet writer
#[derive(Debug, Clone)]
pub struct FlushPolicy {
    /// Maximum number of rows before forcing flush to disk
    pub max_unflushed_rows: usize,
    /// Maximum time before forcing flush to disk
    pub max_unflushed_duration: Duration,
    /// Row group size for Parquet files
    pub row_group_size: usize,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            max_unflushed_rows: 100,
            max_unflushed_duration: Duration::from_secs(10),
            row_group_size: 10_000,
        }
    }
}

/// Dual-write streaming writer with configurable flush
/// Writes to both temp CF (for immediate queries) and Parquet (for long-term storage)
pub struct DualWriteStreamingWriter {
    db: Arc<DB>,
    temp_cf: String,
    index_cf: String,
    target_dir: PathBuf,
    flush_policy: FlushPolicy,
    schema: Arc<Schema>,
    current_writer: Option<ActiveWriter>,
    file_counter: u64,
}

struct ActiveWriter {
    writer: ArrowWriter<File>,
    temp_path: PathBuf,
    final_path: PathBuf,
    start_block: u64,
    end_block: u64,
    row_count: usize,
    unflushed_rows: usize,
    last_flush: Instant,
    block_numbers: Vec<u64>,  // Track all blocks in this file
}

impl DualWriteStreamingWriter {
    pub fn new(
        db: Arc<DB>,
        temp_cf_name: &str,
        index_cf_name: &str,
        target_dir: PathBuf,
        flush_policy: FlushPolicy,
        schema: Arc<Schema>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&target_dir)?;

        Ok(Self {
            db,
            temp_cf: temp_cf_name.to_string(),
            index_cf: index_cf_name.to_string(),
            target_dir,
            flush_policy,
            schema,
            current_writer: None,
            file_counter: 0,
        })
    }

    /// Write a block with dual-write strategy
    pub async fn write_block(&mut self, block_num: u64, batch: RecordBatch) -> Result<()> {
        // 1. Write to temp CF for immediate queryability
        self.write_to_temp_cf(block_num, &batch)?;

        // 2. Write to Parquet file (accumulating)
        self.write_to_parquet(block_num, batch).await?;

        // 3. Check if we need to rotate the file
        if self.should_rotate(block_num)? {
            self.rotate_file().await?;
        }

        Ok(())
    }

    /// Write to temporary column family for immediate queries
    fn write_to_temp_cf(&self, block_num: u64, batch: &RecordBatch) -> Result<()> {
        let cf = self
            .db
            .cf_handle(&self.temp_cf)
            .ok_or_else(|| anyhow::anyhow!("{} CF not found", self.temp_cf))?;

        // Serialize using Arrow IPC
        use arrow::ipc::writer::StreamWriter;
        use std::io::Cursor;

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &self.schema)?;
            writer.write(batch)?;
            writer.finish()?;
        }

        self.db.put_cf(cf, block_num.to_be_bytes(), buffer.into_inner())?;
        debug!("Wrote block {} to temp CF", block_num);

        Ok(())
    }

    /// Write to Parquet file (accumulating with periodic flushes)
    async fn write_to_parquet(&mut self, block_num: u64, batch: RecordBatch) -> Result<()> {
        // Initialize writer if needed
        if self.current_writer.is_none() {
            self.start_new_file(block_num)?;
        }

        if let Some(writer) = &mut self.current_writer {
            // Write the batch
            writer.writer.write(&batch)?;
            writer.row_count += batch.num_rows();
            writer.unflushed_rows += batch.num_rows();
            writer.end_block = block_num;
            writer.block_numbers.push(block_num);

            debug!(
                "Wrote block {} to Parquet (unflushed: {} rows)",
                block_num, writer.unflushed_rows
            );

            // Check if we should flush to disk
            let should_flush = writer.unflushed_rows >= self.flush_policy.max_unflushed_rows
                || writer.last_flush.elapsed() >= self.flush_policy.max_unflushed_duration;

            if should_flush {
                info!("Flushing {} unflushed rows to disk", writer.unflushed_rows);
                writer.writer.flush()?;
                writer.unflushed_rows = 0;
                writer.last_flush = Instant::now();
            }
        }

        Ok(())
    }

    /// Check if we should rotate to a new file
    fn should_rotate(&self, block_num: u64) -> Result<bool> {
        if let Some(writer) = &self.current_writer {
            // Check segment boundary
            let segment_size = 500_000; // TODO: Make configurable
            let current_segment = writer.start_block / segment_size;
            let new_segment = block_num / segment_size;

            // Check row count
            let should_rotate = current_segment != new_segment
                || writer.row_count >= self.flush_policy.row_group_size;

            Ok(should_rotate)
        } else {
            Ok(false)
        }
    }

    /// Start a new Parquet file
    fn start_new_file(&mut self, block_num: u64) -> Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        self.file_counter += 1;

        let filename = format!(
            "blocks_{}_{:08}_{}.parquet",
            timestamp, self.file_counter, block_num
        );

        let temp_filename = format!(
            "blocks_{}_{:08}_{}.tmp",
            timestamp, self.file_counter, block_num
        );

        let final_path = self.target_dir.join(&filename);
        let temp_path = self.target_dir.join(&temp_filename);

        info!("Starting new Parquet file: {}", temp_path.display());

        let file = File::create(&temp_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_max_row_group_size(self.flush_policy.row_group_size)
            .build();

        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;

        self.current_writer = Some(ActiveWriter {
            writer,
            temp_path,
            final_path,
            start_block: block_num,
            end_block: block_num,
            row_count: 0,
            unflushed_rows: 0,
            last_flush: Instant::now(),
            block_numbers: Vec::new(),
        });

        Ok(())
    }

    /// Rotate the current file: finalize, rename, index, and cleanup
    async fn rotate_file(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            info!(
                "Rotating file with {} rows, blocks {} to {}",
                writer.row_count, writer.start_block, writer.end_block
            );

            // 1. Close the Parquet writer
            writer.writer.close()?;

            // 2. Rename .tmp to .parquet
            let final_filename = format!(
                "blocks_{:08}_to_{:08}.parquet",
                writer.start_block, writer.end_block
            );
            let final_path = self.target_dir.join(&final_filename);

            std::fs::rename(&writer.temp_path, &final_path)?;
            info!("Renamed {} to {}", writer.temp_path.display(), final_path.display());

            // 3. Create atomic batch for index update and temp CF cleanup
            let mut batch = WriteBatch::default();

            let index_cf = self
                .db
                .cf_handle(&self.index_cf)
                .ok_or_else(|| anyhow::anyhow!("{} CF not found", self.index_cf))?;

            let temp_cf = self
                .db
                .cf_handle(&self.temp_cf)
                .ok_or_else(|| anyhow::anyhow!("{} CF not found", self.temp_cf))?;

            // Add file info
            let file_info = FileInfo {
                file_path: final_path.to_string_lossy().to_string(),
                block_start: writer.start_block,
                block_end: writer.end_block,
                row_count: writer.row_count as u32,
            };

            let file_key = format!("file_{:016x}_{:016x}", writer.start_block, writer.end_block);
            batch.put_cf(index_cf, file_key.as_bytes(), bincode::serialize(&file_info)?);

            // Add index entries and remove from temp CF
            for (i, block_num) in writer.block_numbers.iter().enumerate() {
                let pointer = BlockPointer {
                    row_group: 0,  // We're using single row group per rotation for now
                    row_offset: i as u32,
                };

                // Add to index
                batch.put_cf(index_cf, block_num.to_be_bytes(), pointer.to_bytes());

                // Remove from temp CF
                batch.delete_cf(temp_cf, block_num.to_be_bytes());
            }

            // 4. Atomic commit
            self.db.write(batch)?;

            info!(
                "Successfully indexed {} blocks and cleaned temp CF",
                writer.block_numbers.len()
            );
        }

        Ok(())
    }

    /// Force flush current writer (for shutdown)
    pub async fn force_flush(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.current_writer {
            if writer.unflushed_rows > 0 {
                info!("Force flushing {} unflushed rows", writer.unflushed_rows);
                writer.writer.flush()?;
                writer.unflushed_rows = 0;
                writer.last_flush = Instant::now();
            }
        }
        Ok(())
    }

    /// Finalize and rotate any active file (for shutdown)
    pub async fn finalize(&mut self) -> Result<()> {
        if self.current_writer.is_some() {
            info!("Finalizing active Parquet file");
            self.rotate_file().await?;
        }
        Ok(())
    }

    /// Get statistics about the writer state
    pub fn get_stats(&self) -> WriterStats {
        let temp_cf_count = self
            .db
            .cf_handle(&self.temp_cf)
            .and_then(|cf| {
                let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
                Some(iter.count())
            })
            .unwrap_or(0);

        let current_file_stats = self.current_writer.as_ref().map(|w| FileStats {
            path: w.temp_path.to_string_lossy().to_string(),
            row_count: w.row_count,
            unflushed_rows: w.unflushed_rows,
            start_block: w.start_block,
            end_block: w.end_block,
        });

        WriterStats {
            temp_cf_count,
            current_file: current_file_stats,
        }
    }
}

#[derive(Debug)]
pub struct WriterStats {
    pub temp_cf_count: usize,
    pub current_file: Option<FileStats>,
}

#[derive(Debug)]
pub struct FileStats {
    pub path: String,
    pub row_count: usize,
    pub unflushed_rows: usize,
    pub start_block: u64,
    pub end_block: u64,
}