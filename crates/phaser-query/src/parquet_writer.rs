use anyhow::Result;
use arrow::array as arrow_array;
use arrow::array::RecordBatch;
use arrow::datatypes as arrow_schema;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tracing::{info, debug, error};

/// Service for writing RecordBatches to Parquet files
pub struct ParquetWriter {
    data_dir: PathBuf,
    current_file: Option<CurrentFile>,
    max_file_size_bytes: u64,
    segment_size: u64,
}

struct CurrentFile {
    writer: ArrowWriter<File>,
    path: PathBuf,
    row_count: usize,
    byte_count: u64,
    start_block: u64,
    end_block: u64,
}

impl ParquetWriter {
    pub fn new(data_dir: PathBuf, max_file_size_mb: u64, segment_size: u64) -> Result<Self> {
        // Create data directory if it doesn't exist
        fs::create_dir_all(&data_dir)?;

        Ok(Self {
            data_dir,
            current_file: None,
            max_file_size_bytes: max_file_size_mb * 1024 * 1024,
            segment_size,
        })
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Extract block number from the batch (assuming _block_num is first column)
        let block_num = if let Some(array) = batch.column(0).as_any().downcast_ref::<arrow_array::UInt64Array>() {
            if array.len() > 0 {
                array.value(0)
            } else {
                return Ok(()); // Skip empty batch
            }
        } else {
            error!("Failed to extract block number from batch");
            return Ok(());
        };

        // Check if we need to start a new file
        if self.should_start_new_file(block_num)? {
            self.finalize_current_file()?;
            self.start_new_file(block_num, batch.schema())?;
        }

        // Initialize file if needed
        if self.current_file.is_none() {
            self.start_new_file(block_num, batch.schema())?;
        }

        // Write the batch
        if let Some(ref mut current) = self.current_file {
            current.writer.write(&batch)?;
            current.row_count += batch.num_rows();
            current.end_block = block_num;

            // Estimate size (this is approximate)
            let batch_size = batch.get_array_memory_size() as u64;
            current.byte_count += batch_size;

            debug!(
                "Wrote batch with {} rows to {}, total rows: {}, block: {}",
                batch.num_rows(),
                current.path.display(),
                current.row_count,
                block_num
            );
        }

        Ok(())
    }

    fn should_start_new_file(&self, block_num: u64) -> Result<bool> {
        if let Some(ref current) = self.current_file {
            // Check segment boundary
            let segment_boundary = (block_num / self.segment_size) != (current.start_block / self.segment_size);

            // Check file size
            let size_exceeded = current.byte_count >= self.max_file_size_bytes;

            Ok(segment_boundary || size_exceeded)
        } else {
            Ok(false)
        }
    }

    fn start_new_file(&mut self, block_num: u64, schema: arrow_schema::SchemaRef) -> Result<()> {
        let segment_id = block_num / self.segment_size;
        let filename = format!(
            "blocks_segment_{:06}_from_{}.parquet",
            segment_id,
            block_num
        );
        let path = self.data_dir.join(filename);

        info!("Starting new parquet file: {}", path.display());

        let file = File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        self.current_file = Some(CurrentFile {
            writer,
            path,
            row_count: 0,
            byte_count: 0,
            start_block: block_num,
            end_block: block_num,
        });

        Ok(())
    }

    pub fn finalize_current_file(&mut self) -> Result<()> {
        if let Some(mut current) = self.current_file.take() {
            current.writer.close()?;

            // Rename file to include end block
            let new_filename = format!(
                "blocks_segment_{:06}_blocks_{}_to_{}.parquet",
                current.start_block / self.segment_size,
                current.start_block,
                current.end_block
            );
            let new_path = self.data_dir.join(new_filename);

            fs::rename(&current.path, &new_path)?;

            info!(
                "Finalized parquet file: {} with {} rows, blocks {} to {}",
                new_path.display(),
                current.row_count,
                current.start_block,
                current.end_block
            );
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut current) = self.current_file {
            current.writer.flush()?;
        }
        Ok(())
    }
}