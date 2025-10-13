use crate::{ColumnOptions, ParquetConfig};
use anyhow::Result;
use arrow::array as arrow_array;
use arrow::array::RecordBatch;
use arrow::datatypes as arrow_schema;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder},
};
use std::fs::{self, File};
use std::path::PathBuf;
use tracing::{debug, error, info};

/// Service for writing RecordBatches to Parquet files
pub struct ParquetWriter {
    data_dir: PathBuf,
    current_file: Option<CurrentFile>,
    max_file_size_bytes: u64,
    segment_size: u64,
    data_type: String, // "blocks", "transactions", or "logs"
    parquet_config: Option<ParquetConfig>,
    is_live: bool, // true for live streaming, false for historical sync
}

struct CurrentFile {
    writer: ArrowWriter<File>,
    temp_path: PathBuf,
    row_count: usize,
    start_block: u64,
    end_block: u64,
}

impl ParquetWriter {
    pub fn new(
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
        data_type: String,
    ) -> Result<Self> {
        Self::with_config(data_dir, max_file_size_mb, segment_size, data_type, None)
    }

    pub fn with_config(
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
        data_type: String,
        parquet_config: Option<ParquetConfig>,
    ) -> Result<Self> {
        Self::with_config_and_mode(
            data_dir,
            max_file_size_mb,
            segment_size,
            data_type,
            parquet_config,
            false, // historical sync by default
        )
    }

    pub fn with_config_and_mode(
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
        data_type: String,
        parquet_config: Option<ParquetConfig>,
        is_live: bool,
    ) -> Result<Self> {
        // Create data directory if it doesn't exist
        fs::create_dir_all(&data_dir)?;

        Ok(Self {
            data_dir,
            current_file: None,
            max_file_size_bytes: max_file_size_mb * 1024 * 1024,
            segment_size,
            data_type,
            parquet_config,
            is_live,
        })
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Extract first and last block numbers from the batch (assuming _block_num is first column)
        let (first_block, last_block) = if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>(
        ) {
            if !array.is_empty() {
                let first = array.value(0);
                let last = array.value(array.len() - 1);
                (first, last)
            } else {
                return Ok(()); // Skip empty batch
            }
        } else {
            error!("Failed to extract block number from batch");
            return Ok(());
        };

        // Check if we need to start a new file (use first block for boundary check)
        if self.should_start_new_file(first_block)? {
            self.finalize_current_file()?;
            self.start_new_file(first_block, batch.schema())?;
        }

        // Initialize file if needed
        if self.current_file.is_none() {
            self.start_new_file(first_block, batch.schema())?;
        }

        // Write the batch
        if let Some(ref mut current) = self.current_file {
            current.writer.write(&batch)?;
            current.row_count += batch.num_rows();
            current.end_block = last_block; // Track the last block we've written

            debug!(
                "Wrote batch with {} rows to {}, total rows: {}, blocks: {}-{}",
                batch.num_rows(),
                current.temp_path.display(),
                current.row_count,
                first_block,
                last_block
            );
        }

        Ok(())
    }

    fn should_start_new_file(&self, block_num: u64) -> Result<bool> {
        if let Some(ref current) = self.current_file {
            // Check segment boundary
            let segment_boundary =
                (block_num / self.segment_size) != (current.start_block / self.segment_size);

            // Check actual file size on disk
            let actual_size = fs::metadata(&current.temp_path)?.len();
            let size_exceeded = actual_size >= self.max_file_size_bytes;

            Ok(segment_boundary || size_exceeded)
        } else {
            Ok(false)
        }
    }

    fn start_new_file(&mut self, block_num: u64, schema: arrow_schema::SchemaRef) -> Result<()> {
        // Create temporary filename with start block - will be renamed with actual range when finalized
        // Format for historical: {data_type}_from_{start}_{timestamp}.parquet.tmp
        // Format for live: live_{data_type}_from_{start}_{timestamp}.tmp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = if self.is_live {
            format!(
                "live_{}_from_{}_{}.tmp",
                self.data_type, block_num, timestamp
            )
        } else {
            format!(
                "{}_from_{}_{}.parquet.tmp",
                self.data_type, block_num, timestamp
            )
        };
        let temp_path = self.data_dir.join(filename);

        info!(
            "Starting new {} parquet file: {}",
            self.data_type,
            temp_path.display()
        );

        let file = File::create(&temp_path)?;
        let props = self.build_writer_properties(&schema)?;

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        self.current_file = Some(CurrentFile {
            writer,
            temp_path,
            row_count: 0,
            start_block: block_num,
            end_block: block_num,
        });

        Ok(())
    }

    fn build_writer_properties(
        &self,
        schema: &arrow_schema::SchemaRef,
    ) -> Result<WriterProperties> {
        let mut builder = WriterProperties::builder();

        // Apply config if available
        if let Some(config) = &self.parquet_config {
            // Set default compression
            let default_compression = parse_compression(&config.default_compression);
            builder = builder.set_compression(default_compression);

            // Set row group size
            builder = builder.set_max_row_group_size(config.row_group_size_mb * 1024 * 1024);

            // Apply per-column options
            for field in schema.fields() {
                let col_name = field.name();
                if let Some(col_opts) = config.column_options.get(col_name) {
                    builder = self.apply_column_options(builder, col_name, col_opts);
                }
            }
        } else {
            // Default to SNAPPY if no config
            builder = builder.set_compression(Compression::SNAPPY);
        }

        // Always enable statistics for the block number column (_block_num)
        // This allows us to query min/max block ranges from parquet metadata
        // without reading the entire file
        builder =
            builder.set_column_statistics_enabled("_block_num".into(), EnabledStatistics::Page);

        Ok(builder.build())
    }

    fn apply_column_options(
        &self,
        mut builder: WriterPropertiesBuilder,
        col_name: &str,
        opts: &ColumnOptions,
    ) -> WriterPropertiesBuilder {
        // Set compression
        if let Some(compression_str) = &opts.compression {
            let compression = parse_compression(compression_str);
            builder = builder.set_column_compression(col_name.into(), compression);
        }

        // Set encoding
        if let Some(encoding_str) = &opts.encoding {
            let encoding = parse_encoding(encoding_str);
            builder = builder.set_column_encoding(col_name.into(), encoding);
        }

        // Set bloom filter
        if let Some(true) = opts.bloom_filter {
            builder = builder.set_column_bloom_filter_enabled(col_name.into(), true);
        }

        // Set statistics
        if let Some(stats_str) = &opts.statistics {
            let stats = parse_statistics(stats_str);
            builder = builder.set_column_statistics_enabled(col_name.into(), stats);
        }

        // Set dictionary
        if let Some(enable_dict) = opts.dictionary {
            builder = builder.set_column_dictionary_enabled(col_name.into(), enable_dict);
        }

        builder
    }

    pub fn finalize_current_file(&mut self) -> Result<()> {
        if let Some(current) = self.current_file.take() {
            current.writer.close()?;

            // Build final filename with actual block range
            // Format: {data_type}_from_{start}_to_{end}.parquet
            let final_filename = format!(
                "{}_from_{}_to_{}.parquet",
                self.data_type, current.start_block, current.end_block
            );
            let final_path = self.data_dir.join(final_filename);

            // Rename from .tmp to final name
            fs::rename(&current.temp_path, &final_path)?;

            info!(
                "Finalized parquet file: {} with {} rows, blocks {} to {}",
                final_path.display(),
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

    /// Write an empty parquet file for a range with no data
    /// This marks the range as "checked" so it won't be re-synced
    pub fn write_empty_range(
        &mut self,
        schema: arrow_schema::SchemaRef,
        start_block: u64,
        end_block: u64,
    ) -> Result<()> {
        // Create temporary filename
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!(
            "{}_from_{}_{}.parquet.tmp",
            self.data_type, start_block, timestamp
        );
        let temp_path = self.data_dir.join(filename);

        info!(
            "No data received for {} blocks {}-{}, creating empty parquet file",
            self.data_type, start_block, end_block
        );

        let file = File::create(&temp_path)?;
        let props = self.build_writer_properties(&schema)?;
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        // Close the writer without writing any rows
        writer.close()?;

        // Build final filename with the range
        let final_filename = format!(
            "{}_from_{}_to_{}.parquet",
            self.data_type, start_block, end_block
        );
        let final_path = self.data_dir.join(final_filename);

        // Rename from .tmp to final name
        fs::rename(&temp_path, &final_path)?;

        info!(
            "Created empty parquet file: {} for blocks {}-{} (no data in range)",
            final_path.display(),
            start_block,
            end_block
        );

        Ok(())
    }
}

fn parse_compression(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "lzo" => Compression::LZO,
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4" => Compression::LZ4,
        "zstd" => Compression::ZSTD(Default::default()),
        "lz4_raw" => Compression::LZ4_RAW,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => {
            info!("Unknown compression '{}', defaulting to ZSTD", s);
            Compression::ZSTD(Default::default())
        }
    }
}

fn parse_encoding(s: &str) -> Encoding {
    match s.to_lowercase().as_str() {
        "plain" => Encoding::PLAIN,
        "rle" => Encoding::RLE,
        "delta_binary_packed" => Encoding::DELTA_BINARY_PACKED,
        "delta_length_byte_array" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        "delta_byte_array" => Encoding::DELTA_BYTE_ARRAY,
        "rle_dictionary" => Encoding::RLE_DICTIONARY,
        "byte_stream_split" => Encoding::BYTE_STREAM_SPLIT,
        _ => {
            info!("Unknown encoding '{}', defaulting to PLAIN", s);
            Encoding::PLAIN
        }
    }
}

fn parse_statistics(s: &str) -> EnabledStatistics {
    match s.to_lowercase().as_str() {
        "none" => EnabledStatistics::None,
        "chunk" => EnabledStatistics::Chunk,
        "page" => EnabledStatistics::Page,
        _ => {
            info!("Unknown statistics level '{}', defaulting to Page", s);
            EnabledStatistics::Page
        }
    }
}
