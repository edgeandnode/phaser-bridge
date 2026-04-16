use anyhow::Result;
use parquet::file::metadata::{KeyValue, ParquetMetaDataBuilder, ParquetMetaDataWriter};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const METADATA_KEY: &str = "phaser.meta";

/// Phaser metadata stored in parquet files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaserMetadata {
    /// Metadata format version
    pub version: u8,
    /// Segment start block (the full segment range, e.g., 0 for segment 0-499999)
    pub segment_start: u64,
    /// Segment end block (the full segment range, e.g., 499999 for segment 0-499999)
    pub segment_end: u64,
    /// File responsibility start (first block this file is responsible for, may be > segment_start)
    pub responsibility_start: u64,
    /// File responsibility end (last block this file is responsible for, may be < segment_end)
    pub responsibility_end: u64,
    /// File actual data start (actual first block with data in this file)
    pub data_start: u64,
    /// File actual data end (actual last block with data in this file)
    pub data_end: u64,
    /// Data type: "blocks", "transactions", or "logs"
    pub data_type: String,
    /// True if written by live streaming worker, false if written by historical sync worker (added in version 2)
    #[serde(default)]
    pub is_live: bool,
}

impl PhaserMetadata {
    pub const VERSION: u8 = 2;

    pub fn new(
        segment_start: u64,
        segment_end: u64,
        responsibility_start: u64,
        responsibility_end: u64,
        data_start: u64,
        data_end: u64,
        data_type: String,
    ) -> Self {
        Self {
            version: Self::VERSION,
            segment_start,
            segment_end,
            responsibility_start,
            responsibility_end,
            data_start,
            data_end,
            data_type,
            is_live: false,
        }
    }

    pub fn with_is_live(mut self, is_live: bool) -> Self {
        self.is_live = is_live;
        self
    }

    /// Encode metadata to bytes using bincode
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Decode metadata from bytes using bincode
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let meta: Self = bincode::deserialize(bytes)?;
        // Support both version 1 (without is_live) and version 2 (with it)
        // The #[serde(default)] on is_live handles missing field gracefully
        if meta.version != 1 && meta.version != Self::VERSION {
            anyhow::bail!("Unsupported metadata version: {}", meta.version);
        }
        Ok(meta)
    }

    /// Convert to parquet KeyValue for writing
    pub fn to_key_value(&self) -> Result<KeyValue> {
        use base64::Engine;
        let encoded = self.encode()?;
        let value = base64::engine::general_purpose::STANDARD.encode(&encoded);
        Ok(KeyValue::new(METADATA_KEY.to_string(), value))
    }

    /// Extract from parquet key-value metadata
    pub fn from_key_value_metadata(kv_metadata: &[KeyValue]) -> Result<Option<Self>> {
        use base64::Engine;
        for kv in kv_metadata {
            if kv.key == METADATA_KEY {
                if let Some(ref value) = kv.value {
                    let decoded = base64::engine::general_purpose::STANDARD.decode(value)?;
                    return Ok(Some(Self::decode(&decoded)?));
                }
            }
        }
        Ok(None)
    }

    /// Update metadata in an existing parquet file using in-place footer mutation
    /// This only modifies the footer, not the data itself
    pub fn update_file_metadata<P: AsRef<Path>>(file_path: P, metadata: &Self) -> Result<()> {
        let file_path = file_path.as_ref();

        // Open file for reading to get current metadata
        let read_file = File::open(file_path)?;
        let reader = SerializedFileReader::new(read_file)?;
        let parquet_metadata = reader.metadata().clone();

        // Get current key-value metadata and update it
        let file_metadata = parquet_metadata.file_metadata();
        let mut existing_kv = file_metadata
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();

        // Remove old phaser.meta if it exists
        existing_kv.retain(|kv| kv.key != METADATA_KEY);

        // Add new metadata
        let new_kv = metadata.to_key_value()?;
        existing_kv.push(new_kv);

        // Create new FileMetaData with updated key-value metadata
        let old_file_metadata = parquet_metadata.file_metadata();
        let new_file_metadata = parquet::file::metadata::FileMetaData::new(
            old_file_metadata.version(),
            old_file_metadata.num_rows(),
            old_file_metadata.created_by().map(|s| s.to_string()),
            Some(existing_kv),
            old_file_metadata.schema_descr_ptr(),
            old_file_metadata.column_orders().cloned(),
        );

        // Build new ParquetMetaData preserving everything except file metadata
        let updated_metadata = ParquetMetaDataBuilder::new(new_file_metadata)
            .set_row_groups(parquet_metadata.row_groups().to_vec())
            .set_column_index(parquet_metadata.column_index().cloned())
            .set_offset_index(parquet_metadata.offset_index().cloned())
            .build();

        // Serialize the new metadata
        let mut footer_bytes = Vec::new();
        ParquetMetaDataWriter::new(&mut footer_bytes, &updated_metadata).finish()?;

        // Open file for writing
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;

        let file_len = file.metadata()?.len();

        // Read old footer length (last 4 bytes before PAR1 magic)
        file.seek(SeekFrom::End(-8))?;
        let mut old_footer_len_bytes = [0u8; 4];
        file.read_exact(&mut old_footer_len_bytes)?;
        let old_footer_len = u32::from_le_bytes(old_footer_len_bytes);

        // Verify PAR1 magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"PAR1" {
            anyhow::bail!("Invalid parquet file: missing PAR1 magic bytes");
        }

        // Calculate where data ends (before old footer)
        let data_end = file_len - (old_footer_len as u64) - 8;

        // Truncate file to data end, then write new footer
        file.set_len(data_end)?;
        file.seek(SeekFrom::End(0))?;

        // Write the new footer (ParquetMetaDataWriter already includes length and magic bytes)
        file.write_all(&footer_bytes)?;
        file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_update_metadata_preserves_row_groups() {
        // Create a test parquet file with multiple row groups
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Write data with small row group size to create multiple groups
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100)) // Small row groups
            .build();

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        // Write 500 rows across multiple batches
        for batch_start in (0..500).step_by(100) {
            let block_nums: Int64Array =
                (batch_start..batch_start + 100).map(|i| i as i64).collect();
            let values: Int64Array = (batch_start..batch_start + 100)
                .map(|i| (i * 10) as i64)
                .collect();

            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(block_nums), Arc::new(values)])
                    .unwrap();

            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        // Verify initial state - should have multiple row groups
        let file = std::fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let initial_row_group_count = reader.metadata().num_row_groups();
        assert!(
            initial_row_group_count > 1,
            "Expected multiple row groups, got {initial_row_group_count}"
        );

        // Update metadata
        let phaser_meta = PhaserMetadata::new(0, 499, 0, 499, 0, 499, "test".to_string());

        PhaserMetadata::update_file_metadata(path, &phaser_meta).unwrap();

        // Verify metadata was written
        let file = std::fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        // Check row group count is preserved
        assert_eq!(
            metadata.num_row_groups(),
            initial_row_group_count,
            "Row group count changed after metadata update"
        );

        // Check phaser metadata is present
        let kv_metadata = metadata.file_metadata().key_value_metadata().unwrap();
        let parsed_meta = PhaserMetadata::from_key_value_metadata(kv_metadata).unwrap();
        assert!(parsed_meta.is_some(), "Phaser metadata not found");

        let parsed_meta = parsed_meta.unwrap();
        assert_eq!(parsed_meta.segment_start, 0);
        assert_eq!(parsed_meta.segment_end, 499);
        assert_eq!(parsed_meta.data_type, "test");

        // Verify file is still readable
        assert_eq!(metadata.file_metadata().num_rows(), 500);
    }

    #[test]
    fn test_corrupted_footer_fails_safely() {
        // Create a valid parquet file
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let ids: Int64Array = (0..100).map(|i| i as i64).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Corrupt the footer by truncating the file
        let original_len = std::fs::metadata(path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();

        // Truncate 20 bytes off the end (into the footer)
        file.set_len(original_len - 20).unwrap();
        drop(file);

        // Attempt to update metadata should fail
        let phaser_meta = PhaserMetadata::new(0, 99, 0, 99, 0, 99, "test".to_string());

        let result = PhaserMetadata::update_file_metadata(path, &phaser_meta);
        assert!(
            result.is_err(),
            "Expected error when reading corrupted footer"
        );
    }

    #[test]
    fn test_corrupted_magic_bytes_fails() {
        // Create a valid parquet file
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let ids: Int64Array = (0..100).map(|i| i as i64).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Corrupt the trailing magic bytes
        let file_len = std::fs::metadata(path).unwrap().len();
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();

        file.seek(SeekFrom::Start(file_len - 4)).unwrap();
        file.write_all(b"XXXX").unwrap();
        drop(file);

        // Attempt to update metadata should fail with magic bytes error
        let phaser_meta = PhaserMetadata::new(0, 99, 0, 99, 0, 99, "test".to_string());

        let result = PhaserMetadata::update_file_metadata(path, &phaser_meta);
        assert!(result.is_err(), "Expected error for corrupted magic bytes");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("PAR1")
                || err_msg.contains("magic")
                || err_msg.contains("Corrupt footer"),
            "Expected error about corrupted footer, got: {err_msg}"
        );
    }
}
