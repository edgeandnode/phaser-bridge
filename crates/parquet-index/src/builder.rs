/// Index building from parquet files
///
/// The IndexBuilder reads parquet files, uses the IndexableSchema to extract
/// keys from typed-arrow Record views, and writes PagePointers to the IndexStorage.
use crate::storage::{FileRegistry, IndexStorage, WriteBatch};
use crate::BuilderError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet_index_schema::{FileId, IndexError, IndexableSchema, PagePointer};
use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

/// Builds indexes from parquet files
///
/// Generic over:
/// - S: IndexableSchema - defines what to index
/// - ST: IndexStorage - where to write indexes
/// - FR: FileRegistry - maps FileId to paths
pub struct IndexBuilder<S: IndexableSchema, ST: IndexStorage, FR: FileRegistry> {
    _schema: PhantomData<S>,
    storage: Arc<ST>,
    file_registry: Arc<FR>,
}

impl<S: IndexableSchema, ST: IndexStorage, FR: FileRegistry> IndexBuilder<S, ST, FR> {
    pub fn new(storage: Arc<ST>, file_registry: Arc<FR>) -> Self {
        Self {
            _schema: PhantomData,
            storage,
            file_registry,
        }
    }

    /// Build indexes for a single parquet file
    ///
    /// Steps:
    /// 1. Register file and get FileId
    /// 2. Open parquet file
    /// 3. For each row group:
    ///    - Read RecordBatch
    ///    - Create typed-arrow views (zero-copy)
    ///    - For each record view:
    ///      - For each IndexSpec:
    ///        - Extract key using KeyExtractor
    ///        - Find which page the row is in (using OffsetIndex)
    ///        - Build PagePointer
    ///        - Write to IndexStorage
    pub fn index_file(&self, path: &Path) -> Result<FileId, BuilderError> {
        info!(path = ?path, schema = S::schema_name(), "Building indexes for parquet file");

        // 1. Register file and get FileId
        let file_id = self.file_registry.register_file(path)?;

        // 2. Open parquet file and get metadata
        let file = File::open(path).map_err(|source| BuilderError::FileOpen {
            path: path.to_path_buf(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().clone();
        let mut reader = builder.build()?;

        info!(
            file_id = file_id.0,
            row_groups = metadata.num_row_groups(),
            "Indexing parquet file"
        );

        // 3. For each row group, read and index
        for row_group_idx in 0..metadata.num_row_groups() {
            // Read the next RecordBatch (corresponds to one row group)
            if let Some(batch) = reader.next() {
                let batch = batch?;

                debug!(
                    file_id = file_id.0,
                    row_group = row_group_idx,
                    rows = batch.num_rows(),
                    "Indexing row group"
                );

                self.index_row_group(file_id, row_group_idx, batch, &metadata)?;
            }
        }

        info!(
            file_id = file_id.0,
            path = ?path,
            "Completed indexing parquet file"
        );

        Ok(file_id)
    }

    /// Index a single row group
    fn index_row_group(
        &self,
        file_id: FileId,
        row_group: usize,
        batch: RecordBatch,
        metadata: &ParquetMetaData,
    ) -> Result<(), BuilderError> {
        debug!(
            file_id = file_id.0,
            row_group,
            rows = batch.num_rows(),
            "Indexing row group"
        );

        // Get offset index for this row group (if available)
        let offset_index = metadata.offset_index().and_then(|idx| idx.get(row_group));

        let mut write_batch = WriteBatch::new();
        let specs = S::index_specs();

        // Reusable buffer for composite keys (one allocation per batch instead of per row!)
        let mut key_buffer = Vec::with_capacity(64);

        // For each row in the batch
        for row_idx in 0..batch.num_rows() {
            // For each index specification
            for spec in &specs {
                // Use the column specified in the IndexSpec
                let column_chunk = spec.column_index;

                let (page_index, row_in_page) = if let Some(offset_idx) = offset_index {
                    // Use OffsetIndex to find the exact page
                    if let Some(column_offset) = offset_idx.get(column_chunk) {
                        find_page_for_row(row_idx, column_offset)?
                    } else {
                        // Column doesn't have offset index - use row group level
                        (0, row_idx as u16)
                    }
                } else {
                    // No offset index - use row group level
                    (0, row_idx as u16)
                };

                // Build page pointer
                let pointer = PagePointer {
                    file_id,
                    row_group: row_group as u16,
                    column_chunk: column_chunk as u16,
                    page_index,
                    row_in_page,
                };

                // Extract key into buffer (handles both zero-copy and composite keys)
                if spec
                    .key_extractor
                    .extract_into(&batch, row_idx, &mut key_buffer)
                {
                    write_batch.put(
                        &spec.column_family,
                        key_buffer.clone(),
                        pointer.to_bytes().to_vec(),
                    );
                }
            }
        }

        // Write batch atomically
        self.storage.write_batch(write_batch)?;

        Ok(())
    }
}

/// Find which page contains a given row using OffsetIndex
///
/// Returns (page_index, row_in_page)
fn find_page_for_row(
    row_idx: usize,
    offset_index: &OffsetIndexMetaData,
) -> Result<(u16, u16), IndexError> {
    let page_locations = &offset_index.page_locations;

    // Find the page containing this row
    for (page_idx, location) in page_locations.iter().enumerate() {
        let page_start = location.first_row_index as usize;

        // Check if this is the last page or if the next page starts after our row
        let page_end = if page_idx + 1 < page_locations.len() {
            page_locations[page_idx + 1].first_row_index as usize
        } else {
            usize::MAX // Last page extends to end of row group
        };

        if row_idx >= page_start && row_idx < page_end {
            let row_in_page = (row_idx - page_start) as u16;
            return Ok((page_idx as u16, row_in_page));
        }
    }

    Err(IndexError::PageNotFound(row_idx))
}

#[cfg(test)]
mod tests {
}
