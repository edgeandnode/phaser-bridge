/// Page-level reader for indexed parquet files
///
/// Uses PagePointers from indexes to read specific row groups from parquet files.
use crate::{FileRegistry, ReaderError};
use arrow::record_batch::RecordBatch;
use fusio::disk::LocalFs;
use fusio::path::Path;
use fusio::DynFs;
use fusio_parquet::reader::AsyncReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet_index_schema::PagePointer;
use std::sync::Arc;

/// Async reader for fetching specific row groups from parquet files
///
/// Uses fusio for async I/O with efficient byte-range reads.
pub struct PageReader<FR: FileRegistry> {
    file_registry: Arc<FR>,
}

impl<FR: FileRegistry> PageReader<FR> {
    /// Create a new PageReader with the given file registry
    pub fn new(file_registry: Arc<FR>) -> Self {
        Self { file_registry }
    }

    /// Read the row group containing the indexed row (async)
    ///
    /// Returns the entire row group as a RecordBatch.
    pub async fn read_row_group_async(
        &self,
        pointer: &PagePointer,
    ) -> Result<RecordBatch, ReaderError> {
        // Get the file path from the registry
        let file_path = self
            .file_registry
            .get_file_path(pointer.file_id)
            .map_err(|_| ReaderError::FileNotFound(pointer.file_id))?;

        // Open the parquet file using fusio
        let path = Path::from_filesystem_path(
            file_path
                .to_str()
                .ok_or_else(|| ReaderError::InvalidPath(format!("{file_path:?}")))?,
        )
        .map_err(|e| ReaderError::InvalidPath(e.to_string()))?;
        let fs = LocalFs {};
        let file = fs
            .open_options(&path, fusio::fs::OpenOptions::default())
            .await
            .map_err(|e| ReaderError::FileOpen {
                path: file_path.clone(),
                source: e,
            })?;

        // Get file size for AsyncReader
        let content_length = file.size().await?;

        // Create async parquet reader
        let async_reader = AsyncReader::new(Box::new(file), content_length).await?;

        // Build the stream reader for the specific row group
        let builder = ParquetRecordBatchStreamBuilder::new(async_reader).await?;

        let mut stream = builder
            .with_row_groups(vec![pointer.row_group as usize])
            .build()?;

        // Read all batches from the row group and concatenate
        use futures::StreamExt;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            batches.push(batch_result?);
        }

        if batches.is_empty() {
            return Err(ReaderError::ParquetRead(format!(
                "No data found in row group {} of file {:?}",
                pointer.row_group, file_path
            )));
        }

        // If there's only one batch, return it
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            // Concatenate multiple batches
            use arrow::compute::concat_batches;
            let schema = batches[0].schema();
            concat_batches(&schema, &batches).map_err(Into::into)
        }
    }

    /// Read multiple row groups efficiently (async)
    ///
    /// Returns batches in the same order as input pointers.
    pub async fn read_row_groups_async(
        &self,
        pointers: &[PagePointer],
    ) -> Result<Vec<RecordBatch>, ReaderError> {
        // TODO: Optimize by grouping pointers by file_id to reuse file handles
        let mut batches = Vec::new();
        for pointer in pointers {
            batches.push(self.read_row_group_async(pointer).await?);
        }
        Ok(batches)
    }

    /// Synchronous wrapper for read_row_group_async
    ///
    /// Uses tokio runtime to run the async version.
    pub fn read_row_group(&self, pointer: &PagePointer) -> Result<RecordBatch, ReaderError> {
        tokio::runtime::Handle::current().block_on(self.read_row_group_async(pointer))
    }

    /// Synchronous wrapper for read_row_groups_async
    pub fn read_row_groups(
        &self,
        pointers: &[PagePointer],
    ) -> Result<Vec<RecordBatch>, ReaderError> {
        tokio::runtime::Handle::current().block_on(self.read_row_groups_async(pointers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileRegistry, StorageError};
    use parquet_index_schema::FileId;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    /// Mock FileRegistry for testing
    struct MockFileRegistry {
        files: Mutex<HashMap<FileId, PathBuf>>,
    }

    impl MockFileRegistry {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
            }
        }

        #[allow(dead_code)]
        fn add_file(&self, file_id: FileId, path: PathBuf) {
            self.files.lock().unwrap().insert(file_id, path);
        }
    }

    impl FileRegistry for MockFileRegistry {
        fn register_file(&self, path: &Path) -> Result<FileId, StorageError> {
            let id = FileId(self.files.lock().unwrap().len() as u32);
            self.files.lock().unwrap().insert(id, path.to_path_buf());
            Ok(id)
        }

        fn get_file_path(&self, file_id: FileId) -> Result<PathBuf, StorageError> {
            self.files
                .lock()
                .unwrap()
                .get(&file_id)
                .cloned()
                .ok_or_else(|| StorageError::FileNotFound(file_id))
        }
    }

    #[test]
    fn test_page_reader_basic() {
        // PageReader can be constructed
        let registry = Arc::new(MockFileRegistry::new());
        let _reader = PageReader::new(registry);
    }
}
