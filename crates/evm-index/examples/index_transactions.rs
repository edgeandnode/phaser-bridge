/// Example: Index EVM transactions from a parquet file
///
/// This demonstrates how to use the indexing system to build page-level
/// indexes for transaction lookups.
use anyhow::Result;
use evm_index::EvmTransactionIndexer;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_index::{FileRegistry, IndexBuilder, IndexStorage, WriteBatch, WriteOp};
use parquet_index_schema::{FileId, IndexableSchema};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Simple in-memory storage for this example
struct MemoryStorage {
    data: Mutex<HashMap<(String, Vec<u8>), Vec<u8>>>,
}

impl MemoryStorage {
    fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
    fn get_all(&self, cf: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.data.lock().unwrap();
        data.iter()
            .filter(|((column_family, _), _)| column_family == cf)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }
}

/// Simple in-memory file registry for this example
struct MemoryFileRegistry {
    files: Mutex<HashMap<PathBuf, FileId>>,
    next_id: Mutex<u32>,
}

impl MemoryFileRegistry {
    fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
        }
    }
}

impl FileRegistry for MemoryFileRegistry {
    fn register_file(&self, path: &Path) -> Result<FileId> {
        let mut files = self.files.lock().unwrap();

        // Return existing ID if file already registered
        if let Some(file_id) = files.get(path) {
            return Ok(*file_id);
        }

        // Assign new ID
        let mut next_id = self.next_id.lock().unwrap();
        let file_id = FileId(*next_id);
        *next_id += 1;

        files.insert(path.to_path_buf(), file_id);
        Ok(file_id)
    }

    fn get_file_path(&self, file_id: FileId) -> Result<PathBuf> {
        let files = self.files.lock().unwrap();
        files
            .iter()
            .find(|(_, id)| **id == file_id)
            .map(|(path, _)| path.clone())
            .ok_or_else(|| anyhow::anyhow!("File not found: {:?}", file_id))
    }
}

impl IndexStorage for MemoryStorage {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        Ok(data.get(&(cf.to_string(), key.to_vec())).cloned())
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.insert((cf.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        for op in batch.operations {
            match op {
                WriteOp::Put { cf, key, value } => {
                    data.insert((cf, key), value);
                }
                WriteOp::Delete { cf, key } => {
                    data.remove(&(cf, key));
                }
            }
        }
        Ok(())
    }

    fn prefix_iterator<'a>(
        &'a self,
        cf: &str,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a> {
        let prefix = prefix.to_vec();
        let cf = cf.to_string();
        let items: Vec<_> = self
            .data
            .lock()
            .unwrap()
            .iter()
            .filter(move |((column_family, key), _)| {
                column_family == &cf && key.starts_with(&prefix)
            })
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();
        Box::new(items.into_iter())
    }
}

fn main() -> Result<()> {
    // This is a minimal example showing the indexing API
    // In production, you'd:
    // 1. Use RocksDB instead of MemoryStorage
    // 2. Read from actual parquet files
    // 3. Integrate with ParquetWriter to index as files are finalized

    println!("EVM Transaction Indexing Example");
    println!("=================================\n");

    // Check if a parquet file path was provided
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} <path-to-transactions.parquet>", args[0]);
        println!("\nThis example demonstrates indexing a parquet file.");
        println!("The indexes would be:");
        for spec in EvmTransactionIndexer::index_specs() {
            println!("  - {}: column {}", spec.column_family, spec.column_index);
        }
        return Ok(());
    }

    let parquet_path = PathBuf::from(&args[1]);
    println!("Indexing file: {}", parquet_path.display());

    // Create in-memory storage and file registry
    let storage = Arc::new(MemoryStorage::new());
    let file_registry = Arc::new(MemoryFileRegistry::new());

    // Create index builder
    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    // Open the parquet file
    let file = File::open(&parquet_path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = reader_builder.metadata().clone();
    let _reader = reader_builder.build()?;

    println!("\nParquet file info:");
    println!("  Row groups: {}", metadata.num_row_groups());
    println!("  Schema: {}", EvmTransactionIndexer::schema_name());
    println!();

    println!("Index specs:");
    for spec in EvmTransactionIndexer::index_specs() {
        println!("  {} (column {})", spec.column_family, spec.column_index,);
    }

    println!("\n Building indexes...");

    // Index the file!
    let file_id = builder.index_file(&parquet_path)?;

    println!("✓ Indexing complete!");
    println!("  File ID: {}", file_id.0);

    // Show some stats
    let tx_by_hash_count = storage.get_all("tx_by_hash").len();
    let tx_by_from_count = storage.get_all("tx_by_from").len();
    let tx_by_to_count = storage.get_all("tx_by_to").len();

    println!("\nIndex statistics:");
    println!("  tx_by_hash entries: {}", tx_by_hash_count);
    println!("  tx_by_from entries: {}", tx_by_from_count);
    println!("  tx_by_to entries: {}", tx_by_to_count);

    Ok(())
}
