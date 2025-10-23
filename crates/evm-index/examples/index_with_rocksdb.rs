/// Example: Index EVM transactions with RocksDB storage
///
/// This demonstrates production usage with persistent RocksDB storage.
use anyhow::Result;
use evm_index::{EvmTransactionIndexer, CF_TX_BY_FROM, CF_TX_BY_HASH};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_index::{IndexBuilder, IndexableSchema};
use parquet_index_rocksdb::{RocksDbFileRegistry, RocksDbIndexStorage};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("EVM Transaction Indexing with RocksDB");
    println!("======================================\n");

    // Check arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <parquet-file> <rocksdb-path>", args[0]);
        println!("\nExample:");
        println!("  {} ./test-data/transactions.parquet ./indexes", args[0]);
        println!("\nThis will:");
        println!("  1. Create RocksDB database at ./indexes");
        println!("  2. Index the parquet file");
        println!("  3. Store indexes persistently in RocksDB");
        return Ok(());
    }

    let parquet_path = PathBuf::from(&args[1]);
    let rocksdb_path = PathBuf::from(&args[2]);

    println!("Configuration:");
    println!("  Parquet file: {}", parquet_path.display());
    println!("  RocksDB path: {}", rocksdb_path.display());
    println!();

    // Get column families from schema
    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    println!("Creating RocksDB storage with column families:");
    for cf in &column_families {
        println!("  - {}", cf);
    }
    println!();

    // Create RocksDB storage and file registry
    let storage = Arc::new(RocksDbIndexStorage::open(&rocksdb_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&rocksdb_path)?);

    // Create index builder
    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    // Get file info
    let file = File::open(&parquet_path)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = reader_builder.metadata().clone();

    println!("Parquet file info:");
    println!("  Row groups: {}", metadata.num_row_groups());
    println!("  Schema: {}", EvmTransactionIndexer::schema_name());
    println!();

    println!("Index specs:");
    for spec in EvmTransactionIndexer::index_specs() {
        println!("  {} (column {})", spec.column_family, spec.column_index);
    }
    println!();

    // Index the file!
    println!("Building indexes...");
    let file_id = builder.index_file(&parquet_path)?;

    println!("✓ Indexing complete!");
    println!("  File ID: {}", file_id.0);
    println!();

    // Query some stats
    println!(
        "Indexes are now persisted in RocksDB at: {}",
        rocksdb_path.display()
    );
    println!("You can query them by reopening the database and using:");
    println!("  storage.get({:?}, <32-byte-hash>)", CF_TX_BY_HASH);
    println!(
        "  storage.prefix_iterator({:?}, <20-byte-address>)",
        CF_TX_BY_FROM
    );

    Ok(())
}
