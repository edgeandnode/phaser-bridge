# Parquet Index

Generic parquet indexing engine for building page-level indexes.

## Overview

This crate provides a pluggable system for building secondary indexes on parquet files. It uses Parquet's Page Index (OffsetIndex) to create precise pointers to individual rows within pages, enabling efficient lookups without scanning entire row groups.

## Architecture

The indexing system consists of three crates:

```
parquet-index-schema  ← Core traits (schema-agnostic)
       ↑
parquet-index         ← Generic indexing engine
       ↑
evm-index             ← EVM-specific implementation
```

### Core Concepts

- **PagePointer**: 12-byte structure pointing to exact page location:
  - `file_id` (4 bytes)
  - `row_group` (2 bytes)
  - `column_chunk` (2 bytes)
  - `page_index` (2 bytes)
  - `row_in_page` (2 bytes)

- **IndexableSchema**: Trait defining what to index for a schema
- **KeyExtractor**: Zero-copy key extraction with two modes:
  - `extract_ref()` - Returns `&[u8]` for simple keys (tx_hash)
  - `extract_into()` - Writes to reusable buffer for composite keys (address || block_num)

- **IndexStorage**: Abstract over RocksDB/LMDB/etc

## Usage

See `evm-index/examples/index_transactions.rs` for a complete working example.

```rust
use evm_index::EvmTransactionIndexer;
use parquet_index::{IndexBuilder, FileRegistry, IndexStorage};

// Create storage backend (RocksDB in production)
let storage = Arc::new(RocksDbStorage::new("./indexes"));
let file_registry = Arc::new(RocksDbFileRegistry::new("./indexes"));

// Create index builder
let builder = IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage, file_registry);

// Index a parquet file
let file_id = builder.index_file("transactions_0_to_1000000.parquet")?;
```

The builder will:
1. Register the file and assign a FileId
2. Open the parquet file and read metadata
3. For each row group:
   - Read the RecordBatch
   - Extract keys using zero-copy or composite extraction
   - Find the exact page for each row using OffsetIndex
   - Build PagePointers
   - Write to storage atomically

### Index Specs (EVM Transactions)

The `evm-index` crate provides three indexes:

1. **tx_by_hash**: Direct hash lookup (zero-copy)
   - Key: 32-byte transaction hash
   - Column: 5 (tx_hash)

2. **tx_by_from**: Address → transactions (composite)
   - Key: 20-byte address || 8-byte block number
   - Columns: 6 (from), 2 (block_num)

3. **tx_by_to**: Recipient → transactions (composite)
   - Key: 20-byte address || 8-byte block number
   - Columns: 7 (to), 2 (block_num)
   - Skips contract creation txs (to = None)

## Integration with phaser-query

### Current State

The parquet writer finalizes files in `ParquetWriter::finalize_current_file()`:

```rust
// crates/phaser-query/src/parquet_writer.rs:291
fs::rename(&current.temp_path, &final_path)?;
```

### Planned Integration

Option 1: **Event-based (Recommended)**

Add a callback/channel to ParquetWriter:

```rust
pub struct ParquetWriter {
    // ... existing fields ...
    on_file_finalized: Option<Box<dyn Fn(PathBuf, FileId) + Send>>,
}

impl ParquetWriter {
    pub fn finalize_current_file(&mut self) -> Result<()> {
        // ... existing finalization ...

        if let Some(callback) = &self.on_file_finalized {
            callback(final_path.clone(), file_id);
        }

        Ok(())
    }
}
```

Then in the sync worker:

```rust
let indexer = Arc::new(Mutex::new(IndexBuilder::new(storage)));
let indexer_clone = indexer.clone();

let writer = ParquetWriter::with_callback(
    data_dir,
    max_file_size,
    segment_size,
    data_type,
    move |path, file_id| {
        // Index in background
        tokio::spawn(async move {
            if let Err(e) = indexer_clone.lock().unwrap().index_file(&path) {
                error!("Failed to index {}: {}", path.display(), e);
            }
        });
    },
);
```

Option 2: **Background Scanner**

Run a periodic background task that scans for new parquet files and indexes them:

```rust
async fn index_scanner_task(
    data_dir: PathBuf,
    indexer: Arc<Mutex<IndexBuilder<EvmTransactionIndexer, RocksDbStorage>>>,
) {
    loop {
        // Scan for unindexed parquet files
        for file in find_unindexed_files(&data_dir)? {
            indexer.lock().unwrap().index_file(&file)?;
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
```

## Performance

### Zero-Copy Extraction

Simple keys (tx_hash) use `extract_ref()` which returns a direct reference to Arrow array data:

```rust
fn extract_ref<'a>(&self, batch: &'a RecordBatch, row_idx: usize) -> Option<&'a [u8]> {
    let hash_array = batch.column(columns::TX_HASH).as_fixed_size_binary();
    Some(hash_array.value(row_idx)) // Zero allocation!
}
```

### Buffer Reuse

Composite keys (address || block_num) reuse a single buffer per batch:

```rust
let mut key_buffer = Vec::with_capacity(64); // One allocation!

for row_idx in 0..batch.num_rows() {
    spec.key_extractor.extract_into(&batch, row_idx, &mut key_buffer);
    // Buffer is reused for next row
}
```

### Page-Level Precision

Using OffsetIndex, we can point to the exact page containing each row:

```rust
fn find_page_for_row(row_idx: usize, offset_index: &OffsetIndexMetaData)
    -> Result<(u16, u16)>
{
    for (page_idx, location) in offset_index.page_locations.iter().enumerate() {
        if row_idx >= location.first_row_index as usize && ... {
            let row_in_page = (row_idx - location.first_row_index) as u16;
            return Ok((page_idx as u16, row_in_page));
        }
    }
}
```

## Status

**Completed:**
- ✓ `IndexBuilder::index_file()` - Fully implemented
- ✓ `FileRegistry` trait - For FileId mapping
- ✓ Zero-copy key extraction
- ✓ Proper OffsetIndex page finding
- ✓ Batch atomic writes
- ✓ EVM transaction indexers (tx_by_hash, tx_by_from, tx_by_to)
- ✓ Working example with in-memory storage

**Future Work:**
- [ ] Implement `PageReader` with fusio async I/O
- [ ] RocksDB storage backend
- [ ] Integrate with phaser-query sync worker
- [ ] Merkle proof generation using indexes
- [ ] Multi-key extractors for Solana (account keys)
- [ ] Column index for min/max filtering

## Testing

Run the example:

```bash
cargo run --example index_transactions -- ./test-data/transactions_0_to_1000.parquet
```

Run tests:

```bash
cargo test -p parquet-index
cargo test -p evm-index
```
