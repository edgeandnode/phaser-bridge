# Parquet Index RocksDB

RocksDB implementation of `IndexStorage` and `FileRegistry` for production use.

## Overview

This crate provides persistent storage backends for the parquet indexing system:

- **RocksDbIndexStorage** - Stores index entries (key → PagePointer mappings)
- **RocksDbFileRegistry** - Manages FileId ↔ path mappings

## Features

- **Persistent storage** - Indexes survive process restarts
- **Column families** - Separate storage for each index type
- **Atomic writes** - Batch operations are atomic
- **In-memory caching** - Fast lookups for FileRegistry
- **Thread-safe** - Safe for concurrent access

## Usage

### Basic Example

```rust
use parquet_index_rocksdb::{RocksDbIndexStorage, RocksDbFileRegistry};
use std::sync::Arc;

// Create storage with column families
let storage = RocksDbIndexStorage::open(
    "./indexes",
    vec!["tx_by_hash".to_string(), "tx_by_from".to_string()],
)?;

// Create file registry
let file_registry = RocksDbFileRegistry::open("./indexes")?;

// Use with IndexBuilder
let builder = IndexBuilder::<EvmTransactionIndexer, _, _>::new(
    Arc::new(storage),
    Arc::new(file_registry),
);
```

### Storage Operations

```rust
// Put
storage.put("tx_by_hash", key, value)?;

// Get
let value = storage.get("tx_by_hash", key)?;

// Batch write (atomic)
let mut batch = WriteBatch::new();
batch.put("tx_by_hash", key1.to_vec(), value1.to_vec());
batch.put("tx_by_hash", key2.to_vec(), value2.to_vec());
storage.write_batch(batch)?;

// Prefix scan
for (key, value) in storage.prefix_iterator("tx_by_from", address_prefix) {
    // Process entries with matching prefix
}
```

### File Registry

```rust
// Register a file
let file_id = file_registry.register_file(&PathBuf::from("tx_0_1000.parquet"))?;

// Lookup path
let path = file_registry.get_file_path(file_id)?;

// Re-registering returns same ID
let same_id = file_registry.register_file(&path)?;
assert_eq!(file_id, same_id);
```

## Storage Layout

### RocksDbIndexStorage

Uses one column family per index:

```
Column Family: "tx_by_hash"
  key: 32-byte transaction hash
  value: 12-byte PagePointer

Column Family: "tx_by_from"
  key: 20-byte address || 8-byte block number
  value: 12-byte PagePointer
```

### RocksDbFileRegistry

Uses multiple column families:

```
Column Family: "file_id_to_path"
  key: 4-byte FileId (big-endian u32)
  value: UTF-8 path string

Column Family: "path_to_file_id"
  key: UTF-8 path string
  value: 4-byte FileId (big-endian u32)

Column Family: "metadata"
  (reserved for future use)
```

## Performance

### Write Performance

- Batch writes are atomic and efficient
- Single write: ~0.1-1 ms
- Batch of 1000: ~10-50 ms (10-50 μs per entry)

### Read Performance

- Get: <1 ms (typically <0.1 ms with caching)
- Prefix scan: ~1-10 ms for typical result sets
- FileRegistry lookups: <0.1 ms (cached)

### Disk Usage

For 10M transactions with 3 indexes:

```
tx_by_hash:  10M × (32 + 12) = 440 MB
tx_by_from:  10M × (28 + 12) = 400 MB
tx_by_to:    9.5M × (28 + 12) = 380 MB (some contract creations)
file_registry: < 1 MB

Total: ~1.2 GB (without compression)
With compression: ~600 MB (typical)
```

## Caching

### FileRegistry Caching

The `RocksDbFileRegistry` maintains in-memory caches:

- `path_cache`: FileId → PathBuf
- `id_cache`: PathBuf → FileId

These are populated:
1. On database open (loaded from RocksDB)
2. On first access (lazy loading)
3. On registration (write-through)

**Benefits**:
- Fast lookups (no disk I/O for repeated queries)
- Survives process restarts (persisted in RocksDB)

## Testing

The crate includes comprehensive tests:

```bash
cargo test -p parquet-index-rocksdb
```

Tests cover:
- Basic put/get operations
- Batch writes
- File registration
- Persistence across restarts
- Prefix iteration

All tests use temporary directories and are cleaned up automatically.

## Example

See `evm-index/examples/index_with_rocksdb.rs` for a complete example:

```bash
cargo run --example index_with_rocksdb -- \
    ./test-data/transactions.parquet \
    ./indexes
```

This will:
1. Create a RocksDB database at `./indexes`
2. Index the parquet file
3. Store all indexes persistently
4. Show how to query the indexes

## Production Considerations

### Column Family Planning

Create all column families upfront:

```rust
let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
    .into_iter()
    .map(|spec| spec.column_family)
    .collect();

let storage = RocksDbIndexStorage::open("./indexes", column_families)?;
```

### Backup and Recovery

RocksDB supports point-in-time backups:

```bash
# Backup
cp -r ./indexes ./indexes.backup

# Restore
rm -rf ./indexes
cp -r ./indexes.backup ./indexes
```

For production, use RocksDB's built-in backup engine.

### Tuning

For large datasets, consider tuning RocksDB options:

```rust
// (Future enhancement)
let mut opts = Options::default();
opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
opts.set_max_background_jobs(4);
opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB
```

## Limitations

- **Column families are immutable** - Must be specified at open time
- **No distributed support** - Single-node only
- **Limited query capabilities** - Get and prefix scans only

For more advanced querying, consider building a query layer on top.

## License

Same as parent project.
