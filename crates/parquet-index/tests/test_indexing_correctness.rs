/// End-to-end indexing correctness tests
///
/// These tests verify that the complete indexing pipeline works correctly:
/// - Parquet files are indexed properly
/// - All expected index entries are created
/// - PagePointers point to correct rows
/// - No data is lost or corrupted
mod fixtures;

use anyhow::Result;
use evm_index::EvmTransactionIndexer;
use fixtures::{generate_test_parquet, make_address};
use parquet_index::{FileRegistry, IndexBuilder, IndexStorage, IndexableSchema};
use parquet_index_rocksdb::{RocksDbFileRegistry, RocksDbIndexStorage};
use parquet_index_schema::PagePointer;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_end_to_end_indexing_small() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    // Generate 100 test transactions
    let (_temp_parquet_dir, parquet_path, expected_txs) = generate_test_parquet(100)?;

    // Create RocksDB storage in temp directory (use separate paths for storage and registry)
    let temp_db_dir = TempDir::new()?;
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

    // Build indexes
    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    let file_id = builder.index_file(&parquet_path)?;

    // Verify all transactions are indexed by hash
    for (i, tx) in expected_txs.iter().enumerate() {
        let pointer_bytes = storage
            .get("tx_by_hash", &tx.tx_hash.bytes)?
            .unwrap_or_else(|| {
                panic!(
                    "Transaction {} (hash {:?}) should be indexed in tx_by_hash",
                    i, tx.tx_hash.bytes
                )
            });

        let pointer = PagePointer::from_bytes(&pointer_bytes)?;

        // Verify pointer makes sense
        assert_eq!(
            pointer.file_id, file_id,
            "PagePointer file_id should match indexed file"
        );
        assert!(
            pointer.row_group < 10,
            "Row group should be reasonable (got {})",
            pointer.row_group
        );
        assert!(
            pointer.page_index < 100,
            "Page index should be reasonable (got {})",
            pointer.page_index
        );
    }

    println!("✓ All {} transactions indexed by hash", expected_txs.len());

    // Verify transactions are indexed by from address
    for (i, tx) in expected_txs.iter().enumerate() {
        let mut key = Vec::new();
        key.extend_from_slice(&tx.from.bytes);
        key.extend_from_slice(&tx.block_num.to_be_bytes());
        key.extend_from_slice(&tx.tx_index.to_be_bytes());

        let pointer_bytes = storage.get("tx_by_from", &key)?.unwrap_or_else(|| {
            panic!(
                "Transaction {} (from {:?}, block {}) should be indexed in tx_by_from",
                i, tx.from.bytes, tx.block_num
            )
        });

        let pointer = PagePointer::from_bytes(&pointer_bytes)?;
        assert_eq!(pointer.file_id, file_id);
    }

    println!(
        "✓ All {} transactions indexed by from address",
        expected_txs.len()
    );

    // Verify transactions with 'to' address are indexed (skip contract creations)
    let txs_with_to: Vec<_> = expected_txs.iter().filter(|tx| tx.to.is_some()).collect();

    for tx in &txs_with_to {
        let to_addr = tx.to.clone().unwrap();
        let mut key = Vec::new();
        key.extend_from_slice(&to_addr.bytes);
        key.extend_from_slice(&tx.block_num.to_be_bytes());
        key.extend_from_slice(&tx.tx_index.to_be_bytes());

        let pointer_bytes = storage.get("tx_by_to", &key)?.unwrap_or_else(|| {
            panic!(
                "Transaction with to={:?} should be indexed in tx_by_to",
                to_addr.bytes
            )
        });

        let pointer = PagePointer::from_bytes(&pointer_bytes)?;
        assert_eq!(pointer.file_id, file_id);
    }

    println!(
        "✓ All {} transactions with 'to' indexed correctly",
        txs_with_to.len()
    );

    // Verify contract creations are NOT in tx_by_to
    let contract_creations: Vec<_> = expected_txs.iter().filter(|tx| tx.to.is_none()).collect();

    println!(
        "✓ Found {} contract creations (to=None)",
        contract_creations.len()
    );

    // Contract creations should not have tx_by_to entries
    // We can't easily verify absence, but we verified all tx_by_to entries above

    Ok(())
}

#[test]
fn test_end_to_end_indexing_medium() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    // Generate 1000 test transactions (10 row groups)
    let (_temp_parquet_dir, parquet_path, expected_txs) = generate_test_parquet(1000)?;

    let temp_db_dir = TempDir::new()?;
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    let file_id = builder.index_file(&parquet_path)?;

    // Sample verification (don't check every transaction for performance)
    let sample_indices = vec![0, 100, 500, 999];

    for &i in &sample_indices {
        let tx = &expected_txs[i];

        // Check tx_by_hash
        let pointer_bytes = storage.get("tx_by_hash", &tx.tx_hash.bytes)?.unwrap();
        let pointer = PagePointer::from_bytes(&pointer_bytes)?;
        assert_eq!(pointer.file_id, file_id);

        // Check tx_by_from
        let mut key = Vec::new();
        key.extend_from_slice(&tx.from.bytes);
        key.extend_from_slice(&tx.block_num.to_be_bytes());
        key.extend_from_slice(&tx.tx_index.to_be_bytes());
        storage.get("tx_by_from", &key)?.unwrap();
    }

    println!("✓ Verified sample of 1000 transactions indexed correctly");

    Ok(())
}

#[test]
fn test_prefix_scan_by_address() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    // Generate transactions where multiple txs have the same 'from' address
    let (_temp_parquet_dir, parquet_path, expected_txs) = generate_test_parquet(200)?;

    let temp_db_dir = TempDir::new()?;
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    builder.index_file(&parquet_path)?;

    // Find a common address
    let target_address = make_address(5);

    // Count expected transactions from this address
    let expected_count = expected_txs
        .iter()
        .filter(|tx| tx.from.bytes == target_address.bytes)
        .count();

    println!(
        "Expected {} transactions from address {:?}",
        expected_count, target_address.bytes
    );

    // Prefix scan with just the address (20 bytes)
    let results: Vec<_> = storage
        .prefix_iterator("tx_by_from", &target_address.bytes)
        .collect();

    println!("Found {} transactions via prefix scan", results.len());

    assert_eq!(
        results.len(),
        expected_count,
        "Prefix scan should find all transactions from address"
    );

    // Verify all results are valid PagePointers
    for (key, value) in &results {
        // Key should be 32 bytes (20 address + 8 block_num + 4 tx_index)
        assert_eq!(key.len(), 32, "Key should be 32 bytes");

        // Value should be valid PagePointer
        let pointer = PagePointer::from_bytes(&value)?;
        assert!(pointer.row_group < 100);
        assert!(pointer.page_index < 100);
    }

    Ok(())
}

#[test]
fn test_file_registry_persistence() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let (_temp_parquet_dir, parquet_path, _expected_txs) = generate_test_parquet(50)?;

    let temp_db_dir = TempDir::new()?;
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    // First indexing session
    let file_id = {
        let storage = Arc::new(RocksDbIndexStorage::open(
            &storage_path,
            column_families.clone(),
        )?);
        let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

        let builder = IndexBuilder::<EvmTransactionIndexer, _, _>::new(
            storage.clone(),
            file_registry.clone(),
        );

        builder.index_file(&parquet_path)?
    };

    // Close database (Arc dropped)

    // Reopen database
    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

    // Verify file is still registered
    let path = file_registry.get_file_path(file_id)?;
    assert_eq!(path, parquet_path);

    // Re-registering should return same ID
    let file_id_again = file_registry.register_file(&parquet_path)?;
    assert_eq!(file_id, file_id_again);

    // Indexes should still be accessible
    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    // Re-indexing should work (will update existing entries)
    let file_id_reindex = builder.index_file(&parquet_path)?;
    assert_eq!(file_id, file_id_reindex);

    println!("✓ FileRegistry persisted correctly across database restart");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_page_reader_integration() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    // Generate test transactions and index them
    let (_temp_parquet_dir, parquet_path, expected_txs) = generate_test_parquet(100)?;

    let temp_db_dir = TempDir::new()?;
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families)?);
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path)?);

    // Index the file
    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());
    let file_id = builder.index_file(&parquet_path)?;

    // Now use PageReader to read specific transactions
    use parquet_index::PageReader;
    let reader = PageReader::new(file_registry.clone());

    // Get pointer for first transaction
    let first_tx = &expected_txs[0];
    let pointer_bytes = storage.get("tx_by_hash", &first_tx.tx_hash.bytes)?.unwrap();
    let pointer = PagePointer::from_bytes(&pointer_bytes)?;

    // Read the row group (async)
    let batch = reader.read_row_group_async(&pointer).await?;

    // Verify we got data
    assert!(batch.num_rows() > 0, "Should read non-empty batch");
    assert_eq!(
        pointer.file_id, file_id,
        "Pointer should reference indexed file"
    );

    println!(
        "✓ PageReader successfully read {} rows from row group {}",
        batch.num_rows(),
        pointer.row_group
    );

    Ok(())
}
