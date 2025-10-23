/// Benchmarks for parquet read operations
///
/// Measures PageReader performance and compares indexed reads vs full scans.
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use evm_index::{EvmTransactionIndexer, CF_TX_BY_HASH};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_index::{FileRegistry, IndexBuilder, IndexStorage, IndexableSchema, PageReader};
use parquet_index_rocksdb::{RocksDbFileRegistry, RocksDbIndexStorage};
use parquet_index_schema::PagePointer;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

mod common;
use common::generate_test_parquet;

/// Setup indexed data for benchmarking parquet operations
fn setup_parquet_data(
    size: usize,
) -> (
    TempDir,
    TempDir,
    PathBuf,
    Arc<RocksDbIndexStorage>,
    Arc<RocksDbFileRegistry>,
    Vec<common::TestTransaction>,
) {
    let (temp_parquet, parquet_path, txs) = generate_test_parquet(size).unwrap();

    let temp_db_dir = TempDir::new().unwrap();
    let storage_path = temp_db_dir.path().join("storage");
    let registry_path = temp_db_dir.path().join("registry");

    let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
        .into_iter()
        .map(|spec| spec.column_family)
        .collect();

    let storage = Arc::new(RocksDbIndexStorage::open(&storage_path, column_families).unwrap());
    let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path).unwrap());

    let builder =
        IndexBuilder::<EvmTransactionIndexer, _, _>::new(storage.clone(), file_registry.clone());

    builder.index_file(&parquet_path).unwrap();

    (
        temp_parquet,
        temp_db_dir,
        parquet_path,
        storage,
        file_registry,
        txs,
    )
}

/// Benchmark PageReader row group reads (async)
fn bench_page_reader(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("page_reader");

    for size in [1_000, 10_000, 100_000].iter() {
        let (_temp_parquet, _temp_db, _parquet_path, storage, file_registry, txs) =
            setup_parquet_data(*size);

        let reader = PageReader::new(file_registry.clone());

        // Get pointer for middle transaction
        let target_tx = &txs[txs.len() / 2];
        let pointer_bytes = storage
            .get(CF_TX_BY_HASH, &target_tx.tx_hash.bytes)
            .unwrap()
            .unwrap();
        let pointer = PagePointer::from_bytes(&pointer_bytes).unwrap();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.to_async(&runtime).iter(|| async {
                let batch = reader.read_row_group_async(&pointer).await.unwrap();
                black_box(batch);
            });
        });
    }

    group.finish();
}

/// Benchmark full parquet scan (baseline comparison)
fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");

    for size in [1_000, 10_000, 100_000].iter() {
        let (temp_parquet, _temp_db, parquet_path, _storage, _file_registry, _txs) =
            setup_parquet_data(*size);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let file = File::open(&parquet_path).unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                let mut reader = builder.build().unwrap();

                // Read all batches
                let mut total_rows = 0;
                while let Some(batch_result) = reader.next() {
                    let batch = batch_result.unwrap();
                    total_rows += batch.num_rows();
                }

                black_box(total_rows);
            });
        });

        // Keep temp_parquet alive
        drop(temp_parquet);
    }

    group.finish();
}

/// Benchmark indexed lookup + read vs full scan
fn bench_indexed_vs_scan(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("indexed_vs_scan");

    let size = 100_000;
    let (_temp_parquet, _temp_db, parquet_path, storage, file_registry, txs) =
        setup_parquet_data(size);

    let reader = PageReader::new(file_registry.clone());

    // Find a transaction to lookup
    let target_tx = &txs[size / 2];

    // Indexed lookup + read
    group.bench_function("indexed_lookup_and_read", |b| {
        b.to_async(&runtime).iter(|| async {
            // 1. Lookup in index
            let pointer_bytes = storage
                .get(CF_TX_BY_HASH, &target_tx.tx_hash.bytes)
                .unwrap()
                .unwrap();
            let pointer = PagePointer::from_bytes(&pointer_bytes).unwrap();

            // 2. Read row group
            let batch = reader.read_row_group_async(&pointer).await.unwrap();
            black_box(batch);
        });
    });

    // Full scan to find the same transaction
    group.bench_function("full_scan_to_find", |b| {
        b.iter(|| {
            let file = File::open(&parquet_path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader = builder.build().unwrap();

            // Scan until we find the transaction (simplified)
            let mut found = false;
            while let Some(batch_result) = reader.next() {
                let batch = batch_result.unwrap();
                if batch.num_rows() > 0 {
                    // In real scenario we'd check tx_hash column
                    found = true;
                    break;
                }
            }

            black_box(found);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_page_reader,
    bench_full_scan,
    bench_indexed_vs_scan
);
criterion_main!(benches);
