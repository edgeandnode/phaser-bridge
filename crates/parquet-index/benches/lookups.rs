/// Benchmarks for index lookup performance
///
/// Measures how fast we can lookup transactions from indexes.
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use evm_index::EvmTransactionIndexer;
use parquet_index::{FileRegistry, IndexBuilder, IndexStorage, IndexableSchema};
use parquet_index_rocksdb::{RocksDbFileRegistry, RocksDbIndexStorage};
use parquet_index_schema::PagePointer;
use std::sync::Arc;
use tempfile::TempDir;

mod common;
use common::generate_test_parquet;

/// Setup indexed data for benchmarking lookups
fn setup_indexed_data(
    size: usize,
) -> (
    TempDir,
    Arc<RocksDbIndexStorage>,
    Arc<RocksDbFileRegistry>,
    Vec<common::TestTransaction>,
) {
    let (_temp_parquet, parquet_path, txs) = generate_test_parquet(size).unwrap();

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

    (temp_db_dir, storage, file_registry, txs)
}

/// Benchmark single hash lookups
fn bench_hash_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_lookups");

    for size in [1_000, 10_000, 100_000].iter() {
        let (_temp_db, storage, _registry, txs) = setup_indexed_data(*size);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            // Lookup the middle transaction
            let target_tx = &txs[txs.len() / 2];

            b.iter(|| {
                let result = storage.get("tx_by_hash", &target_tx.tx_hash.bytes).unwrap();
                black_box(result);
            });
        });
    }

    group.finish();
}

/// Benchmark prefix scans (all txs from an address)
fn bench_prefix_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_scans");

    for size in [1_000, 10_000, 100_000].iter() {
        let (_temp_db, storage, _registry, txs) = setup_indexed_data(*size);

        // Find an address with multiple transactions
        let target_address = common::make_address(5); // This address appears frequently

        let expected_count = txs
            .iter()
            .filter(|tx| tx.from.bytes == target_address.bytes)
            .count();

        group.throughput(Throughput::Elements(expected_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let results: Vec<_> = storage
                    .prefix_iterator("tx_by_from", &target_address.bytes)
                    .collect();
                black_box(results);
            });
        });
    }

    group.finish();
}

/// Benchmark batch lookups (multiple hash lookups)
fn bench_batch_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_lookups");

    let batch_sizes = [10, 100, 1000];

    for batch_size in batch_sizes.iter() {
        let (_temp_db, storage, _registry, txs) = setup_indexed_data(10_000);

        // Select batch_size transactions to lookup
        let lookup_txs: Vec<_> = txs.iter().step_by(10_000 / batch_size).collect();

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &_size| {
                b.iter(|| {
                    for tx in &lookup_txs {
                        let result = storage.get("tx_by_hash", &tx.tx_hash.bytes).unwrap();
                        black_box(result);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark PagePointer deserialization
fn bench_pointer_deserialization(c: &mut Criterion) {
    let (_temp_db, storage, _registry, txs) = setup_indexed_data(1_000);

    let target_tx = &txs[0];
    let pointer_bytes = storage
        .get("tx_by_hash", &target_tx.tx_hash.bytes)
        .unwrap()
        .unwrap();

    c.bench_function("pointer_deserialization", |b| {
        b.iter(|| {
            let pointer = PagePointer::from_bytes(&pointer_bytes).unwrap();
            black_box(pointer);
        });
    });
}

criterion_group!(
    benches,
    bench_hash_lookups,
    bench_prefix_scans,
    bench_batch_lookups,
    bench_pointer_deserialization
);
criterion_main!(benches);
