/// Benchmarks for indexing performance
///
/// Measures how fast we can build indexes from parquet files of varying sizes.
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use evm_index::EvmTransactionIndexer;
use parquet_index::{FileRegistry, IndexBuilder, IndexStorage, IndexableSchema};
use parquet_index_rocksdb::{RocksDbFileRegistry, RocksDbIndexStorage};
use std::sync::Arc;
use tempfile::TempDir;

mod common;
use common::generate_test_parquet;

/// Benchmark building indexes for different file sizes
fn bench_index_file_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_file_sizes");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let (_temp_parquet, parquet_path, _txs) = generate_test_parquet(*size).unwrap();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let temp_db_dir = TempDir::new().unwrap();
                let storage_path = temp_db_dir.path().join("storage");
                let registry_path = temp_db_dir.path().join("registry");

                let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
                    .into_iter()
                    .map(|spec| spec.column_family)
                    .collect();

                let storage = Arc::new(
                    RocksDbIndexStorage::open(&storage_path, column_families).unwrap(),
                );
                let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path).unwrap());

                let builder = IndexBuilder::<EvmTransactionIndexer, _, _>::new(
                    storage.clone(),
                    file_registry.clone(),
                );

                black_box(builder.index_file(&parquet_path).unwrap());
            });
        });
    }

    group.finish();
}

/// Benchmark the overhead of RocksDB operations during indexing
fn bench_rocksdb_write_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("rocksdb_write_patterns");

    // Generate test data once
    let (_temp_parquet, parquet_path, _txs) = generate_test_parquet(10_000).unwrap();

    group.throughput(Throughput::Elements(10_000));

    // Sequential writes (current implementation)
    group.bench_function("sequential_writes", |b| {
        b.iter(|| {
            let temp_db_dir = TempDir::new().unwrap();
            let storage_path = temp_db_dir.path().join("storage");
            let registry_path = temp_db_dir.path().join("registry");

            let column_families: Vec<String> = EvmTransactionIndexer::index_specs()
                .into_iter()
                .map(|spec| spec.column_family)
                .collect();

            let storage =
                Arc::new(RocksDbIndexStorage::open(&storage_path, column_families).unwrap());
            let file_registry = Arc::new(RocksDbFileRegistry::open(&registry_path).unwrap());

            let builder = IndexBuilder::<EvmTransactionIndexer, _, _>::new(
                storage.clone(),
                file_registry.clone(),
            );

            black_box(builder.index_file(&parquet_path).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_index_file_sizes, bench_rocksdb_write_patterns);
criterion_main!(benches);
