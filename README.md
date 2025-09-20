# phaser-query

A lightweight query engine that provides both RPC and SQL interfaces for blockchain data stored in Parquet files.

## Overview

- **Fast point lookups** for RPC-style queries (eth_getBlockByNumber, etc.)
- **SQL interface** for analytical queries using DataFusion
- **Lightweight deployment** with no external database dependencies
- **Efficient indexing** of Parquet files with pointers stored in RocksDB

## Architecture

```
┌─────────────────────────────────────────┐
│            phaser-query                  │
├─────────────────────────────────────────┤
│  RPC Interface    │    SQL Interface    │
│  (JSON-RPC)       │    (HTTP/Flight)    │
├───────────────────┴────────────────────┤
│         DataFusion Query Engine         │
├─────────────────────────────────────────┤
│         RocksDB Catalog                 │
│   - Block index                         │
│   - Transaction index                   │
│   - Log index                           │
│   - File metadata                       │
├─────────────────────────────────────────┤
│         Parquet Files on Disk           │
└─────────────────────────────────────────┘
```

## Key Features

- **Dual Interface**: Supports both Ethereum JSON-RPC methods and SQL queries
- **Index-based Lookups**: RocksDB stores indexes pointing to locations within Parquet files
- **No Data Duplication**: Only indexes are stored in RocksDB, actual data remains in Parquet

## Building

```bash
cargo build --release
```

## Running

```bash
cargo run -- --data-dir /path/to/parquet/files --rocksdb-path /path/to/indexes
```

## Configuration

TODO: Add configuration details

## RPC Methods

Supported Ethereum JSON-RPC methods:
- `eth_blockNumber`
- `eth_getBlockByNumber`
- `eth_getTransactionByHash`
- `eth_getLogs`
- More coming soon...

## SQL Interface

Execute SQL queries against the Parquet data:

```sql
SELECT * FROM blocks WHERE number > 1000000 LIMIT 10;
SELECT COUNT(*) FROM transactions WHERE from_address = '0x...';
```

## Development Status

This is an early prototype exploring the architecture of separating query engine concerns from orchestration/job management.

## License

TODO: Add license information
