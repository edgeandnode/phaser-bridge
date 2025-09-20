# RocksDB Index Design for phaser-query

## Overview
RocksDB stores only indexes that point to locations within Parquet files. The actual blockchain data remains in Parquet format for efficient analytical queries.

## Index Structure

### 1. File Registry
**Column Family**: `files`
- **Key**: `file_id` (u32, incrementing)
- **Value**:
  ```rust
  struct FileMetadata {
      path: String,
      block_range: (u64, u64),  // (start_block, end_block)
      row_count: u32,
      file_size: u64,
      created_at: u64,
      schema_version: u8,
  }
  ```

### 2. Block Index
**Column Family**: `blocks`
- **Key**: `block_number` (u64, big-endian for ordering)
- **Value**:
  ```rust
  struct BlockPointer {
      file_id: u32,
      row_group: u16,
      row_offset: u32,
      // Total: 10 bytes
  }
  ```
- **Use Case**: Fast `eth_getBlockByNumber` lookups

### 3. Transaction Index
**Column Family**: `transactions`
- **Key**: `tx_hash` (32 bytes)
- **Value**:
  ```rust
  struct TxPointer {
      block_number: u64,
      file_id: u32,
      row_group: u16,
      row_offset: u32,
      tx_index: u16,  // position within block
      // Total: 20 bytes
  }
  ```
- **Use Case**: Fast `eth_getTransactionByHash` lookups

### 4. Log Bloom Filter Index
**Column Family**: `log_blooms`
- **Key**: `block_number` (u64)
- **Value**: `bloom_filter` (256 bytes)
- **Use Case**: Quick filtering for `eth_getLogs` before scanning

### 5. Address Activity Index
**Column Family**: `address_activity`
- **Key**: `address || block_number` (20 + 8 = 28 bytes)
- **Value**:
  ```rust
  struct ActivityPointer {
      file_id: u32,
      row_group: u16,
      has_logs: bool,
      has_internal_calls: bool,
      // Total: 8 bytes
  }
  ```
- **Use Case**: Find all blocks where an address was active

### 6. Topic Index (for common event topics)
**Column Family**: `topics`
- **Key**: `topic0 || block_number` (32 + 8 = 40 bytes)
- **Value**:
  ```rust
  struct TopicPointer {
      file_id: u32,
      row_group: u16,
      log_indices: Vec<u16>,  // which logs in the block
  }
  ```
- **Use Case**: Fast event filtering for known topics (Transfer, Swap, etc.)

### 7. Latest State Index
**Column Family**: `latest`
- **Key**: Fixed strings like "latest_block", "latest_safe", "latest_finalized"
- **Value**: `block_number` (u64)
- **Use Case**: Track chain head for RPC responses

## Indexing Strategy

### On Parquet File Import
1. Open Parquet file and read metadata
2. Register file in `files` CF with ID and metadata
3. Iterate through row groups:
   - For each block: Add to `blocks` index
   - For each transaction: Add to `transactions` index
   - For each log: Update bloom filter and topic indexes
   - Track address activity

### Memory Optimization
- Use batch writes with WriteBatch for efficiency
- Implement LRU cache for hot paths (recent blocks)
- Compress pointer values where possible

### Query Patterns

#### RPC: eth_getBlockByNumber
```
1. Look up block_number in `blocks` CF → get BlockPointer
2. Open Parquet file by file_id
3. Seek to row_group and row_offset
4. Read and return block data
```

#### RPC: eth_getTransactionByHash
```
1. Look up tx_hash in `transactions` CF → get TxPointer
2. Open Parquet file by file_id
3. Seek to row_group and row_offset
4. Read and return transaction data
```

#### RPC: eth_getLogs(filter)
```
1. For each block in range:
   a. Check bloom filter in `log_blooms` CF
   b. If bloom matches, get block pointer from `blocks` CF
   c. Read logs from Parquet and filter
2. Return matching logs
```

#### SQL: Complex queries
```
1. Use indexes to identify relevant Parquet files
2. Push down predicates to DataFusion
3. Let DataFusion handle the query execution
```

## Size Estimates

Assuming Ethereum mainnet (~20M blocks, ~2B transactions):

- **blocks index**: 20M × 10 bytes = 200 MB
- **transactions index**: 2B × 20 bytes = 40 GB
- **log_blooms index**: 20M × 256 bytes = 5 GB
- **address_activity**: ~10B entries × 8 bytes = 80 GB (sparse)
- **topics index**: ~5B entries × 10 bytes = 50 GB (common topics only)
- **files registry**: ~10K files × 100 bytes = 1 MB

**Total index size**: ~175 GB (vs ~10 TB of Parquet data)

## Implementation Priority

1. **Phase 1 (MVP)**:
   - File registry
   - Block index
   - Transaction index
   - Latest state tracking

2. **Phase 2 (Performance)**:
   - Bloom filters
   - LRU cache
   - Batch indexing

3. **Phase 3 (Advanced)**:
   - Address activity index
   - Topic index for common events
   - Range queries optimization
