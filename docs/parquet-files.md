# Parquet File Format and Naming

## Overview

Phaser uses Parquet files to store blockchain data (blocks, transactions, logs). Files are organized by data type and block range, with metadata stored in Parquet column statistics for efficient querying.

## Filename Convention

### Final Files
```
{data_type}_from_{segment_start}_to_{segment_end}_{sequence}.parquet
```

Examples:
- `blocks_from_0_to_499999_0.parquet`
- `transactions_from_500000_to_999999_0.parquet`
- `transactions_from_500000_to_999999_1.parquet`
- `logs_from_1000000_to_1499999_0.parquet`

**Key points:**
- `data_type`: One of `blocks`, `transactions`, or `logs`
- `segment_start` and `segment_end`: The 500K block segment this file belongs to
- `sequence`: Incrementing number starting from 0 for file rotations within a segment
- Range is **inclusive** on both ends
- Files are named by segment boundaries, not actual data ranges
- Multiple files can exist for the same segment (indicated by sequence number)

### Temporary Files During Writing
```
{data_type}_{timestamp}.parquet.tmp
```

Examples:
- `blocks_1733160000123.parquet.tmp`
- `transactions_1733160001456.parquet.tmp`

**Key points:**
- `.tmp` extension indicates file is actively being written
- Timestamp (Unix epoch milliseconds) ensures uniqueness
- Renamed to final format when file is finalized
- Temporary files are cleaned up on worker restart if incomplete

## File Metadata

Phaser stores two types of metadata in Parquet files:
1. **Column statistics** - For efficient querying
2. **Custom key-value metadata** - For file management and gap detection

### Phaser Metadata Format

Each file contains metadata with three range types:

- **Segment range**: The full 500K block segment this file belongs to
- **Responsibility range**: The block range this file is responsible for covering
- **Data range**: The actual blocks that have data (may skip empty blocks)

Metadata storage:
- Encoded with bincode, base64-encoded
- Stored in `phaser.meta` key-value metadata
- Can be updated in-place without rewriting file data

See [`../crates/phaser-parquet-metadata/src/lib.rs`](../crates/phaser-parquet-metadata/src/lib.rs) for `PhaserMetadata` struct definition and read/write methods.

### In-Place Metadata Updates

Metadata can be updated without rewriting file data using the `parquet-meta` CLI tool:

```bash
parquet-meta fix-meta transactions_from_1_to_499999.parquet \
  --segment-start 0 \
  --segment-end 499999 \
  --responsibility-start 1 \
  --responsibility-end 499999 \
  --data-type transactions \
  --infer  # Infer data_start/data_end from statistics
```

Works by reading footer, updating key-value metadata, truncating old footer, appending new footer. No data copying - works on multi-terabyte files in milliseconds.

See [`../crates/phaser-parquet-metadata/src/lib.rs`](../crates/phaser-parquet-metadata/src/lib.rs) for implementation.

### Block Range Statistics

Column-level statistics are enabled for `_block_num` column, storing min/max block numbers in file footer. This enables fast gap detection and query optimization without opening files.

See [`../crates/phaser-query/src/parquet_writer.rs`](../crates/phaser-query/src/parquet_writer.rs) for statistics configuration.

## File Creation Process

Files are created in three stages:

1. **Start** - Create `.tmp` file, initialize writer, track starting block
2. **Write** - Append batches, update end block, check finalization triggers
3. **Finalize** - Close writer, rename to final format, update metadata

Files finalize when:
- Segment boundary crossed
- Size limit exceeded

This ensures files never span segments and sizes stay bounded.

See [`../crates/phaser-query/src/parquet_writer.rs`](../crates/phaser-query/src/parquet_writer.rs) for implementation.

## Multiple Files Per Segment

Segments are logical units for parallel processing (e.g., 500K blocks). When files reach the size limit before the segment ends, they rotate creating multiple files with incrementing sequence numbers:

```
Segment 0 (blocks 0-499,999):
  - blocks_from_0_to_499999_0.parquet (sequence 0)
  - blocks_from_0_to_499999_1.parquet (sequence 1, if rotated)

Segment 1 (blocks 500,000-999,999):
  - blocks_from_500000_to_999999_0.parquet
```

Gap detection uses responsibility ranges from metadata to verify continuous coverage across all files in a segment.

See [`../crates/phaser-query/src/sync/data_scanner.rs`](../crates/phaser-query/src/sync/data_scanner.rs) for gap detection implementation.

## Configuration

Parquet writing is configured per data directory:

```yaml
# config.yaml
max_file_size_mb: 500    # Maximum file size before finalization
segment_size: 500000     # Logical segment size for parallelization

parquet:
  default_compression: "zstd"
  row_group_size_mb: 128
  column_options:
    data:
      compression: "zstd"
      encoding: "plain"
    _block_num:
      statistics: "page"  # Always enabled regardless of config
```

**Note**: Statistics for `_block_num` are **always** enabled at the Page level, regardless of config, as they are essential for gap detection and query optimization.

## Historical vs Live Streaming

### Historical Sync
- Fetches specific block ranges via Historical mode
- Writes to Parquet files with known boundaries
- Multiple workers can write different segments in parallel
- Files are immediately finalized when range complete

### Live Streaming
- Subscribes to current head via Live mode
- Writes to Parquet files as blocks arrive
- Sets `LiveStreamingState` boundary when first block received
- Files are finalized at segment boundaries or size limits
- Creates `.tmp` files that are renamed when finalized

The boundary between historical and live data is tracked in `LiveStreamingState` to ensure no gaps or overlaps.

## Cleanup and Recovery

### Worker Restart
When a sync worker restarts:
1. Scans existing `.parquet` files to find completed ranges
2. Deletes orphaned `.parquet.tmp` files from previous run
3. Identifies gaps in segment coverage
4. Resumes from gaps, not from beginning

### Interrupted Writes
If a write is interrupted:
- `.tmp` file remains on disk
- On restart, worker detects gap and re-fetches that range
- Old `.tmp` file is deleted before starting new worker
- No corrupted data in final `.parquet` files

## Example: File Lifecycle

```
1. Worker starts segment 0 (blocks 0-499,999), sequence 0
   Create: blocks_1733160000123.parquet.tmp

2. Write blocks 0-99,999
   Update: end_block = 99999

3. Write blocks 100,000-199,999
   Update: end_block = 199999

4. Block 200,000 arrives, size limit reached
   Close writer, rename to: blocks_from_0_to_499999_0.parquet
   Metadata: segment 0-499999, responsibility 0-199999, data 0-199999
   Increment sequence to 1
   Create: blocks_1733160005678.parquet.tmp

5. Write blocks 200,000-299,999
   Update: end_block = 299999

6. Write blocks 300,000-399,999
   Update: end_block = 399999

7. Write blocks 400,000-499,999
   Update: end_block = 499999

8. Block 500,000 arrives, segment boundary crossed
   Close writer, rename to: blocks_from_0_to_499999_1.parquet
   Metadata: segment 0-499999, responsibility 200000-499999, data 200000-499999
   Reset sequence to 0 for next segment

Result: Segment 0 covered by 2 files with contiguous responsibility ranges
```

## Querying Files

To find data for a specific block range:

1. **List files** in data directory
2. **Read Parquet metadata** for each file (cheap - just footer)
3. **Check block range** from `_block_num` statistics
4. **Skip files** outside query range
5. **Open relevant files** and apply additional filters

This allows efficient queries without scanning all files.

## Parquet Metadata CLI Tool

The `parquet-meta` CLI tool provides utilities for inspecting and modifying Parquet file metadata.

### Installation

```bash
cargo build -p parquet-meta
# Binary at: target/debug/parquet-meta
```

### Commands

#### Show Metadata

Display file metadata including Phaser metadata, schema, and row group information:

```bash
parquet-meta show <file.parquet>

# With verbose output (shows row group details)
parquet-meta show <file.parquet> --verbose
```

Example output:
```
File: transactions_from_1_to_499999_0.parquet

Schema:
  Version: 1
  Num rows: 504708

Phaser metadata:
  Version: 1
  Segment: 0-499999
  Responsibility: 1-499999
  Data range: 46147-499998
  Data type: transactions

Row groups: 4539
  Block range (from statistics): 46147-499998
```

#### Fix Metadata

Update or add Phaser metadata to an existing file:

```bash
parquet-meta fix-meta <file.parquet> \
  --segment-start <start> \
  --segment-end <end> \
  --responsibility-start <start> \
  --responsibility-end <end> \
  --data-type <type> \
  --infer
```

**Parameters:**
- `--segment-start`: Start of the full segment (e.g., 0 for segment 0-499999)
- `--segment-end`: End of the full segment (e.g., 499999)
- `--responsibility-start`: First block this file is responsible for
- `--responsibility-end`: Last block this file is responsible for
- `--data-type`: Type of data ("blocks", "transactions", or "logs")
- `--infer`: Infer `data_start` and `data_end` from Parquet statistics

Example:
```bash
parquet-meta fix-meta transactions_from_1_to_499999_0.parquet \
  --segment-start 0 \
  --segment-end 499999 \
  --responsibility-start 1 \
  --responsibility-end 499999 \
  --data-type transactions \
  --infer
```

**Important:** This modifies the file in-place. The operation is safe (validates footer before writing) but you may want to backup critical files first.
