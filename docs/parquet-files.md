# Parquet File Format and Naming

## Overview

Phaser uses Parquet files to store blockchain data (blocks, transactions, logs). Files are organized by data type and block range, with metadata stored in Parquet column statistics for efficient querying.

## Filename Convention

### Final Files
```
{data_type}_from_{start_block}_to_{end_block}.parquet
```

Examples:
- `blocks_from_0_to_99999.parquet`
- `transactions_from_100000_to_199999.parquet`
- `logs_from_200000_to_299999.parquet`

**Key points:**
- `data_type`: One of `blocks`, `transactions`, or `logs`
- `start_block` and `end_block`: The actual block range contained in the file
- Range is **inclusive** on both ends
- No segment information in filename - segments are a logical concept for parallel processing

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

Each file contains a compact, versioned metadata structure in the Parquet file footer:

```rust
pub struct PhaserMetadata {
    pub version: u8,                    // Format version (currently 1)
    pub segment_start: u64,             // Full segment range start (e.g., 0)
    pub segment_end: u64,               // Full segment range end (e.g., 499999)
    pub responsibility_start: u64,      // This file's responsibility start (e.g., 1)
    pub responsibility_end: u64,        // This file's responsibility end (e.g., 499999)
    pub data_start: u64,                // Actual first block with data (e.g., 46147)
    pub data_end: u64,                  // Actual last block with data (e.g., 499998)
    pub data_type: String,              // "blocks", "transactions", or "logs"
}
```

**Three types of ranges:**
- **Segment range**: The full 500K block segment this file belongs to
- **Responsibility range**: The block range this file is responsible for covering
- **Data range**: The actual blocks that have data (may skip empty blocks)

The metadata is:
- Encoded with bincode (compact binary format)
- Base64-encoded for storage in Parquet key-value metadata
- Stored in a single `phaser.meta` key
- Updated in-place without rewriting data (see below)

Reading metadata:
```rust
use phaser_parquet_metadata::PhaserMetadata;

let file = File::open(path)?;
let reader = SerializedFileReader::new(file)?;
let metadata = reader.metadata();

if let Some(kv_metadata) = metadata.file_metadata().key_value_metadata() {
    if let Ok(Some(phaser_meta)) = PhaserMetadata::from_key_value_metadata(kv_metadata) {
        println!("Segment: {}-{}", phaser_meta.segment_start, phaser_meta.segment_end);
        println!("Data: {}-{}", phaser_meta.data_start, phaser_meta.data_end);
    }
}
```

### In-Place Metadata Updates

Metadata can be updated **without rewriting the entire file** using the `parquet-meta` CLI tool or the library:

```bash
# Update metadata on an existing file
parquet-meta fix-meta transactions_from_1_to_499999.parquet \
  --segment-start 0 \
  --segment-end 499999 \
  --responsibility-start 1 \
  --responsibility-end 499999 \
  --data-type transactions \
  --infer  # Infer data_start/data_end from statistics
```

This works by:
1. Reading the existing Parquet footer (small, at end of file)
2. Updating the key-value metadata
3. Serializing the new footer using `ParquetMetaDataWriter`
4. Truncating the file to remove the old footer
5. Appending the new footer

**Benefits:**
- No data copying (preserves all row groups and compression)
- Works on multi-terabyte files in milliseconds
- Safe: detects corrupted footers and fails before writing
- Preserves file structure: row groups, column indexes, compression settings

**Safety:**
- Validates PAR1 magic bytes before updating
- Fails if footer is corrupted (detected during read)
- Uses atomic file operations where possible
- Tested with files containing thousands of row groups

### Block Range Statistics

Every Parquet file has **column-level statistics** enabled for the `_block_num` column:

```rust
// In ParquetWriter
builder.set_column_statistics_enabled("_block_num".into(), EnabledStatistics::Page);
```

This stores min/max block numbers in the Parquet file footer, allowing:
- **Fast gap detection**: Read block ranges without opening files
- **Efficient queries**: Skip files outside requested range
- **Resume logic**: Determine what data exists without scanning contents

Reading statistics:
```rust
let file = File::open(path)?;
let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
let metadata = builder.metadata();

// Read min/max from row group statistics
for row_group in metadata.row_groups() {
    let column_stats = row_group.column(block_num_col_idx).statistics();
    // Statistics are stored as Int64 at Parquet physical level
    if let Statistics::Int64(int_stats) = stats {
        let min_block = int_stats.min_opt().map(|&v| v as u64);
        let max_block = int_stats.max_opt().map(|&v| v as u64);
    }
}
```

**Note**: Parquet stores UInt64 as Int64 at the physical level, so we cast when reading.

## File Creation Process

### 1. Start New File
```rust
ParquetWriter::start_new_file(block_num, schema)
```
- Creates temporary file with timestamp
- Initializes ArrowWriter with schema and WriterProperties
- Tracks `start_block` (set to first block number written)

### 2. Write Batches
```rust
ParquetWriter::write_batch(batch)
```
- Writes RecordBatch to current file
- Updates `end_block` to latest block written
- Checks if file should be finalized (segment boundary or size limit)

### 3. Finalize File
```rust
ParquetWriter::finalize_current_file()
```
- Closes writer (flushes all data and writes footer with statistics)
- Renames `.tmp` file to final format with actual block range
- Logs completion with row count and block range

### 4. Finalization Triggers

Files are finalized and a new file started when:
1. **Segment boundary crossed**: `(block_num / segment_size) != (start_block / segment_size)`
2. **Size limit exceeded**: File size >= `max_file_size_bytes`

This means:
- Multiple files can cover a single segment
- Files never span segment boundaries
- File sizes are bounded

## Multiple Files Per Segment

Segments are logical units for parallel processing (e.g., 500K blocks). Physical files can be smaller:

```
Segment 0 (blocks 0-499,999):
  - blocks_from_0_to_249999.parquet
  - blocks_from_250000_to_499999.parquet

Segment 1 (blocks 500,000-999,999):
  - blocks_from_500000_to_999999.parquet
```

Gap detection checks if the **union** of file ranges covers the segment:
```rust
// Sort files by start block
ranges.sort_by_key(|r| r.start);

// Check continuous coverage
let mut covered_up_to = segment_start - 1;
for range in ranges {
    if range.start > covered_up_to + 1 {
        return false; // Gap found
    }
    covered_up_to = covered_up_to.max(range.end);
}

covered_up_to >= segment_end // Segment complete if covered to end
```

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
1. Worker starts segment 0 (blocks 0-499,999)
   Create: blocks_1733160000123.parquet.tmp

2. Write blocks 0-99,999
   Update: end_block = 99999

3. Write blocks 100,000-199,999
   Update: end_block = 199999

4. Block 200,000 arrives, size limit reached
   Close writer, rename to: blocks_from_0_to_199999.parquet
   Create: blocks_1733160005678.parquet.tmp

5. Write blocks 200,000-299,999
   Update: end_block = 299999

6. Write blocks 300,000-399,999
   Update: end_block = 399999

7. Write blocks 400,000-499,999
   Update: end_block = 499999

8. Block 500,000 arrives, segment boundary crossed
   Close writer, rename to: blocks_from_200000_to_499999.parquet

Result: Segment 0 covered by 2 files
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
