# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0-alpha.1] - 2026-03-26

### Added

- **Smart Client Sync Module** (`phaser-client`):
  - `PhaserSyncer` orchestrator with parallel worker pool
  - `BatchWriter` trait for pluggable storage backends
  - `WriterFactory` trait for creating writers per data type
  - Exponential backoff retry with `RetryPolicy` configuration
  - Progress streaming via `subscribe_progress()` method
  - Error categorization (transient vs permanent) for intelligent retry decisions
  - Position-agnostic `Range` and `SegmentWork` types for any ordered data source

- **Generic Position/Range Types**:
  - Replaced EVM-specific `BlockRange` with generic `Range` type
  - `SegmentWork` uses `HashMap<String, Vec<Range>>` for flexible data type tracking
  - Supports any ordered data source (EVM blocks, Solana slots, Canton offsets)

- **Proto Updates** (`phaser-query`):
  - Added generic `Range` message (replaces `BlockRange`)
  - Added `DataTypeRanges` for flexible missing range tracking
  - Added `by_type` maps in `DataProgress` and `FileStatistics`
  - Renamed `from_block`/`to_block` to `from_position`/`to_position`

### Changed

- `phaser-query` now uses `phaser-client::sync::SegmentWork` instead of local definition
- Proto `IncompleteSegment` uses generic `missing_ranges` field (old fields deprecated)

### Removed

- **Dead code from `phaser-types`**:
  - `BlockRange` struct (had unused `source: DataSource` field)
  - `DataSource` enum (never instantiated)
  - `DataAvailability` struct (never used)

### Deprecated

- Proto fields `missing_blocks_ranges`, `missing_transactions_ranges`, `missing_logs_ranges`
  in `IncompleteSegment` (use `missing_ranges` instead)
- Proto fields `blocks`, `transactions`, `logs` in `DataProgress` (use `by_type` instead)
