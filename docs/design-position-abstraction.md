# Design: Position-Based Data Abstraction

## Problem Statement

The current Phaser and AMP architecture uses `BlockRange` as its fundamental unit of progress tracking:

```rust
pub struct BlockRange {
    pub numbers: RangeInclusive<u64>,
    pub network: NetworkId,
    pub hash: BlockHash,       // EVM-specific: 32-byte Keccak hash
    pub prev_hash: BlockHash,  // EVM-specific: parent block hash
    pub timestamp: Option<u64>,
}
```

This design has several problems:

1. **EVM-centric assumptions**: `BlockHash` is a 32-byte Keccak hash, which only makes sense for EVM chains. Canton, Solana, and other chains have different hash formats or no meaningful block hashes at all.

2. **"Block" is misleading**: For non-blockchain data sources (Canton ledger offsets, event streams), there are no "blocks" - just positions in a sequence.

3. **Hash-based adjacency is fragile**: The `prev_hash == hash` adjacency check assumes blockchain fork semantics. Canton has no forks. Solana has different finality semantics.

4. **Leaky abstraction**: Code throughout the stack has to handle `BlockHash::ZERO` as a sentinel value meaning "hash not applicable."

## Proposed Solution: Position Ranges

Replace the EVM-specific `BlockRange` with a chain-agnostic `PositionRange`:

```rust
/// A range of positions in a data stream.
///
/// Positions are opaque u64 values that represent progress through a data source.
/// The interpretation depends on the source:
/// - EVM chains: block number
/// - Canton: ledger offset
/// - Solana: slot number
/// - Event streams: sequence number
pub struct PositionRange {
    /// Start position (inclusive)
    pub start: u64,
    /// End position (inclusive)
    pub end: u64,
    /// Network identifier
    pub network: NetworkId,
    /// Optional bridge-specific cursor for precise resumption
    pub cursor: Option<Cursor>,
    /// Optional timestamp of the end position
    pub timestamp: Option<u64>,
}

/// Opaque cursor for precise position identification.
///
/// Cursors are bridge-specific state serialized as JSON. The sync layer
/// treats them as opaque - it stores and forwards them, but never inspects
/// the contents. Bridges produce cursors when emitting data and consume
/// them when resuming.
///
/// Using JSON provides:
/// - Human-readable debugging
/// - Easy serialization/deserialization in any language
/// - Schema flexibility - bridges can evolve their cursor format
/// - No coupling between sync layer and bridge internals
pub type Cursor = serde_json::Value;
```

### Example Cursors

Bridges define their own cursor schemas. The sync layer never parses these:

```json
// EVM bridge cursor - enables fork detection
{
  "block_hash": "0xabc123...",
  "prev_hash": "0xdef456...",
  "total_difficulty": "12345678901234567890"
}

// Canton bridge cursor - participant-specific offset
{
  "participant_id": "participant1::namespace",
  "offset": 12345,
  "domain_id": "domain1"
}

// Solana bridge cursor - slot with commitment
{
  "slot": 123456789,
  "commitment": "finalized",
  "blockhash": "ABC123..."
}

// Kafka bridge cursor - partition offsets
{
  "partition_offsets": {
    "0": 1000,
    "1": 2000,
    "2": 1500
  }
}

// Generic event stream - just a sequence number
{
  "seq": 999
}
```

## Key Design Principles

### 0. Sync Layer is Source-Agnostic

The sync layer (phaser-client) should have **zero knowledge** of specific data sources. It doesn't know about:
- Blockchains vs event streams vs databases
- EVM vs Solana vs Canton
- Blocks vs slots vs offsets
- Hashes vs sequence numbers

It only knows about:
- **Positions**: monotonically increasing u64 values
- **Ranges**: start and end positions
- **Data types**: string names like "transactions", "events", "state_changes"
- **Cursors**: opaque JSON blobs it passes through

This separation keeps the sync layer simple and lets bridges implement source-specific logic without polluting the core abstractions.

### 1. Position is Always u64

Every data source can be mapped to a monotonically increasing u64 position:

| Source | Position Meaning |
|--------|------------------|
| EVM chain | Block number |
| Canton | Ledger offset |
| Solana | Slot number |
| Kafka | Offset |
| Generic stream | Sequence number |

This enables uniform progress tracking, range queries, and segment alignment regardless of the underlying source.

### 2. Cursor is Optional and Opaque

Cursors carry bridge-specific state needed for:
- **Fork detection** (EVM: compare hashes to detect reorgs)
- **Precise resumption** (resume from exact position after restart)
- **Consistency guarantees** (Solana commitment levels)

But cursors are optional because:
- Not all sources have meaningful cursors (simple event streams may not need them)
- Progress tracking works fine with just positions
- Cursors are only needed for advanced features (fork handling, exact resumption)

**Key principle**: The sync layer is cursor-agnostic. It stores cursors in metadata, passes them to bridges on resume, but never parses or interprets them. This keeps the sync layer generic while letting bridges implement source-specific semantics.

### 3. Adjacency is Position-Based

Two ranges are adjacent if `range1.end + 1 == range2.start`. Fork detection (if needed) is the bridge's responsibility, not the sync layer's.

```rust
impl PositionRange {
    /// Check if this range is adjacent to (immediately precedes) another
    pub fn is_adjacent_to(&self, other: &PositionRange) -> bool {
        self.network == other.network && self.end + 1 == other.start
    }
}

// Fork detection is bridge-specific, not in PositionRange
trait Bridge {
    /// Check if resuming from this cursor would hit a fork.
    /// Bridge implementations parse their own cursor format.
    fn detects_fork(&self, stored_cursor: &Cursor, current_state: &BridgeState) -> bool;
}
```

### 4. Zero is a Valid Position

Unlike `BlockHash::ZERO` which is a sentinel meaning "no hash," position 0 is a valid position (genesis block, first event). There's no need for sentinel values.

## Migration Path

### Phase 1: Add PositionRange alongside BlockRange

```rust
// In phaser-client
pub use position::{PositionRange, Cursor};

// Bridge metadata can return either
pub struct BatchMetadata {
    pub responsibility_range: PositionRange,
    // Deprecated: for backwards compat
    pub legacy_block_range: Option<BlockRange>,
}
```

### Phase 2: Update bridges to emit PositionRange

- erigon-bridge: JSON cursor with `block_hash`, `prev_hash`
- canton-bridge: JSON cursor with `participant_id`, `offset`
- New bridges: Define their own JSON cursor schema or use `None`

### Phase 3: Update consumers

- phaser-query: Use `PositionRange` for gap analysis, file naming
- AMP: Use `PositionRange` in parquet metadata, restore logic

### Phase 4: Deprecate BlockRange

Once all consumers are migrated, deprecate `BlockRange` and remove EVM-specific assumptions from the core abstractions.

## File Naming

Current file naming uses block numbers:
```
000000000-{hash}.parquet  # Start block in filename
```

This continues to work since positions are u64:
```
000000000-{random}.parquet  # Position-based, works for any source
```

The hash suffix becomes optional/random since it was primarily for debugging EVM data.

## Segment Boundaries

Segment boundaries remain position-based:
```rust
let segment_size = 500_000;
let segment_num = position / segment_size;
let segment_start = segment_num * segment_size;
let segment_end = segment_start + segment_size - 1;
```

This works identically for blocks, offsets, slots, or any u64 position.

## Benefits

1. **Chain-agnostic**: Same abstractions work for EVM, Canton, Solana, and future sources
2. **Simpler**: No sentinel values, no hash-based adjacency checks
3. **Cleaner separation**: Position for progress, cursor for chain-specific semantics
4. **Forward-compatible**: New chains just define their cursor type
5. **Testable**: Position ranges are easy to construct and compare in tests

## Implementation in phaser-client Sync Module

The `phaser-client` sync module introduces a simple `Range` type for internal progress tracking:

```rust
/// A contiguous range of positions (e.g., blocks, slots, heights)
///
/// Generic range type for sync operations. The unit depends on context -
/// could be block numbers, slot numbers, or any ordered position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}
```

This is intentionally simpler than `PositionRange`:
- No `network` field (implied by context)
- No `cursor` field (sync module doesn't need fork detection)
- No `timestamp` (not needed for range tracking)

### SegmentWork Uses Generic Ranges

```rust
pub struct SegmentWork {
    pub segment_num: u64,
    pub segment_from: u64,
    pub segment_to: u64,
    /// Missing ranges by data type name (e.g., "blocks", "transactions", "events")
    pub missing_ranges: HashMap<String, Vec<Range>>,
    pub retry_count: Option<u32>,
    pub last_attempt: Instant,
}
```

Key design decisions:
- **String-keyed data types**: Rather than an enum (`Blocks`, `Transactions`, `Logs`), use strings. This makes the sync module chain-agnostic - Canton can use `"events"`, Solana can use `"account_updates"`, etc.
- **No hardcoded types**: The sync module doesn't know about "blocks" or "transactions" - it just syncs named data streams.

### WorkerProgress Uses String-Based Completion Tracking

```rust
pub struct WorkerProgress {
    pub worker_id: u32,
    pub from_block: u64,  // TODO: rename to from_position
    pub to_block: u64,    // TODO: rename to to_position
    pub current_phase: String,
    /// Data types that have been completed
    pub completed_types: HashSet<String>,
    /// Data types expected to complete
    pub expected_types: HashSet<String>,
    // ...
}
```

### Relationship to PositionRange

| Type | Location | Purpose |
|------|----------|---------|
| `Range` | phaser-client/sync | Internal progress tracking, simple start/end |
| `PositionRange` | phaser-types (proposed) | Wire format with network ID, cursor, timestamp |
| `BlockRange` | phaser-types (legacy) | EVM-specific, has hash fields |

The sync module's `Range` is intentionally minimal. When we need to communicate ranges across the wire (batch metadata, manifest files), we'd use `PositionRange`. The sync module converts between them at boundaries.

## Open Questions

1. **Should PositionRange be in phaser-client or a shared crate?** If AMP also uses it, maybe it belongs in a common crate.

2. **Naming: `from_block`/`to_block` vs `from_position`/`to_position`?** The sync module still uses "block" terminology in some places. Should rename to be position-agnostic.

3. **Should Range be `RangeInclusive<u64>` or custom struct?** Using `std::ops::RangeInclusive` loses the nice methods (overlaps, contains) and has different semantics (exclusive vs inclusive end).

4. **Cursor validation**: Should bridges validate cursor schemas on resume? JSON is flexible but a malformed cursor could cause confusing errors deep in bridge code. Consider adding a `validate_cursor()` method to bridges.

5. **Cursor versioning**: If a bridge's cursor schema changes, how do we handle old cursors? Options:
   - Include a version field in the cursor
   - Bridges handle migration internally
   - Just fail and require re-sync from position (safest)
