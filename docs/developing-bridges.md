# Developing Phaser Bridges

## Overview

Bridges translate blockchain node protocols into Arrow Flight streams. Bridges may buffer data for batching or validation (e.g., merkle trie construction), but don't cache historical data - that's phaser-query's responsibility.

**Architecture**: Node → Bridge (convert) → Arrow Flight → phaser-query (buffer/write) → Parquet

**Responsibilities**:
- Bridge: Stateless protocol translation
- phaser-query: Buffering, gap detection, Parquet writing

## Implementation

### FlightBridge Trait

Implement the `FlightBridge` trait from [`../crates/phaser-bridge/src/bridge.rs`](../crates/phaser-bridge/src/bridge.rs). Core methods:

- `get_info()` - Return bridge metadata (name, node type, chain ID, capabilities)
- `get_capabilities()` - Advertise supported features (historical, streaming, validation)
- `do_get()` - Stream data for a ticket (decode descriptor, return Arrow Flight stream)
- `health_check()` - Verify node connectivity

See [`../crates/bridges/evm/erigon-bridge/src/bridge.rs`](../crates/bridges/evm/erigon-bridge/src/bridge.rs) for complete implementation.

### Arrow Schemas

Define Arrow schemas for each data type. For EVM chains, use `evm-common` crate schemas. For other chains, define custom schemas.

**Critical**: Include `_block_num` column (UInt64) for gap detection and query optimization.

Schema references:
- `evm_common::block_arrow_schema()`
- `evm_common::transaction_arrow_schema()`
- `evm_common::log_arrow_schema()`

### Data Conversion

Create converter that decodes node-specific formats (RLP, JSON, protobuf) to Arrow RecordBatches. Pattern:

1. Decode node format to intermediate representation
2. Map to typed records (using `typed-arrow` or manual builders)
3. Build RecordBatch from Arrow arrays

See [`../crates/bridges/evm/erigon-bridge/src/converter.rs`](../crates/bridges/evm/erigon-bridge/src/converter.rs) for RLP → Arrow conversion example.

### Query Modes

Support two modes via `BlockchainDescriptor`:

**Historical** - Fetch specific block ranges from node
**Live** - Subscribe to new data as it arrives

See [`../crates/phaser-bridge/src/subscription.rs`](../crates/phaser-bridge/src/subscription.rs) for query mode definitions.

### Validation

Implement two-stage validation:

**Ingestion** - Validate raw data from node (RLP encoding, JSON structure)
**Conversion** - Validate Arrow RecordBatch after conversion (schema compliance, field constraints)

Make validation configurable via `ValidationStage` enum. See [`../crates/phaser-bridge/src/descriptors.rs#L69-L80`](../crates/phaser-bridge/src/descriptors.rs#L69-L80).

### Streaming

Implement `do_get()` to return Arrow Flight stream. Respect client preferences:
- `batch_size_hint` - Blocks per batch
- `max_message_bytes` - Maximum message size
- `compression` - gRPC compression (none, gzip, zstd)

See [`../crates/bridges/evm/erigon-bridge/src/streaming_service.rs`](../crates/bridges/evm/erigon-bridge/src/streaming_service.rs) for live streaming implementation.

## Configuration

Bridges run as standalone services, connect via Unix socket or TCP:

```bash
# Unix socket (recommended for local phaser-query)
erigon-bridge --erigon-grpc localhost:9090 \
  --ipc-path /var/run/erigon-bridge.sock \
  --chain-id 1

# TCP (for remote phaser-query)
erigon-bridge --erigon-grpc localhost:9090 \
  --flight-addr 0.0.0.0:8091 \
  --chain-id 1
```

Configure in phaser-query config.yaml:

```yaml
bridges:
  - name: erigon
    chain_id: 1
    endpoint: /var/run/erigon-bridge.sock  # or localhost:8091
    protocol: grpc
```

## Performance

**Batch Size** - Larger batches improve throughput, increase memory. Respect client's `batch_size_hint` and `max_message_bytes`.

**Compression** - Enable gRPC compression. ZSTD for best ratio, gzip for speed.

**Connection Pooling** - Use pools for concurrent historical queries. Single connection sufficient for live streaming.

**Validation** - Ingestion validation is cheap (decode check). Conversion validation higher cost (schema + constraints). Make configurable.

## Reference Implementation

See erigon-bridge for complete example:
- [`../crates/bridges/evm/erigon-bridge/src/bridge.rs`](../crates/bridges/evm/erigon-bridge/src/bridge.rs) - FlightBridge implementation
- [`../crates/bridges/evm/erigon-bridge/src/converter.rs`](../crates/bridges/evm/erigon-bridge/src/converter.rs) - RLP to Arrow conversion
- [`../crates/bridges/evm/erigon-bridge/src/streaming_service.rs`](../crates/bridges/evm/erigon-bridge/src/streaming_service.rs) - Live streaming
- [`../crates/bridges/evm/erigon-bridge/src/client.rs`](../crates/bridges/evm/erigon-bridge/src/client.rs) - Node client wrapper
