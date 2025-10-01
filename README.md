# Phaser 🔫

A protocol abstraction layer for blockchain data using Apache Arrow Flight.

## Overview

Phaser provides a standardized Arrow Flight interface for blockchain data, allowing different node implementations (Erigon, Reth, etc.) to expose their data through a common protocol. This separation enables:

- **Protocol independence**: Write data consumers once, use with any blockchain node
- **Stateless bridges**: Node-specific translators with no caching or buffering
- **High-performance streaming**: Zero-copy data transfer using Apache Arrow
- **Flexible deployment**: Bridges can run as separate processes or embedded

### Use Cases

Phaser supports two primary data access patterns:

**Live Streaming** - Real-time blockchain event consumption:
- Subscribe to new blocks, transactions, and logs as they're produced
- Low-latency event delivery for real-time indexing and analytics
- Automatic backpressure handling for consumers

*Why lower latency?* When colocated with the node, bridges can potentially subscribe directly to the node's internal event streams, receiving notifications as soon as blocks are processed—before they're written to disk or exposed via external APIs. This direct subscription path eliminates the polling overhead inherent in request/response protocols like JSON-RPC, where consumers must repeatedly query for new data.

**Historical Queries** - Bulk access to historical blockchain data:
- Query arbitrary block ranges for batch processing
- Parallel streaming of historical data across multiple workers
- Efficient for backfilling indexes or data warehouses
- Example: Sync millions of blocks in minutes vs hours with JSON-RPC

## Architecture

```
    ┌──────────────────────────────────────────────────────────────┐
    │                    Data Consumers                            │
    │  (phaser-query, custom analytics, data pipelines, etc.)      │
    └───────────────────────┬──────────────────────────────────────┘
                            │ Arrow Flight Protocol
                            │
    ┌───────────────────────┴──────────────────────────────────────┐
    │                    phaser-bridge                             │
    │              (Common Flight Interface)                       │
    │  - BlockchainDescriptor: Query specification                 │
    │  - StreamType: Blocks, Transactions, Logs, State             │
    │  - FlightBridge trait: Standard implementation interface     │
    └───────────────────────┬──────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
     ┌──────▼──────┐ ┌──────▼──────┐  ┌─────▼──────┐
     │   erigon-   │ │  jsonrpc-   │  │   ????-    │
     │   bridge    │ │   bridge    │  │   bridge   │
     └──────┬──────┘ └──────┬──────┘  └─────┬──────┘
            │               │               │
     ┌──────▼──────┐ ┌──────▼──────┐  ┌─────▼──────┐
     │   Erigon    │ │  Any JSON-  │  │    ????    │
     │    Node     │ │  RPC Node   │  │    Node    │
     └─────────────┘ └─────────────┘  └────────────┘
```

## Core Components

### phaser-bridge (`crates/phaser-bridge/`)

The core library defining the Arrow Flight protocol abstraction:

- **`FlightBridge` trait**: Interface that bridge implementations must satisfy
- **`BlockchainDescriptor`**: Specifies what data to stream (type, range, filters)
- **`StreamType`**: Blocks, Transactions, Logs, State, etc.
- **`FlightBridgeClient`**: Client for connecting to any bridge implementation
- **`FlightBridgeServer`**: Server wrapper for exposing a bridge via Flight

**Compression Support:**

Arrow Flight supports configurable compression at multiple levels:
- **Transport-level**: gRPC compression (gzip, zstd) can be negotiated between client and server for the entire stream
- **IPC format**: Arrow's IPC format can include compression metadata in FlightData messages
- **RecordBatch streaming**: Bridges can optionally compress RecordBatches before transmission

While not currently exposed in the `BridgeCapabilities`, bridges could advertise supported compression codecs, allowing consumers to request specific compression strategies based on their network/CPU tradeoffs.


### Schema Definitions (`crates/schemas/`)

Each bridge implementation is responsible for defining its Arrow schema that represents the blockchain data model.

Schemas are organized by blockchain flavor (EVM, Solana, etc.).

### Schemas

Define the Arrow schema for chain data. In our evm example (`crates/schemas/evm/common/`), we've used `typed-arrow`.

- **`BlockRecord`**: Block header schema with all EVM block fields
- **`TransactionRecord`**: Transaction schema with signatures, gas, and data
- **`LogRecord`**: Event log schema with topics and data
- **`TrieNodeRecord`**: State trie node schema for state snapshots
- **Domain Type Conversion**: Implements `From` traits to convert [Alloy](https://github.com/alloy-rs/alloy) types (the standard Ethereum library) to typed-arrow schemas

**Data Transformation Flow (EVM Example):**

```
    Blockchain Wire Format          Domain Library              Schema Layer              Arrow Format                Consumer/Storage
    ┌──────────────────┐           ┌──────────────┐          ┌──────────────┐         ┌──────────────┐            ┌──────────────────┐
    │                  │           │              │          │              │         │              │            │                  │
    │   RLP Encoded    │  decode   │    Alloy     │   From   │ BlockRecord  │  Arrow  │ RecordBatch  │  Flight    │  Consumer        │
    │   Block Bytes    │  ──────>  │   Header     │  ──────> │  (typed-     │  ─────> │  (columnar)  │  ──────>   │  - Compression   │
    │                  │           │              │          │   arrow)     │         │              │            │  - Encoding      │
    │  0x48656c6c...   │           │ Header {     │          │ block_num:   │         │ ┌──────────┐ │            │  - Parquet       │
    │                  │           │   number,    │          │   u64,       │         │ │ num | 42 │ │            │    (per-column   │
    │                  │           │   timestamp, │          │ timestamp:   │         │ │ ts  | 99 │ │            │     zstd/snappy) │
    │                  │           │   hash,      │          │   i64,       │         │ │ hash| .. │ │            │  - Analytics     │
    │                  │           │   ...        │          │ hash: Hash32 │         │ └──────────┘ │            │  - Aggregation   │
    │                  │           │ }            │          │ }            │         │              │            │                  │
    └──────────────────┘           └──────────────┘          └──────────────┘         └──────────────┘            └──────────────────┘
         Node Protocol              Rust Types                  Schema                 Flight/Parquet                 Processing
```

**Data flow layers:**
- **Bridge (left)**: Protocol → domain types (Alloy, solana-sdk, etc.)
- **Schema (middle)**: Domain types → Arrow columnar format
- **Consumer (right)**: RecordBatch processing - compression, encoding, storage (Parquet), analytics

The columnar Arrow format enables consumers to apply optimizations like per-column compression (zstd for hashes, dictionary encoding for addresses) and selective column reading.

**Why typed-arrow?**

The `typed-arrow` library (workspace dependency) provides derive macros that automatically generate Arrow schemas from Rust structs:

```rust
#[derive(Record)]
pub struct BlockRecord {
    pub block_num: u64,
    pub timestamp: i64,
    pub hash: Hash32,
    // ... Arrow schema generated automatically
}
```

This ensures:
- Type-safe schema definitions at compile time
- Zero-copy conversions between Rust types and Arrow
- Consistent schema across all bridges for the same blockchain flavor
- Easy schema evolution and versioning

**Adding schemas for new blockchain flavors:**

When implementing a bridge for a non-EVM chain (e.g., Solana, Cosmos):

1. Create `crates/schemas/<flavor>/common/`
2. Define your domain types using `typed-arrow::Record`
3. Implement conversions from your chain's native types (e.g., `solana-sdk` types)
4. Use these schemas in your bridge implementation

### Current Bridge Implementations

#### erigon-bridge (`crates/bridges/erigon-bridge/`)

Translates Erigon's gRPC protocol to Arrow Flight:

- Intended to use a customized version of Erigon [custom BlockDataBackend](https://github.com/edgeandnode/erigon-customized) for historical sync
- Connects to Erigon's private API (gRPC)
- Converts protobuf types to Arrow RecordBatches
- Supports both real-time streaming and historical queries

**Running:**
```bash
# TCP mode
./erigon-bridge --erigon-grpc localhost:9090 --flight-addr 0.0.0.0:8090

# IPC mode (Unix socket)
./erigon-bridge --erigon-grpc localhost:9090 --ipc-path /path/to/bridge.sock
```

#### jsonrpc-bridge (`crates/bridges/jsonrpc-bridge/`)

Translates any Ethereum JSON-RPC node to Arrow Flight:

- Works with any node exposing JSON-RPC (Geth, Nethermind, etc.)
- Converts JSON responses to Arrow RecordBatches
- Supports standard eth_* methods

**Running:**
```bash
./jsonrpc-bridge --rpc-url http://localhost:8545 --flight-addr 0.0.0.0:8090
```

## Bridge Design Principles

Bridges are **stateless protocol translators** that:

- Convert node-specific protocols to Arrow Flight
- Perform zero caching or buffering (consumers handle that)
- Expose a consistent Arrow schema regardless of source
- Run as separate processes for isolation and flexibility

### Why Colocation

Running a bridge colocated with the node (e.g., `erigon-bridge` on the same machine as Erigon) provides significant performance benefits compared to JSON-RPC over the network:

**Protocol Efficiency:**
- **Binary vs Text**: gRPC uses binary Protobuf encoding vs JSON's text format, reducing payload size
- **Streaming**: Native bidirectional streaming for bulk data vs request/response roundtrips
- **Batch Processing**: Arrow Flight sends columnar batches (thousands of rows) vs individual JSON objects

**Network Topology:**
- **Local Communication**: IPC via Unix sockets eliminates network stack overhead
- **Reduced Latency**: Sub-millisecond local communication vs network roundtrip time
- **Higher Bandwidth**: No network bandwidth constraints for large historical syncs

**Data Path:**
- **Zero-Copy Potential**: Arrow's columnar format enables zero-copy transfers when using shared memory transports
- **Efficient Serialization**: Arrow IPC format is designed for direct memory mapping
- **No Double Parsing**: Direct Protobuf→Arrow conversion vs JSON→Object→Arrow

## Example Consumer: phaser-query

`phaser-query` (`crates/phaser-query/`) is an example of a data consumer that uses phaser-bridge:

- Connects to any bridge implementation via Arrow Flight
- Provides JSON-RPC and SQL interfaces for blockchain data
- Writes data to Parquet files with RocksDB indexes
- Manages historical sync jobs with parallel workers

This is just one possible consumer. Other examples:
- Real-time analytics pipelines
- Data warehouses ingesting blockchain data
- Custom indexing solutions
- ML training data preparation

## Building

```bash
# Build all components
cargo build --release

# Build specific bridge
cargo build --release -p erigon-bridge
cargo build --release -p jsonrpc-bridge

# Build example consumer
cargo build --release -p phaser-query
```

## Example: Historical Sync with phaser-query

```bash
# 1. Start Erigon with gRPC enabled
erigon --private.api.addr=0.0.0.0:9090

# 2. Start erigon-bridge
./erigon-bridge --erigon-grpc localhost:9090 --ipc-path /tmp/erigon.sock

# 3. Configure phaser-query (config.yaml)
bridges:
  - chain_id: 1
    name: erigon
    endpoint: /tmp/erigon.sock

# 4. Start phaser-query
./phaser-query -c config.yaml

# 5. Start a sync job via phaser-cli
./phaser-cli sync -c 1 -b erigon -f 0 -t 1000000
```

## Project Structure

```
phaser/
├── crates/
│   ├── phaser-bridge/          # Core Flight protocol abstraction
│   ├── bridges/
│   │   ├── erigon-bridge/      # Erigon → Arrow Flight
│   │   └── jsonrpc-bridge/     # JSON-RPC → Arrow Flight
│   ├── phaser-query/           # Example consumer implementation
│   └── schemas/
│       └── evm/                # Arrow schema definitions for EVM chains
```

## Development Status

This is an early prototype exploring:
- Blockchain data abstraction via Apache Arrow Flight
- Separation of protocol translation from data consumption
- High-performance streaming with zero-copy transfers

## Adding a New Bridge

To implement a bridge for a new node type:

1. **Define your schema** (if not using EVM):
   - Create `crates/schemas/<flavor>/common/`
   - Define domain types with `typed-arrow::Record` derive
   - Implement `From` traits to convert your chain's native types (e.g., `solana-sdk`, `cosmos-sdk`)

2. **Create bridge implementation**:
   - Create a new crate in `crates/bridges/your-bridge/`
   - Implement the `FlightBridge` trait from `phaser-bridge`
   - Convert your node's protocol types to the schema types from step 1
   - Use `typed-arrow` to convert schema types to Arrow RecordBatches

3. **Expose via Flight**:
   - Use `FlightBridgeServer` to expose your bridge

**For EVM bridges**: Reuse `crates/schemas/evm/common/` - no need to define new schemas.

See `erigon-bridge` (uses EVM schema) and `jsonrpc-bridge` for reference implementations.

## License

TODO: Add license information
