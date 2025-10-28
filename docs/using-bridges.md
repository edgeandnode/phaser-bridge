# Using Phaser Bridges

Phaser bridges translate blockchain node protocols into Arrow Flight streams for efficient data sync.

## Available Bridges

### Erigon Bridge

Connects to Erigon nodes via gRPC to stream EVM blockchain data.

**Supports:**
- Blocks, transactions, logs
- Historical queries (specific block ranges)
- Live streaming (chain head subscription)
- Validation at ingestion and conversion stages

**Requirements:**
- Erigon node with gRPC enabled
- Network access to Erigon gRPC port (default: 9090)

**Connection:**
```yaml
# config.yaml
bridges:
  - name: erigon
    chain_id: 1
    endpoint: localhost:9090
    protocol: grpc
```

Start the bridge:
```bash
erigon-bridge --erigon-grpc localhost:9090
```

The bridge listens on a Unix socket or TCP port for phaser-query to connect.

### JSON-RPC Bridge

Connects to standard Ethereum JSON-RPC endpoints.

**Supports:**
- Blocks, transactions, logs
- Historical queries only (no live streaming)
- Works with any JSON-RPC compatible node (Geth, Nethermind, etc.)

**Requirements:**
- JSON-RPC endpoint (HTTP or WebSocket)
- Archive node for historical data access

**Connection:**
```yaml
bridges:
  - name: rpc
    chain_id: 1
    endpoint: https://eth-mainnet.example.com
    protocol: jsonrpc
```

Start the bridge:
```bash
jsonrpc-bridge --rpc-url https://eth-mainnet.example.com
```

## Data Schemas

All bridges provide data in standardized Arrow schemas:

### Blocks
- `number` (UInt64) - Block number
- `hash` (Binary) - Block hash
- `parent_hash` (Binary)
- `timestamp` (UInt64)
- `gas_used` (UInt64)
- `gas_limit` (UInt64)
- Additional chain-specific fields

### Transactions
- `hash` (Binary)
- `block_number` (UInt64)
- `from` (Binary) - Sender address
- `to` (Binary) - Recipient address
- `value` (Binary) - Amount (U256 as bytes)
- `gas` (UInt64)
- `gas_price` (Binary)
- `nonce` (UInt64)
- `input` (Binary) - Call data

### Logs
- `address` (Binary) - Contract address
- `block_number` (UInt64)
- `transaction_hash` (Binary)
- `log_index` (UInt32)
- `topics` (List<Binary>) - Indexed event parameters
- `data` (Binary) - Non-indexed parameters

## Validation

Bridges can validate data at two stages:

**Ingestion** - Validates raw data from the node
- Checks RLP encoding
- Verifies structural integrity
- Minimal performance impact

**Conversion** - Validates after Arrow conversion
- Ensures schema compliance
- Checks field constraints
- Higher accuracy, slight performance cost

Configure in phaser-query:
```yaml
validation: both  # none, ingestion, conversion, both
```

## Performance Tuning

### Batch Size

Controls how many blocks are fetched per request:
```yaml
batch_size_hint: 100  # Default
```

Larger batches improve throughput but increase memory usage.

### Compression

Enable compression for network transfer:
```yaml
compression: zstd  # none, gzip, zstd
```

ZSTD offers best compression ratio. Gzip is faster.

### Message Size

Maximum message size for large batches:
```yaml
max_message_bytes: 33554432  # 32MB default
```

Increase for blocks with many transactions.

## Monitoring

Bridges expose metrics on :9091/metrics:

- `bridge_blocks_served` - Total blocks streamed
- `bridge_requests_total` - Request count by type
- `bridge_request_duration_seconds` - Request latency
- `bridge_errors_total` - Error count by type

## Troubleshooting

**Bridge fails to connect to node:**
- Verify node gRPC/RPC endpoint is accessible
- Check firewall rules
- Confirm node is synced

**Data validation errors:**
- Check node is not corrupt or desynced
- Try reducing validation level
- Verify schema compatibility with node version

**Slow performance:**
- Increase batch size
- Enable compression
- Check network latency to node
- Verify node can handle request rate
