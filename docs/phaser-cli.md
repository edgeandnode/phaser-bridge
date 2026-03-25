# phaser-cli - Historical Sync Administration

`phaser-cli` is a command-line tool for managing historical blockchain data synchronization with phaser-query.

## Installation

Build from source:

```bash
cargo build -p phaser-query --bin phaser-cli
```

The binary will be at `./target/debug/phaser-cli` (or `./target/release/phaser-cli` with `--release`).

## Prerequisites

Before using the CLI, you need:

1. **phaser-query running** with sync admin enabled (default port 9093)
2. **erigon-bridge running** connected to a custom Erigon node with BlockDataBackend

Example startup:

```bash
# Start erigon-bridge (connects to custom Erigon)
./target/debug/erigon-bridge \
  --erigon-grpc 192.168.0.174:9090 \
  --flight-addr 0.0.0.0:8090

# Start phaser-query (without live streaming for historical sync)
./target/debug/phaser-query \
  -c config.yaml \
  --disable-streaming \
  --metrics-port 9092
```

## Commands

### sync - Start a Historical Sync Job

Start syncing blockchain data for a range of blocks.

```bash
phaser-cli -e http://127.0.0.1:9093 sync \
  --chain-id 1 \
  --bridge erigon \
  --from 0 \
  --to 24628000
```

**Options:**
- `-c, --chain-id` - Chain ID (must match a configured bridge)
- `-b, --bridge` - Bridge name (as defined in config.yaml)
- `-f, --from` - Starting block number (inclusive)
- `-t, --to` - Ending block number (inclusive)

**Example output:**

```
✓ Sync job started
  Job ID: 9fe2cf40-9f24-4b8c-a866-f57844bfdea4
  Sync job created for blocks 0-24628000 on chain 1 via bridge 'erigon'

Gap Analysis:
  Total segments: 50
  Complete: 0 (0.0%)
  Missing: 50

  50 incomplete segments (showing first 5):
    Segment 0 (blocks 0-499999): missing transactions, logs
    Segment 1 (blocks 500000-999999): missing transactions, logs
    ...
```

### status - Check Sync Job Status

View status of all jobs or a specific job.

**List all jobs:**

```bash
phaser-cli -e http://127.0.0.1:9093 status
```

**View specific job:**

```bash
phaser-cli -e http://127.0.0.1:9093 status 9fe2cf40-9f24-4b8c-a866-f57844bfdea4
```

**Filter by status:**

```bash
phaser-cli -e http://127.0.0.1:9093 status --status RUNNING
```

Valid status filters: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`

**Example output:**

```
Found 1 sync job(s):

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Job ID: 9fe2cf40-9f24-4b8c-a866-f57844bfdea4
Status: RUNNING
Chain: 1 / Bridge: erigon
Blocks: 0-24628000

Data Progress (by segment):
  Blocks:        7/50 segments - 8 files, 907.3 MB (2 gaps)
  Transactions:  0/50 segments - 1 files, 12.5 MB (2 gaps)
  Logs:          0/50 segments - 1 files, 7.6 MB (2 gaps)

Total Files: 10 (927.4 MB)

Complete Segments: 0/50 (0.0% of segments)
Incomplete Segments: 50
  - 43 segments missing blocks
  - 50 segments missing transactions
  - 50 segments missing logs
Active workers: 4
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### cancel - Cancel a Running Sync Job

Stop a sync job in progress.

```bash
phaser-cli -e http://127.0.0.1:9093 cancel 9fe2cf40-9f24-4b8c-a866-f57844bfdea4
```

**Example output:**

```
✓ Sync job cancelled
```

### analyze - Analyze Data Gaps

Check what data is missing without starting a sync.

```bash
phaser-cli -e http://127.0.0.1:9093 analyze \
  --chain-id 1 \
  --bridge erigon \
  --from 0 \
  --to 24628000
```

**Example output:**

```
Analyzed blocks 0-24628000 for chain 1 via bridge 'erigon'

Gap Analysis:
  Total segments: 50
  Complete: 7 (14.0%)
  Missing: 43

  Incomplete segments:
    Segment 0 (blocks 0-499999):
      - blocks:
        0-499999 (500000 blocks)
      - transactions:
        0-499999 (500000 blocks)
    ...

  Segments to sync: [0, 1, 2, 3, 4, 5, ...]
```

## Concepts

### Segments

Data is organized into segments of 500,000 blocks each (aligned with Erigon snapshots):

- Segment 0: blocks 0 - 499,999
- Segment 1: blocks 500,000 - 999,999
- Segment 2: blocks 1,000,000 - 1,499,999
- etc.

A segment is "complete" when it has all three data types: blocks, transactions, and logs.

### Data Types

Each segment contains three types of data:

- **Blocks**: Block headers (hash, number, timestamp, gas, etc.)
- **Transactions**: Transaction data with sender addresses
- **Logs**: Event logs from contract execution

### Workers

Sync jobs run with multiple parallel workers (default: 4). Each worker syncs one segment at a time.

## Configuration

phaser-query reads configuration from a YAML file:

```yaml
# config.yaml
rocksdb_path: ./data/rocksdb
data_root: ./data

bridges:
  - chain_id: 1
    endpoint: http://127.0.0.1:8090
    name: erigon

segment_size: 500000
sync_admin_port: 9093
sync_parallelism: 4
```

## Monitoring

### Prometheus Metrics

phaser-query exposes metrics on port 9092 (configurable):

```bash
curl http://localhost:9092/metrics
```

### Log Files

When running with output redirection:

```bash
./target/debug/phaser-query -c config.yaml > ./logs/phaser-query.log 2>&1 &
```

Monitor sync progress:

```bash
tail -f ./logs/phaser-query.log | grep -E "segment|sync|worker"
```

## Troubleshooting

### "Live streaming is enabled, waiting for boundary..."

This happens when phaser-query is started with live streaming enabled but the Erigon node isn't receiving new blocks. Either:

1. Wait for a new block (~12 seconds on mainnet)
2. Restart phaser-query with `--disable-streaming`

### Connection refused

Make sure phaser-query's sync admin gRPC server is running:

```bash
# Check port 9093 is listening
lsof -i :9093
```

### No sync jobs found

The sync job may have completed or failed. Check phaser-query logs:

```bash
tail -100 ./logs/phaser-query.log | grep -E "sync|error|fail"
```

## Examples

### Full Mainnet Sync

Sync the entire Ethereum mainnet history:

```bash
# Get latest block number
LATEST=$(curl -s http://localhost:8545 -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  | jq -r '.result' | xargs printf "%d")

# Start sync
phaser-cli -e http://127.0.0.1:9093 sync \
  --chain-id 1 \
  --bridge erigon \
  --from 0 \
  --to $LATEST
```

### Monitor Progress

Watch sync progress in a loop:

```bash
watch -n 5 './target/debug/phaser-cli -e http://127.0.0.1:9093 status'
```

### Resume After Failure

If a sync job fails, just start a new one - it will automatically detect existing data and only sync missing segments:

```bash
phaser-cli -e http://127.0.0.1:9093 sync \
  --chain-id 1 \
  --bridge erigon \
  --from 0 \
  --to 24628000
```

The gap analysis will show which segments still need syncing.
