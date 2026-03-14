# Full Chain Sync Testing Guide

This guide documents how to test phaser-query with a full Ethereum mainnet archive sync.

## Infrastructure Requirements

### Storage Requirements

Erigon archive data requires ~2 TB of storage.

### Server Setup

The target server needs:
- High I/O throughput (NVMe recommended)
- 3+ TB available storage
- Lighthouse beacon node for consensus layer (erigon runs with `--externalcl`)

**Directory layout on target server:**
```
$REMOTE_ROOT/                           # Default: /mnt/nvme-raid
├── erigon-customized/
│   ├── bin/
│   │   └── erigon              # Custom erigon with BlockDataBackend
│   ├── data/                   # Erigon archive data (~2 TB)
│   │   └── jwt.hex
│   ├── logs/
│   │   └── erigon.log
│   └── scripts/
│       └── start-erigon.sh
│
├── phaser/
│   ├── bin/
│   │   ├── erigon-bridge-{debug,release}
│   │   ├── phaser-query-{debug,release}
│   │   └── phaser-cli-{debug,release}
│   ├── config/
│   │   └── config.yaml
│   ├── data/
│   │   └── 1/erigon/           # Parquet output
│   ├── logs/
│   │   ├── erigon-bridge.log
│   │   └── phaser-query.log
│   └── scripts/
│       └── run-full-sync.sh
│
└── lighthouse-beacon/          # Consensus layer (for erigon)
```

## Prerequisites

### 1. Build Binaries

Build both debug and release versions locally:

```bash
# Release builds (for performance testing)
cargo build --release -p erigon-bridge -p phaser-query

# Debug builds (for debugging)
cargo build -p erigon-bridge -p phaser-query
cargo build -p phaser-query --bin phaser-cli

# Verify debug info present in release builds
file ./target/release/erigon-bridge
# Should show: "with debug_info, not stripped"
```

### 2. Deploy to Server

Use the deploy script:

```bash
# Deploy everything to a host
./scripts/deploy-superserver.sh myserver

# Or use environment variable
DEPLOY_HOST=myserver ./scripts/deploy-superserver.sh --all

# Deploy selectively
./scripts/deploy-superserver.sh myserver --phaser   # Just phaser binaries
./scripts/deploy-superserver.sh myserver --erigon   # Just custom erigon
./scripts/deploy-superserver.sh myserver --scripts  # Just scripts
./scripts/deploy-superserver.sh myserver --config   # Just config

# Custom remote root (default: /mnt/nvme-raid)
REMOTE_ROOT=/data ./scripts/deploy-superserver.sh myserver --all
```

### 3. Migrate Existing Erigon Data (if needed)

If you have existing erigon archive data:

```bash
# Option 1: Symlink existing data
ssh $HOST "ln -s /path/to/existing/erigon /mnt/nvme-raid/erigon-customized/data"

# Option 2: Copy JWT if using existing data
ssh $HOST "cp /path/to/existing/jwt.hex /mnt/nvme-raid/erigon-customized/data/"
```

## Running the Sync

### Step 1: Start Erigon (separate process)

Erigon runs independently from the phaser sync. Start it first:

```bash
ssh $HOST

# Start erigon (default path: /mnt/nvme-raid/erigon-customized)
/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh

# Or with custom path
ERIGON_ROOT=/data/erigon-customized ./scripts/start-erigon.sh

# Check status
/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh --status

# View logs
tail -f /mnt/nvme-raid/erigon-customized/logs/erigon.log

# Stop (when done)
/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh --stop
```

The erigon start script:
- Checks binary exists
- Creates JWT secret if missing
- Rotates logs (keeps last 3)
- Runs with archive mode and BlockDataBackend gRPC on port 9091

### Step 2: Start Phaser Sync

Once erigon is running:

```bash
ssh $HOST
cd /mnt/nvme-raid/phaser

# Fresh sync with release builds (default)
./scripts/run-full-sync.sh --clean

# Or with debug builds
./scripts/run-full-sync.sh --clean --debug

# Resume existing sync
./scripts/run-full-sync.sh --release

# Custom block range
./scripts/run-full-sync.sh --from 0 --to 10000000 --debug

# Custom paths via environment
PHASER_ROOT=/data/phaser ERIGON_ROOT=/data/erigon ./scripts/run-full-sync.sh --clean

# Show help
./scripts/run-full-sync.sh --help
```

The sync script handles:
- Log rotation (keeps last 5 runs)
- Data cleanup with `--clean`
- Prerequisite checks (binaries, config, erigon connectivity, disk space)
- Service startup with `tee` for logging
- Optional monitoring loop

### Stopping Services

```bash
# Stop phaser (Ctrl+C or)
pkill -f 'erigon-bridge-' && pkill -f 'phaser-query-'

# Stop erigon (separately)
/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh --stop
```

## Monitoring

### From Local Machine (via SSH)

```bash
HOST=myserver

# Check sync status
ssh $HOST "/mnt/nvme-raid/phaser/bin/phaser-cli-release -e http://127.0.0.1:9093 status"

# Watch logs
ssh $HOST "tail -f /mnt/nvme-raid/phaser/logs/phaser-query.log"

# Check erigon status
ssh $HOST "/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh --status"

# Check data growth
ssh $HOST "du -sh /mnt/nvme-raid/phaser/data/1/erigon/"

# Count parquet files
ssh $HOST "ls /mnt/nvme-raid/phaser/data/1/erigon/*.parquet 2>/dev/null | wc -l"

# Check disk space
ssh $HOST "df -h /mnt/nvme-raid"
```

### Prometheus Metrics

```bash
ssh $HOST "curl -s http://localhost:9092/metrics | grep phaser_query"
```

## Troubleshooting

### "Erigon not reachable"
Erigon must be started separately before running the sync:
```bash
/mnt/nvme-raid/erigon-customized/scripts/start-erigon.sh
```

### "no transactions snapshot file"
Erigon is pruned. Need archive node with `--prune.mode=archive`.

### "erigon-bridge-release not found"
Deploy the binaries with proper suffixes:
```bash
./scripts/deploy-superserver.sh $HOST --phaser
```

### Connection refused to bridge
Check erigon-bridge is running: `pgrep -a erigon-bridge`

### High memory usage
- Reduce `sync_parallelism` in config (default 4)
- Check for stuck workers

### Disk full
Monitor with `df -h` - need 3+ TB free

### Logs filling up
Rotate logs: `mv logs/phaser-query.log logs/phaser-query.log.1`

## Verifying Data Quality

After sync, verify early blocks have transaction data:

```bash
ssh $HOST "python3 << 'EOF'
import pyarrow.parquet as pq

# Check transactions for early blocks
txs = pq.read_table('/mnt/nvme-raid/phaser/data/1/erigon/transactions_from_0_to_499999_0.parquet')
print(f'Transactions: {len(txs)} rows')
print(f'First block with tx: {min(txs[\"block_num\"].to_pylist())}')
# Should be block 46147 (first tx on mainnet)
EOF"
```
