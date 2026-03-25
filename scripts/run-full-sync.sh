#!/bin/bash
# Full chain sync automation script
# Runs on the target server, writes to configured data directory
#
# Usage: ./run-full-sync.sh [OPTIONS]
#   --from BLOCK    Starting block (default: 0)
#   --to BLOCK      Ending block (default: latest from erigon)
#   --clean         Remove existing data before starting
#   --debug         Use debug builds
#   --release       Use release builds (default)
#   --help          Show this help
#
# Environment (auto-loaded from config/env.sh if present):
#   PHASER_ROOT     Base path for phaser (default: /mnt/nvme-raid/phaser)
#   ERIGON_ROOT     Base path for erigon (default: /mnt/nvme-raid/erigon-customized)
#   BUILD_TYPE      "release" or "debug" (default: release)
#
# Prerequisites:
#   Erigon must be running separately (use start-erigon.sh)
set -e

# Find script directory and source env.sh if present
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -f "$SCRIPT_DIR/../config/env.sh" ]]; then
    source "$SCRIPT_DIR/../config/env.sh"
fi

# Configurable paths (override via environment or env.sh)
PHASER_ROOT="${PHASER_ROOT:-/mnt/nvme-raid/phaser}"
ERIGON_ROOT="${ERIGON_ROOT:-/mnt/nvme-raid/erigon-customized}"

# Derived paths
BIN_DIR="$PHASER_ROOT/bin"
DATA_DIR="$PHASER_ROOT/data"
CONFIG_FILE="$PHASER_ROOT/config/config.yaml"
LOG_DIR="$PHASER_ROOT/logs"

# Build type: "release" or "debug"
BUILD_TYPE="${BUILD_TYPE:-release}"

# Ports
ERIGON_GRPC="127.0.0.1:9091"
ERIGON_HTTP="127.0.0.1:8546"
BRIDGE_PORT="8090"
BRIDGE_METRICS_PORT="9094"
ADMIN_PORT="9093"
METRICS_PORT="9092"

# Defaults
FROM_BLOCK="${FROM_BLOCK:-0}"
TO_BLOCK="${TO_BLOCK:-}"
CLEAN_DATA=false

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --from) FROM_BLOCK="$2"; shift 2 ;;
        --to) TO_BLOCK="$2"; shift 2 ;;
        --clean) CLEAN_DATA=true; shift ;;
        --debug) BUILD_TYPE="debug"; shift ;;
        --release) BUILD_TYPE="release"; shift ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --from BLOCK    Starting block (default: 0)"
            echo "  --to BLOCK      Ending block (default: latest from erigon)"
            echo "  --clean         Remove existing data before starting"
            echo "  --debug         Use debug builds"
            echo "  --release       Use release builds (default)"
            echo ""
            echo "Environment:"
            echo "  PHASER_ROOT     Base path (default: /mnt/nvme-raid/phaser)"
            echo "  ERIGON_ROOT     Erigon path (default: /mnt/nvme-raid/erigon-customized)"
            echo "  BUILD_TYPE      'release' or 'debug' (default: release)"
            echo ""
            echo "Paths:"
            echo "  Binaries: $BIN_DIR/*-{debug,release}"
            echo "  Data:     $DATA_DIR"
            echo "  Config:   $CONFIG_FILE"
            echo "  Logs:     $LOG_DIR"
            echo ""
            echo "Prerequisites:"
            echo "  Erigon must be running separately:"
            echo "    $ERIGON_ROOT/scripts/start-erigon.sh"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Binary names with build type suffix
ERIGON_BRIDGE="$BIN_DIR/erigon-bridge-$BUILD_TYPE"
PHASER_QUERY="$BIN_DIR/phaser-query-$BUILD_TYPE"
PHASER_CLI="$BIN_DIR/phaser-cli-$BUILD_TYPE"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }

# Ensure directories exist
mkdir -p "$LOG_DIR" "$DATA_DIR/1/erigon"

# Cleanup function
cleanup() {
    log "Stopping phaser processes..."
    pkill -f "erigon-bridge-$BUILD_TYPE.*--flight-addr.*$BRIDGE_PORT" 2>/dev/null || true
    pkill -f "phaser-query-$BUILD_TYPE.*--metrics-port.*$METRICS_PORT" 2>/dev/null || true
}

handle_interrupt() {
    warn "Interrupted by user"
    cleanup
    exit 130
}

trap handle_interrupt SIGINT SIGTERM

# Clean data if requested
clean_data() {
    log "Cleaning existing data..."
    rm -rf "$DATA_DIR/1/erigon/"*.parquet 2>/dev/null || true
    rm -rf "$DATA_DIR/1/erigon/"*.tmp 2>/dev/null || true
    rm -rf "$DATA_DIR/rocksdb" 2>/dev/null || true
    log "Data cleaned"
}

# Rotate logs
rotate_logs() {
    local max_rotations=5

    for logfile in erigon-bridge.log phaser-query.log; do
        if [[ -f "$LOG_DIR/$logfile" ]]; then
            # Shift old logs
            for i in $(seq $((max_rotations-1)) -1 1); do
                if [[ -f "$LOG_DIR/${logfile}.$i" ]]; then
                    mv "$LOG_DIR/${logfile}.$i" "$LOG_DIR/${logfile}.$((i+1))"
                fi
            done
            mv "$LOG_DIR/$logfile" "$LOG_DIR/${logfile}.1"
        fi
    done

    # Remove old rotations
    for logfile in erigon-bridge.log phaser-query.log; do
        rm -f "$LOG_DIR/${logfile}.$((max_rotations+1))" 2>/dev/null || true
    done

    log "Rotated logs (keeping last $max_rotations)"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    log "Build type: $BUILD_TYPE"

    # Check binaries
    [[ -f "$ERIGON_BRIDGE" ]] || error "erigon-bridge-$BUILD_TYPE not found at $BIN_DIR"
    [[ -f "$PHASER_QUERY" ]] || error "phaser-query-$BUILD_TYPE not found at $BIN_DIR"
    [[ -f "$PHASER_CLI" ]] || error "phaser-cli-$BUILD_TYPE not found at $BIN_DIR"
    [[ -f "$CONFIG_FILE" ]] || error "Config not found at $CONFIG_FILE"

    # Check erigon is running (must be started separately)
    if ! curl -s --connect-timeout 5 "http://$ERIGON_HTTP" -X POST \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        | grep -q "result"; then
        error "Erigon not reachable at $ERIGON_HTTP. Start it first with: $ERIGON_ROOT/scripts/start-erigon.sh"
    fi

    # Check disk space
    local avail_gb=$(df -BG "$DATA_DIR" | tail -1 | awk '{print $4}' | tr -d 'G')
    if [[ $avail_gb -lt 2000 ]]; then
        warn "Low disk space: ${avail_gb}GB available (recommend 3TB+)"
    fi

    log "Prerequisites OK (${avail_gb}GB available)"
}

# Get latest block
get_latest_block() {
    curl -s "http://$ERIGON_HTTP" -X POST \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result'],16))"
}

# Start erigon-bridge with tee
start_bridge() {
    log "Starting erigon-bridge-$BUILD_TYPE..."

    pkill -f "erigon-bridge-$BUILD_TYPE.*--flight-addr.*$BRIDGE_PORT" 2>/dev/null || true
    sleep 1

    "$ERIGON_BRIDGE" \
        --erigon-grpc "$ERIGON_GRPC" \
        --flight-addr "0.0.0.0:$BRIDGE_PORT" \
        --metrics-port "$BRIDGE_METRICS_PORT" \
        2>&1 | tee "$LOG_DIR/erigon-bridge.log" &
    BRIDGE_PID=$!

    sleep 5

    if ! pgrep -f "erigon-bridge-$BUILD_TYPE.*--flight-addr.*$BRIDGE_PORT" > /dev/null; then
        error "erigon-bridge failed to start"
    fi

    if grep -q "BlockDataBackend service is available" "$LOG_DIR/erigon-bridge.log" 2>/dev/null; then
        log "BlockDataBackend connection confirmed"
    else
        warn "BlockDataBackend not confirmed yet"
    fi

    log "erigon-bridge started"
}

# Start phaser-query with tee
start_query() {
    log "Starting phaser-query-$BUILD_TYPE..."

    pkill -f "phaser-query-$BUILD_TYPE.*--metrics-port.*$METRICS_PORT" 2>/dev/null || true
    sleep 1

    rm -f "$DATA_DIR/rocksdb/LOCK" 2>/dev/null

    "$PHASER_QUERY" \
        -c "$CONFIG_FILE" \
        --disable-streaming \
        --metrics-port "$METRICS_PORT" \
        2>&1 | tee "$LOG_DIR/phaser-query.log" &
    QUERY_PID=$!

    sleep 3

    if ! pgrep -f "phaser-query-$BUILD_TYPE.*--metrics-port.*$METRICS_PORT" > /dev/null; then
        error "phaser-query failed to start"
    fi

    log "phaser-query started"
}

# Start sync
start_sync() {
    local from=$1
    local to=$2

    log "Starting sync (blocks $from to $to)..."

    "$PHASER_CLI" -e "http://127.0.0.1:$ADMIN_PORT" sync \
        --chain-id 1 \
        --bridge erigon \
        --from "$from" \
        --to "$to"
}

# Show status
show_status() {
    "$PHASER_CLI" -e "http://127.0.0.1:$ADMIN_PORT" status 2>/dev/null
}

# Main
main() {
    info "========================================"
    info "   Phaser Full Chain Sync"
    info "========================================"
    info "Build:       $BUILD_TYPE"
    info "Phaser root: $PHASER_ROOT"
    info "Erigon gRPC: $ERIGON_GRPC"
    info "Data dir:    $DATA_DIR"
    info "Logs:        $LOG_DIR"
    info "Clean:       $CLEAN_DATA"
    echo

    check_prerequisites

    if [[ "$CLEAN_DATA" == true ]]; then
        clean_data
    fi

    rotate_logs

    # Get latest block if not specified
    if [[ -z "$TO_BLOCK" ]]; then
        TO_BLOCK=$(get_latest_block)
    fi

    log "Sync range: $FROM_BLOCK to $TO_BLOCK"

    start_bridge
    start_query

    sleep 2

    start_sync "$FROM_BLOCK" "$TO_BLOCK"

    echo
    log "Sync job started!"
    echo
    info "Monitor with:"
    echo "  $PHASER_CLI -e http://127.0.0.1:$ADMIN_PORT status"
    echo "  tail -f $LOG_DIR/phaser-query.log"
    echo "  du -sh $DATA_DIR/1/erigon/"
    echo
    info "Running processes:"
    pgrep -a "erigon-bridge\|phaser-query" | head -5
    echo

    # Monitoring loop
    if [[ -t 0 ]]; then
        read -p "Enter monitoring loop? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            while true; do
                clear
                echo "=== $(date) ==="
                echo
                show_status || echo "Status unavailable"
                echo
                echo "Data size: $(du -sh "$DATA_DIR/1/erigon/" 2>/dev/null | cut -f1)"
                echo "Disk free: $(df -h "$DATA_DIR" | tail -1 | awk '{print $4}')"
                echo
                echo "Press Ctrl+C to exit (processes continue in background)"
                sleep 30
            done
        fi
    fi
}

main "$@"
