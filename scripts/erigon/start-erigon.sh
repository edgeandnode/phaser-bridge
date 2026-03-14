#!/bin/bash
# Start custom Erigon archive node for phaser sync testing
# Logs to file with rotation
#
# Usage: ./start-erigon.sh [OPTIONS]
#   --stop          Stop running erigon
#   --status        Check if running
#   --help          Show this help
#
# Environment (auto-loaded from config/env.sh if present):
#   ERIGON_ROOT     Base path (default: /mnt/nvme-raid/erigon-customized)
set -e

# Find script directory and source env.sh if present
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -f "$SCRIPT_DIR/../config/env.sh" ]]; then
    source "$SCRIPT_DIR/../config/env.sh"
fi

# Configurable paths (override via environment or env.sh)
ERIGON_ROOT="${ERIGON_ROOT:-/mnt/nvme-raid/erigon-customized}"

# Derived paths
BIN_DIR="$ERIGON_ROOT/bin"
LIB_DIR="$ERIGON_ROOT/lib"
DATA_DIR="$ERIGON_ROOT/data"
LOG_DIR="$ERIGON_ROOT/logs"

# Add lib dir to library path (for libsilkworm_capi.so)
export LD_LIBRARY_PATH="${LIB_DIR}:${LD_LIBRARY_PATH:-}"

# Ports (avoid conflict with any systemd erigon services)
HTTP_PORT="8546"
WS_PORT="8547"
GRPC_PORT="9091"
AUTH_PORT="8552"
METRICS_PORT="6061"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; exit 1; }

# Ensure directories exist
mkdir -p "$LOG_DIR" "$DATA_DIR"

# Check if erigon is running (our custom one)
is_running() {
    pgrep -f "$BIN_DIR/erigon.*--private.api.addr.*$GRPC_PORT" > /dev/null 2>&1
}

get_pid() {
    pgrep -f "$BIN_DIR/erigon.*--private.api.addr.*$GRPC_PORT" 2>/dev/null || echo ""
}

# Stop erigon
stop_erigon() {
    if is_running; then
        local pid=$(get_pid)
        log "Stopping erigon (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
        sleep 3
        if is_running; then
            warn "Erigon didn't stop gracefully, forcing..."
            kill -9 "$pid" 2>/dev/null || true
            sleep 1
        fi
        log "Erigon stopped"
    else
        log "Erigon not running"
    fi
}

# Show status
show_status() {
    if is_running; then
        local pid=$(get_pid)
        echo -e "${GREEN}Erigon is running${NC} (PID: $pid)"
        echo ""
        echo "Ports:"
        echo "  HTTP RPC:    http://127.0.0.1:$HTTP_PORT"
        echo "  WebSocket:   ws://127.0.0.1:$WS_PORT"
        echo "  gRPC:        127.0.0.1:$GRPC_PORT"
        echo "  Metrics:     http://127.0.0.1:$METRICS_PORT"
        echo ""
        echo "Logs: $LOG_DIR/erigon.log"
        echo ""
        # Show current block
        local block=$(curl -s "http://127.0.0.1:$HTTP_PORT" -X POST \
            -H "Content-Type: application/json" \
            -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' 2>/dev/null \
            | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result'],16))" 2>/dev/null || echo "unknown")
        echo "Current block: $block"
    else
        echo -e "${YELLOW}Erigon is not running${NC}"
        return 1
    fi
}

# Rotate logs
rotate_logs() {
    local max_rotations=3
    if [[ -f "$LOG_DIR/erigon.log" ]]; then
        for i in $(seq $((max_rotations-1)) -1 1); do
            if [[ -f "$LOG_DIR/erigon.log.$i" ]]; then
                mv "$LOG_DIR/erigon.log.$i" "$LOG_DIR/erigon.log.$((i+1))"
            fi
        done
        mv "$LOG_DIR/erigon.log" "$LOG_DIR/erigon.log.1"
        rm -f "$LOG_DIR/erigon.log.$((max_rotations+1))" 2>/dev/null || true
    fi
}

# Start erigon
start_erigon() {
    if is_running; then
        warn "Erigon already running (PID: $(get_pid))"
        show_status
        return 0
    fi

    # Check binary exists
    [[ -f "$BIN_DIR/erigon" ]] || error "Erigon binary not found at $BIN_DIR/erigon"
    [[ -x "$BIN_DIR/erigon" ]] || chmod +x "$BIN_DIR/erigon"

    # Check/create JWT secret
    if [[ ! -f "$DATA_DIR/jwt.hex" ]]; then
        log "Generating JWT secret..."
        openssl rand -hex 32 > "$DATA_DIR/jwt.hex"
    fi

    rotate_logs

    log "Starting erigon archive node..."
    log "  Root:     $ERIGON_ROOT"
    log "  Data dir: $DATA_DIR"
    log "  gRPC:     0.0.0.0:$GRPC_PORT"
    log "  HTTP:     0.0.0.0:$HTTP_PORT"

    # Start erigon with all args from systemd service
    "$BIN_DIR/erigon" \
        --datadir="$DATA_DIR" \
        --chain=mainnet \
        --prune.mode=archive \
        --authrpc.jwtsecret="$DATA_DIR/jwt.hex" \
        --authrpc.addr=0.0.0.0 \
        --authrpc.port="$AUTH_PORT" \
        --http \
        --http.addr=0.0.0.0 \
        --http.port="$HTTP_PORT" \
        --http.vhosts='*' \
        --http.corsdomain='*' \
        --http.api=eth,debug,net,trace,web3,erigon \
        --ws \
        --ws.port="$WS_PORT" \
        --private.api.addr=0.0.0.0:"$GRPC_PORT" \
        --metrics \
        --metrics.addr=0.0.0.0 \
        --metrics.port="$METRICS_PORT" \
        --torrent.download.rate=62mb \
        --torrent.upload.rate=10mb \
        --maxpeers=50 \
        --db.size.limit=4TB \
        --externalcl \
        2>&1 | tee "$LOG_DIR/erigon.log" &

    sleep 5

    if is_running; then
        log "Erigon started (PID: $(get_pid))"
        echo ""
        show_status
    else
        error "Erigon failed to start. Check $LOG_DIR/erigon.log"
    fi
}

# Parse args
case "${1:-start}" in
    --stop|-s)
        stop_erigon
        ;;
    --status|-t)
        show_status
        ;;
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --stop, -s      Stop running erigon"
        echo "  --status, -t    Check if running"
        echo "  --help, -h      Show this help"
        echo ""
        echo "Environment:"
        echo "  ERIGON_ROOT     Base path (default: /mnt/nvme-raid/erigon-customized)"
        echo ""
        echo "Paths:"
        echo "  Binary:   $BIN_DIR/erigon"
        echo "  Data:     $DATA_DIR"
        echo "  Logs:     $LOG_DIR"
        ;;
    start|"")
        start_erigon
        ;;
    *)
        echo "Unknown option: $1"
        exit 1
        ;;
esac
