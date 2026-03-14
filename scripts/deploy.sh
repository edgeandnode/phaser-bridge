#!/bin/bash
# Deploy phaser and custom erigon binaries to a remote server
#
# Usage: ./scripts/deploy.sh [OPTIONS] HOST
#   HOST              SSH host to deploy to (required)
#   --env FILE        env.sh file to deploy (required for --config)
#   --all             Deploy everything
#   --phaser          Deploy only phaser binaries
#   --erigon          Deploy only custom erigon
#   --scripts         Deploy only scripts
#   --config          Deploy config files (requires --env)
#   --help            Show this help
#
# Example:
#   # Generate env.sh first
#   ./scripts/generate-env.sh --phaser-root /mnt/nvme-raid/phaser \
#     --erigon-root /mnt/nvme-raid/erigon-customized -o env.sh
#
#   # Then deploy
#   ./scripts/deploy.sh myserver --env env.sh --all
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
ERIGON_REPO="$REPO_DIR/../erigon"

# Settings
REMOTE=""
ENV_FILE=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[deploy]${NC} $1"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $1"; }
error() { echo -e "${RED}[deploy]${NC} $1"; exit 1; }

# What to deploy
DEPLOY_PHASER=false
DEPLOY_ERIGON=false
DEPLOY_SCRIPTS=false
DEPLOY_CONFIG=false

# Parse args
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            DEPLOY_PHASER=true
            DEPLOY_ERIGON=true
            DEPLOY_SCRIPTS=true
            DEPLOY_CONFIG=true
            shift ;;
        --phaser) DEPLOY_PHASER=true; shift ;;
        --erigon) DEPLOY_ERIGON=true; shift ;;
        --scripts) DEPLOY_SCRIPTS=true; shift ;;
        --config) DEPLOY_CONFIG=true; shift ;;
        --env) ENV_FILE="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 [OPTIONS] HOST"
            echo ""
            echo "Arguments:"
            echo "  HOST              SSH host to deploy to (required)"
            echo ""
            echo "Options:"
            echo "  --env FILE        env.sh file to deploy (required for --config)"
            echo "  --all             Deploy everything"
            echo "  --phaser          Deploy only phaser binaries"
            echo "  --erigon          Deploy only custom erigon"
            echo "  --scripts         Deploy only scripts"
            echo "  --config          Deploy config files (requires --env)"
            echo ""
            echo "Example:"
            echo "  # Generate env.sh first"
            echo "  ./scripts/generate-env.sh --phaser-root /mnt/nvme-raid/phaser \\"
            echo "    --erigon-root /mnt/nvme-raid/erigon-customized -o env.sh"
            echo ""
            echo "  # Then deploy"
            echo "  $0 myserver --env env.sh --all"
            exit 0 ;;
        -*) error "Unknown option: $1" ;;
        *) POSITIONAL_ARGS+=("$1"); shift ;;
    esac
done

# Get host from positional arg
if [[ ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
    REMOTE="${POSITIONAL_ARGS[0]}"
fi

# Require host
if [[ -z "$REMOTE" ]]; then
    error "No host specified. Usage: $0 [OPTIONS] HOST"
fi

# Default to deploying phaser + erigon + scripts if nothing specified
if [[ "$DEPLOY_PHASER" == false && "$DEPLOY_ERIGON" == false && "$DEPLOY_SCRIPTS" == false && "$DEPLOY_CONFIG" == false ]]; then
    DEPLOY_PHASER=true
    DEPLOY_ERIGON=true
    DEPLOY_SCRIPTS=true
fi

# If deploying config, require env file
if [[ "$DEPLOY_CONFIG" == true && -z "$ENV_FILE" ]]; then
    error "--config requires --env FILE. Generate one with: ./scripts/generate-env.sh --help"
fi

# Load paths from env file
if [[ -n "$ENV_FILE" ]]; then
    if [[ ! -f "$ENV_FILE" ]]; then
        error "env file not found: $ENV_FILE"
    fi
    source "$ENV_FILE"
fi

# Remote paths (from env file or defaults)
PHASER_DIR="${PHASER_ROOT:-/mnt/nvme-raid/phaser}"
ERIGON_DIR="${ERIGON_ROOT:-/mnt/nvme-raid/erigon-customized}"

# Create remote directories
create_dirs() {
    log "Creating remote directories on $REMOTE..."
    ssh "$REMOTE" "mkdir -p $ERIGON_DIR/{bin,config,data,logs,scripts}"
    ssh "$REMOTE" "mkdir -p $PHASER_DIR/{bin,config,data/1/erigon,logs,scripts}"
}

# Deploy phaser binaries
deploy_phaser() {
    log "Deploying phaser binaries..."

    # Check release builds exist
    if [[ ! -f "$REPO_DIR/target/release/erigon-bridge" ]]; then
        warn "Release builds not found. Building..."
        (cd "$REPO_DIR" && cargo build --release -p erigon-bridge -p phaser-query)
    fi

    # Check debug builds exist
    if [[ ! -f "$REPO_DIR/target/debug/erigon-bridge" ]]; then
        warn "Debug builds not found. Building..."
        (cd "$REPO_DIR" && cargo build -p erigon-bridge -p phaser-query)
    fi

    # Check CLI
    if [[ ! -f "$REPO_DIR/target/debug/phaser-cli" ]]; then
        warn "phaser-cli not found. Building..."
        (cd "$REPO_DIR" && cargo build -p phaser-query --bin phaser-cli)
    fi

    # Copy release binaries
    log "  erigon-bridge-release"
    scp "$REPO_DIR/target/release/erigon-bridge" "$REMOTE:$PHASER_DIR/bin/erigon-bridge-release"
    log "  phaser-query-release"
    scp "$REPO_DIR/target/release/phaser-query" "$REMOTE:$PHASER_DIR/bin/phaser-query-release"

    # Copy debug binaries
    log "  erigon-bridge-debug"
    scp "$REPO_DIR/target/debug/erigon-bridge" "$REMOTE:$PHASER_DIR/bin/erigon-bridge-debug"
    log "  phaser-query-debug"
    scp "$REPO_DIR/target/debug/phaser-query" "$REMOTE:$PHASER_DIR/bin/phaser-query-debug"

    # CLI (same for both, use debug build)
    log "  phaser-cli-{debug,release}"
    scp "$REPO_DIR/target/debug/phaser-cli" "$REMOTE:$PHASER_DIR/bin/phaser-cli-debug"
    scp "$REPO_DIR/target/debug/phaser-cli" "$REMOTE:$PHASER_DIR/bin/phaser-cli-release"

    log "Phaser binaries deployed"
}

# Deploy custom erigon
deploy_erigon() {
    log "Deploying custom erigon..."

    if [[ ! -f "$ERIGON_REPO/build/bin/erigon" ]]; then
        error "Custom erigon not found at $ERIGON_REPO/build/bin/erigon"
    fi

    log "  erigon"
    scp "$ERIGON_REPO/build/bin/erigon" "$REMOTE:$ERIGON_DIR/bin/"
    ssh "$REMOTE" "chmod +x $ERIGON_DIR/bin/erigon"

    log "Custom erigon deployed"
}

# Deploy scripts
deploy_scripts() {
    log "Deploying scripts..."

    # Phaser scripts
    log "  run-full-sync.sh"
    scp "$REPO_DIR/scripts/run-full-sync.sh" "$REMOTE:$PHASER_DIR/scripts/"
    ssh "$REMOTE" "chmod +x $PHASER_DIR/scripts/*.sh"

    # Erigon scripts
    log "  start-erigon.sh"
    scp "$REPO_DIR/scripts/erigon/start-erigon.sh" "$REMOTE:$ERIGON_DIR/scripts/"
    ssh "$REMOTE" "chmod +x $ERIGON_DIR/scripts/*.sh"

    log "Scripts deployed"
}

# Deploy config
deploy_config() {
    log "Deploying config..."

    # Deploy env.sh to both locations
    log "  env.sh -> phaser/config/"
    scp "$ENV_FILE" "$REMOTE:$PHASER_DIR/config/env.sh"
    log "  env.sh -> erigon-customized/config/"
    scp "$ENV_FILE" "$REMOTE:$ERIGON_DIR/config/env.sh"

    # Deploy phaser config.yaml
    log "  config.yaml"
    ssh "$REMOTE" "cat > $PHASER_DIR/config/config.yaml << EOF
# Phaser Query Configuration
rocksdb_path: $PHASER_DIR/data/rocksdb
data_root: $PHASER_DIR/data

bridges:
  - chain_id: 1
    endpoint: http://127.0.0.1:8090
    name: erigon

segment_size: 500000
max_file_size_mb: 1024
buffer_timeout_secs: 60

rpc_port: 8545
sql_port: 0
sync_admin_port: 9093

sync_parallelism: 4
EOF"

    log "Config deployed"
}

# Main
main() {
    log "Deploying to $REMOTE..."
    log "  Phaser: $PHASER_DIR"
    log "  Erigon: $ERIGON_DIR"
    echo ""

    create_dirs

    if [[ "$DEPLOY_ERIGON" == true ]]; then
        deploy_erigon
        echo ""
    fi

    if [[ "$DEPLOY_PHASER" == true ]]; then
        deploy_phaser
        echo ""
    fi

    if [[ "$DEPLOY_SCRIPTS" == true ]]; then
        deploy_scripts
        echo ""
    fi

    if [[ "$DEPLOY_CONFIG" == true ]]; then
        deploy_config
        echo ""
    fi

    log "Deployment complete!"
    echo ""
    echo "Remote layout:"
    ssh "$REMOTE" "ls -la $ERIGON_DIR/bin/ $PHASER_DIR/bin/ 2>/dev/null | head -20"
}

main "$@"
