#!/bin/bash
set -e

# Add all changes (will detect moves properly)
git add -A

# Create commit
git commit -m "$(cat <<'EOF'
Reorganize EVM bridges under crates/bridges/evm/

Moved bridges to better namespace structure:
- crates/bridges/erigon-bridge → crates/bridges/evm/erigon-bridge
- crates/bridges/jsonrpc-bridge → crates/bridges/evm/jsonrpc-bridge

Benefits:
- Clear EVM-specific namespace for future multi-chain support
- Consistent location for all EVM-related bridges
- Better organization as we add more bridge types

Also updated to use workspace dependencies:
- Added erigon-bridge, jsonrpc-bridge, phaser-bridge, evm-common to
  workspace.dependencies in root Cargo.toml
- Updated all crates to use `{ workspace = true }` instead of relative paths
- Cleaner dependency management across the workspace

No functional changes - pure reorganization.
EOF
)"

echo ""
echo "✓ Reorganization committed successfully!"
echo ""
echo "New structure:"
echo "  crates/bridges/evm/"
echo "    ├── erigon-bridge/"
echo "    └── jsonrpc-bridge/"
