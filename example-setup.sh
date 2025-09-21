#!/bin/bash

# Example of how to run the complete system

echo "Starting Erigon Bridge..."
# The bridge connects to Erigon and serves Arrow Flight
./target/debug/erigon-bridge \
  --erigon-grpc 192.168.0.174:9090 \
  --flight-addr 0.0.0.0:8090 \
  --chain-id 1 &

BRIDGE_PID=$!
echo "Bridge started with PID $BRIDGE_PID"

sleep 2

echo "Starting Phaser Query Service..."
# Phaser connects to the bridge (not directly to Erigon)
./target/debug/phaser-query \
  --data-dir ./data \
  --rocksdb-path ./rocksdb \
  --rpc-port 8545 \
  --sql-port 8080

# The phaser service would be modified to connect to bridges like:
# --bridge-endpoints localhost:8090
# And it would use FlightBridgeClient to pull data