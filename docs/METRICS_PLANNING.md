# Phaser Metrics Planning

## Overview

This document outlines the metrics architecture for phaser bridges and query services, with focus on multi-chain support and protocol-agnostic design.

## Problem Statement

Bridges (jsonrpc-bridge, erigon-bridge) can connect to different chains:
- Ethereum mainnet (chain_id: 1)
- Arbitrum One (chain_id: 42161)
- Base (chain_id: 8453)
- Gnosis (chain_id: 100)
- Polygon (chain_id: 137)
- etc.

Erigon is adding Arbitrum support. A single jsonrpc-bridge binary can point at any EVM-compatible JSON-RPC endpoint. Metrics need to cleanly disambiguate which chain is being synced.

## Current State

### Labels in Use
```
chain_id="1"           # Chain identifier
bridge_name="jsonrpc"  # Bridge instance name
service_name="jsonrpc_bridge"  # Metric prefix
```

### Issues
1. **Service name collisions**: Running `jsonrpc-bridge` against mainnet and arbitrum creates duplicate metric registrations
2. **Metric prefix baked in**: `jsonrpc_bridge_*` doesn't indicate which chain
3. **No standard chain labeling**: Each bridge handles chain_id differently

## Proposed Design

### 1. Chain-Aware Metric Naming

**Option A: Chain in service name**
```
jsonrpc_bridge_1_segment_duration_seconds{...}      # Mainnet
jsonrpc_bridge_42161_segment_duration_seconds{...}  # Arbitrum
```
- Pro: Clear separation, no collision
- Con: Many metrics per chain, harder to aggregate

**Option B: Chain as primary label (recommended)**
```
bridge_segment_duration_seconds{bridge_type="jsonrpc", chain_id="1", ...}
bridge_segment_duration_seconds{bridge_type="jsonrpc", chain_id="42161", ...}
```
- Pro: Easy to aggregate across chains, filter by chain
- Con: Requires changing existing metric names

**Option C: Instance label**
```
jsonrpc_bridge_segment_duration_seconds{instance="mainnet", chain_id="1", ...}
jsonrpc_bridge_segment_duration_seconds{instance="arbitrum", chain_id="42161", ...}
```
- Pro: Backward compatible
- Con: Instance name is arbitrary, chain_id is authoritative

### 2. Standard Labels

All bridge metrics should include:
```
chain_id      # Numeric chain ID (1, 42161, etc.)
chain_name    # Human-readable (mainnet, arbitrum, base) - optional
bridge_type   # jsonrpc, erigon, canton
bridge_name   # Instance name for disambiguation
```

### 3. Generic Segment Metrics

Protocol-agnostic metrics that work for any data source:

```rust
pub struct SegmentMetricsConfig {
    /// Metric name prefix
    pub prefix: String,

    /// Chain identifier
    pub chain_id: u64,

    /// Optional chain name for labels
    pub chain_name: Option<String>,

    /// Bridge type (jsonrpc, erigon, etc.)
    pub bridge_type: String,

    /// Bridge instance name
    pub bridge_name: String,

    /// Data types/phases to track (e.g., ["blocks", "transactions", "logs"])
    pub data_types: Vec<String>,
}
```

### 4. Metrics to Implement

#### Segment Lifecycle
```
segment_start_timestamp{segment_num, chain_id}
segment_complete_timestamp{segment_num, chain_id}
segment_duration_total_seconds{segment_num, chain_id}  # Wall-clock time
segment_attempts_total{chain_id, result}               # success/failure
segment_retry_count{segment_num, chain_id}
```

#### Data Progress
```
items_processed_total{chain_id, data_type, segment_num}
items_per_second{chain_id, data_type}  # Gauge, current rate
bytes_processed_total{chain_id, data_type}
```

#### RPC/Transport
```
rpc_requests_total{chain_id, method, status}
rpc_request_duration_seconds{chain_id, method}
rpc_errors_total{chain_id, error_type}
rate_limit_events_total{chain_id}
```

#### Consumer Lag
```
consumer_lag_blocks{chain_id, consumer}
consumer_lag_seconds{chain_id, consumer}
pending_batches{chain_id}
```

## Migration Path

1. **Phase 1**: Add `chain_name` label to existing metrics (non-breaking)
2. **Phase 2**: Create new unified `bridge_*` metrics alongside existing
3. **Phase 3**: Deprecate old metrics, update dashboards
4. **Phase 4**: Remove deprecated metrics

## Dashboard Considerations

With chain-aware metrics, dashboards can:
- Show all chains on one view with chain selector
- Compare sync progress across chains
- Aggregate error rates across deployments
- Alert on chain-specific issues

Example Grafana variables:
```
$chain_id = label_values(bridge_segment_duration_seconds, chain_id)
$chain_name = label_values(bridge_segment_duration_seconds, chain_name)
```

## Implementation Notes

### BridgeMetrics Constructor
```rust
impl BridgeMetrics {
    pub fn new(config: SegmentMetricsConfig) -> Self {
        // Use config.prefix for metric names
        // Include chain_id, chain_name, bridge_type in all labels
    }
}
```

### CLI Configuration
```bash
jsonrpc-bridge \
  --jsonrpc-url http://127.0.0.1:8545 \
  --chain-name mainnet \        # Optional, derived from chain_id if not set
  --metrics-prefix jsonrpc \    # Or use unified "bridge" prefix
```

### Chain Name Resolution
```rust
fn chain_name(chain_id: u64) -> &'static str {
    match chain_id {
        1 => "mainnet",
        42161 => "arbitrum",
        8453 => "base",
        100 => "gnosis",
        137 => "polygon",
        10 => "optimism",
        _ => "unknown",
    }
}
```

## Open Questions

1. Should we use a unified `bridge_*` prefix or keep `jsonrpc_bridge_*` / `erigon_bridge_*`?
2. How to handle metrics for bridges serving multiple chains (if ever)?
3. Should chain_name be required or auto-derived from chain_id?
4. Cardinality concerns with segment_num labels for long-running syncs?

## Related Files

- `crates/phaser-metrics/src/segment_metrics.rs` - Current implementation
- `crates/phaser-metrics/src/lib.rs` - QueryMetrics, BridgeMetrics
- `test-data/jsonrpc-bridge-tests/IMPROVEMENTS.md` - Testing observations
