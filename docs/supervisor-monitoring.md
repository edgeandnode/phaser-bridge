# Using Balloons Supervisor for Phaser Monitoring

The Balloons supervisor provides background process management with log capture,
ideal for long-running sync jobs.

## Benefits Over Direct Execution

| Feature | Direct (bash &) | Supervisor |
|---------|----------------|------------|
| Log capture | Manual redirection | Automatic ring buffer |
| Process tracking | By PID | By name/ID |
| Session awareness | None | Tied to session |
| Remote execution | Manual SSH | Built-in SSH support |
| Output querying | `tail -f` | Structured queries |

## Starting Services via Supervisor

### Local Execution

Start erigon-bridge:
```
<balloons-tool>
{"name": "supervisor_start", "args": {
  "command": "./target/release/erigon-bridge --erigon-grpc 192.168.0.174:9091 --flight-addr 0.0.0.0:8090",
  "name": "erigon-bridge",
  "working_dir": "/home/dan/Development/en/phaser",
  "env": {"RUST_BACKTRACE": "1"}
}}
</balloons-tool>
```

Start phaser-query:
```
<balloons-tool>
{"name": "supervisor_start", "args": {
  "command": "./target/release/phaser-query -c ./test-data/test-config.yaml --disable-streaming --metrics-port 9092",
  "name": "phaser-query",
  "working_dir": "/home/dan/Development/en/phaser"
}}
</balloons-tool>
```

### Remote Execution (via SSH)

If you have hosts configured in `~/.balloons/supervisor.yaml`:

```yaml
# ~/.balloons/supervisor.yaml
hosts:
  superserver:
    type: ssh
    host: 192.168.0.174
    user: dan
    tags: [erigon, ethereum]
```

Then execute remotely:
```
<balloons-tool>
{"name": "supervisor_start", "args": {
  "command": "/home/dan/bin/erigon --datadir=/mnt/nvme-raid/erigon-archive --prune.mode=archive ...",
  "name": "erigon-archive",
  "host": "superserver"
}}
</balloons-tool>
```

## Monitoring Processes

### List All Processes
```
<balloons-tool>
{"name": "supervisor_list", "args": {}}
</balloons-tool>
```

Returns:
```json
[
  {
    "process_id": "abc123",
    "name": "erigon-bridge",
    "command": "./target/release/erigon-bridge ...",
    "status": {"state": "running", "pid": 12345},
    "session_id": "current-session"
  },
  ...
]
```

### Get Process Output
```
<balloons-tool>
{"name": "supervisor_output", "args": {
  "process_id": "abc123",
  "limit": 50
}}
</balloons-tool>
```

Returns last 50 log entries with timestamps and source (stdout/stderr).

### Filter by Session
```
<balloons-tool>
{"name": "supervisor_list", "args": {"all_sessions": false}}
</balloons-tool>
```

Only shows processes from current session.

### Check Specific Host
```
<balloons-tool>
{"name": "supervisor_host_status", "args": {"host": "superserver"}}
</balloons-tool>
```

## Stopping Processes

```
<balloons-tool>
{"name": "supervisor_stop", "args": {"process_id": "abc123"}}
</balloons-tool>
```

## Example Workflow: Full Sync Monitoring

### 1. Start Services

```
# Start bridge
<balloons-tool>
{"name": "supervisor_start", "args": {
  "command": "./target/release/erigon-bridge --erigon-grpc 192.168.0.174:9091 --flight-addr 0.0.0.0:8090",
  "name": "bridge",
  "working_dir": "/home/dan/Development/en/phaser"
}}
</balloons-tool>

# Start query service
<balloons-tool>
{"name": "supervisor_start", "args": {
  "command": "./target/release/phaser-query -c ./test-data/test-config.yaml --disable-streaming",
  "name": "query",
  "working_dir": "/home/dan/Development/en/phaser"
}}
</balloons-tool>
```

### 2. Start Sync Job (via bash, not supervisor)

```bash
./target/debug/phaser-cli -e http://127.0.0.1:9093 sync \
  --chain-id 1 --bridge erigon --from 0 --to 24600000
```

### 3. Periodic Status Checks

Ask Claude to check status:
```
Check on the sync - list supervised processes and show recent output from phaser-query
```

Claude will:
```
<balloons-tool>
{"name": "supervisor_list", "args": {}}
</balloons-tool>

<balloons-tool>
{"name": "supervisor_output", "args": {"process_id": "query-id", "limit": 30}}
</balloons-tool>
```

### 4. Error Investigation

If errors occur:
```
<balloons-tool>
{"name": "supervisor_output", "args": {
  "process_id": "bridge-id",
  "limit": 100
}}
</balloons-tool>
```

Look for ERROR/WARN entries in the output.

## Log Entry Format

Each log entry contains:
- `timestamp`: When the log was captured
- `source`: "stdout", "stderr", or "system"
- `content`: The log line content

Example:
```json
{
  "timestamp": "2026-03-11T06:30:00Z",
  "source": "stdout",
  "content": "2026-03-11T06:30:00.123Z INFO phaser_query::sync: Segment 5 complete"
}
```

## Integration with Metrics

While supervisor handles logs, combine with Prometheus metrics for full observability:

```bash
# Metrics scraping alongside supervisor monitoring
curl -s http://localhost:9092/metrics | grep -E "segment|error|worker"
```

## Best Practices

1. **Name processes clearly** - Use descriptive names like "bridge-archive" not "proc1"
2. **Check output periodically** - Don't wait for failures; proactively monitor
3. **Use working_dir** - Always specify to ensure correct relative paths
4. **Set env vars** - Include RUST_BACKTRACE=1 for debugging
5. **Monitor both services** - Bridge and query are interdependent
