# Resumability Edge Cases Discovered

Testing Date: 2025-10-01
Test: Multiple crash/resume cycles with historical sync job (blocks 0-1000000)

## Critical Issues Found

### 1. **Sync Jobs Don't Persist Across Restarts**
**Severity:** CRITICAL
**Impact:** Historical sync jobs are completely lost on crash

**Evidence:**
- Created job: `220ee211-67db-43ba-8a8e-7a26c22f961e`
- After crash: "No sync jobs found"
- Job made 0 progress before crash (0/1000001 blocks, 0.0%)

**Root Cause:** Sync jobs only stored in-memory, no RocksDB persistence

**Fix Needed:**
- Persist sync job metadata to RocksDB on creation
- Restore and auto-resume jobs on startup
- Track job state: created → running → paused (on crash) → resumed → completed

---

### 2. **Live Streaming Auto-Starts Instead of Historical Sync**
**Severity:** HIGH
**Impact:** Historical sync requested but live sync runs instead

**Evidence:**
```
[INFO] phaser_query: Starting streaming service for chain 1...
[INFO] Subscribing to blocks from bridge
[INFO] Received blocks batch with 1 rows, block #23487129
```
- Historical job requested blocks 0-1000000
- Actual data received: blocks 23487116+ (current tip)
- 18 live sync temp files created, 0 historical sync files

**Root Cause:** `streaming_with_writer` service auto-starts on phaser-query startup, independent of sync jobs

**Fix Needed:**
- Separate live streaming (real-time data) from historical sync jobs
- Only start live streaming if explicitly requested or no historical jobs pending
- Add config option: `auto_start_live_sync: false`

---

### 3. **Stale Temp Files Accumulate**
**Severity:** MEDIUM
**Impact:** Disk space waste, potential data corruption on resume

**Evidence:**
18 temp files left behind, all 0 bytes:
```
blocks_from_23487117_to_23499999.parquet.tmp (0 bytes, Oct 1 19:27)
transactions_from_23487117_to_23499999.parquet.tmp (0 bytes, Oct 1 19:27)
logs_from_23487116_to_23499999.parquet.tmp (0 bytes, Oct 1 19:27)
... (15 more)
```

**Pattern:** Each crash leaves 3 temp files (blocks + txs + logs), multiple crashes = accumulation

**Root Cause:**
1. Files created immediately on first batch received
2. Writers receive batches but never flush before crash
3. DataScanner only cleans historical temp files (segment boundary), not live ones (mid-segment)

**Fix Needed:**
- Clean ALL temp files on startup that are >5 seconds old
- Or: Track active temp files in memory, clean untracked ones
- Or: Don't create file until first actual data write

---

### 4. **Indexer Ignores Temp Files**
**Severity:** MEDIUM
**Impact:** Resume logic doesn't account for in-progress work

**Evidence:**
```
[INFO] Found 0 block files in ".../resumability/1/erigon"
```
But 18 `.parquet.tmp` files exist in that directory!

**Root Cause:** Indexer only globs for `*.parquet`, not `*.parquet.tmp`

**Fix Needed:**
- DataScanner already handles temp files - use it for resume logic
- Don't rely on indexer for determining what needs syncing
- Sync service should scan for existing files using DataScanner

---

### 5. **No Historical Sync Progress Despite "Running" Status**
**Severity:** HIGH
**Impact:** Jobs appear to be working but actually aren't

**Evidence:**
- Job status: "RUNNING"
- Progress: "0/1000001 blocks (0.0%)"
- Active workers: 2
- But after 20+ seconds: still 0 progress

**Root Cause:** Workers weren't actually started for the historical job

**Logs Show:** No worker logs, no segment processing, only live streaming activity

**Fix Needed:**
- Investigate why workers didn't start
- Add worker startup logs
- Add watchdog to detect stuck jobs (no progress after N seconds)

---

### 6. **Data Directory Structure Unexpected**
**Severity:** LOW
**Impact:** Test script couldn't find files

**Config:** `data_root: /media/raid/erigon-copy/phaser/test-data/resumability`
**Actual files:** `/media/raid/erigon-copy/phaser/test-data/resumability/1/erigon/*.parquet.tmp`

**Structure:** `{data_root}/{chain_id}/{bridge_name}/`

**Fix:** Not a bug - test script needs to be aware of directory structure

---

## Summary of Fixes Needed

### Priority 1 (Blocking Resumability)
1. ✓ Persist sync jobs to RocksDB
2. ✓ Auto-resume jobs on startup
3. ✓ Investigate why workers don't start for historical jobs
4. ✓ Separate live streaming from historical sync

### Priority 2 (Data Integrity)
5. ✓ Clean ALL stale temp files on startup (not just historical)
6. ✓ Use DataScanner in sync service for resume logic
7. ✓ Add job state transitions (created/running/paused/completed)

### Priority 3 (Observability)
8. ✓ Add worker startup logs
9. ✓ Add job watchdog (detect stuck jobs)
10. ✓ Better progress tracking (show per-segment status)

## Test Improvements Needed

1. Update test script to handle `{chain_id}/{bridge_name}/` directory structure
2. Add test for job persistence across multiple restarts
3. Add test for temp file cleanup on resume
4. Add test for mid-segment crash and resume
5. Add test for resume when some segments complete, some partial
