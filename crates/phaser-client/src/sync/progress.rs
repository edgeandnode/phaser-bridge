//! Progress tracking for sync operations
//!
//! This module provides position-agnostic progress tracking. All types use
//! generic "position" terminology rather than blockchain-specific terms like
//! "block" to support any ordered data source.

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

/// A contiguous range of positions (e.g., blocks, slots, offsets, sequence numbers)
///
/// Generic range type for sync operations. The unit depends on context -
/// could be block numbers, slot numbers, ledger offsets, or any ordered position.
///
/// TODO: Move to phaser-types for reuse across crates
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}

impl Range {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Number of positions in this range (inclusive)
    pub fn len(&self) -> u64 {
        if self.start > self.end {
            0
        } else {
            self.end - self.start + 1
        }
    }

    /// Check if range is empty
    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    /// Check if this range contains a position
    pub fn contains(&self, pos: u64) -> bool {
        pos >= self.start && pos <= self.end
    }

    /// Check if this range overlaps with another
    pub fn overlaps(&self, other: &Range) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Convert to std::ops::Range (exclusive end)
    pub fn to_std_range(&self) -> std::ops::Range<u64> {
        self.start..self.end.saturating_add(1)
    }
}

impl From<std::ops::Range<u64>> for Range {
    fn from(r: std::ops::Range<u64>) -> Self {
        Self {
            start: r.start,
            end: r.end.saturating_sub(1),
        }
    }
}

/// Progress information for a single worker
#[derive(Debug, Clone)]
pub struct WorkerProgress {
    /// Worker identifier
    pub worker_id: u32,
    /// Start of the position range being synced
    pub from_position: u64,
    /// End of the position range being synced
    pub to_position: u64,
    /// Current phase of work (e.g., "syncing transactions", "syncing events")
    pub current_phase: String,
    /// Data types that have been completed (string names for flexibility)
    pub completed_types: HashSet<String>,
    /// Data types expected to complete (for is_complete check)
    pub expected_types: HashSet<String>,
    /// When the worker started
    pub started_at: SystemTime,
    /// Current position being processed
    pub current_position: u64,
    /// Total positions processed so far
    pub positions_processed: u64,
    /// Total bytes written so far
    pub bytes_written: u64,
    /// Number of files created
    pub files_created: u32,
}

impl WorkerProgress {
    /// Create a new WorkerProgress for a worker starting a segment
    pub fn new(worker_id: u32, from_position: u64, to_position: u64) -> Self {
        Self {
            worker_id,
            from_position,
            to_position,
            current_phase: "initializing".to_string(),
            completed_types: HashSet::new(),
            expected_types: HashSet::new(),
            started_at: SystemTime::now(),
            current_position: from_position,
            positions_processed: 0,
            bytes_written: 0,
            files_created: 0,
        }
    }

    /// Create with expected data types to track
    pub fn with_expected_types(
        mut self,
        types: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.expected_types = types.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Update the current phase
    pub fn set_phase(&mut self, phase: impl Into<String>) {
        self.current_phase = phase.into();
    }

    /// Mark a data type as completed (by string name)
    pub fn mark_completed(&mut self, data_type: impl AsRef<str>) {
        self.completed_types.insert(data_type.as_ref().to_string());
    }

    /// Check if all expected data types are completed
    pub fn is_complete(&self) -> bool {
        if self.expected_types.is_empty() {
            // If no expected types specified, we can't determine completion
            false
        } else {
            self.expected_types
                .iter()
                .all(|t| self.completed_types.contains(t))
        }
    }

    /// Get elapsed time since worker started
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at
            .elapsed()
            .unwrap_or(std::time::Duration::ZERO)
    }

    /// Calculate positions per second
    pub fn positions_per_second(&self) -> f64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.positions_processed as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// Overall sync progress across all workers
#[derive(Debug, Clone, Default)]
pub struct SyncProgress {
    /// Total segments to sync
    pub total_segments: u64,
    /// Segments completed
    pub completed_segments: u64,
    /// Segments currently being processed
    pub in_progress_segments: u64,
    /// Total positions to sync
    pub total_positions: u64,
    /// Positions synced so far
    pub positions_synced: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of retries that have occurred
    pub total_retries: u64,
    /// Number of permanent errors encountered
    pub permanent_errors: u64,
}

impl SyncProgress {
    /// Create a new SyncProgress for a sync range
    pub fn new(total_segments: u64, total_positions: u64) -> Self {
        Self {
            total_segments,
            total_positions,
            ..Default::default()
        }
    }

    /// Calculate completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_segments == 0 {
            return 100.0;
        }
        (self.completed_segments as f64 / self.total_segments as f64) * 100.0
    }

    /// Check if sync is complete
    pub fn is_complete(&self) -> bool {
        self.completed_segments >= self.total_segments
    }

    /// Mark a segment as completed
    pub fn mark_segment_completed(&mut self, positions: u64, bytes: u64) {
        self.completed_segments += 1;
        self.positions_synced += positions;
        self.bytes_written += bytes;
        if self.in_progress_segments > 0 {
            self.in_progress_segments -= 1;
        }
    }

    /// Mark a segment as started
    pub fn mark_segment_started(&mut self) {
        self.in_progress_segments += 1;
    }

    /// Record a retry
    pub fn record_retry(&mut self) {
        self.total_retries += 1;
    }

    /// Record a permanent error
    pub fn record_permanent_error(&mut self) {
        self.permanent_errors += 1;
    }
}

/// Work item for a segment that needs syncing
///
/// Uses a map of data type name -> missing ranges for flexibility.
/// Callers define their own data type names (e.g., "blocks", "events", "account_updates").
#[derive(Debug, Clone)]
pub struct SegmentWork {
    /// Segment number
    pub segment_num: u64,
    /// Start of the segment (position)
    pub segment_start: u64,
    /// End of the segment (position)
    pub segment_end: u64,
    /// Missing ranges by data type name
    pub missing_ranges: HashMap<String, Vec<Range>>,
    /// Number of times this has been retried
    pub retry_count: Option<u32>,
    /// Last attempt time (for backoff)
    pub last_attempt: std::time::Instant,
}

impl SegmentWork {
    /// Create an empty segment work item
    pub fn new(segment_num: u64, segment_start: u64, segment_end: u64) -> Self {
        Self {
            segment_num,
            segment_start,
            segment_end,
            missing_ranges: HashMap::new(),
            retry_count: None,
            last_attempt: std::time::Instant::now(),
        }
    }

    /// Create work for a completely missing segment with specified data types
    pub fn new_full_segment(
        segment_num: u64,
        segment_start: u64,
        segment_end: u64,
        data_types: &[&str],
    ) -> Self {
        let full_range = Range::new(segment_start, segment_end);
        let mut missing_ranges = HashMap::new();
        for dt in data_types {
            missing_ranges.insert(dt.to_string(), vec![full_range.clone()]);
        }
        Self {
            segment_num,
            segment_start,
            segment_end,
            missing_ranges,
            retry_count: None,
            last_attempt: std::time::Instant::now(),
        }
    }

    /// Add missing ranges for a data type
    pub fn add_missing(&mut self, data_type: impl Into<String>, ranges: Vec<Range>) {
        self.missing_ranges.insert(data_type.into(), ranges);
    }

    /// Check if this segment is complete (no missing data for any type)
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.values().all(|ranges| ranges.is_empty())
    }

    /// Get list of data types that have missing ranges
    pub fn missing_types(&self) -> Vec<String> {
        self.missing_ranges
            .iter()
            .filter(|(_, ranges)| !ranges.is_empty())
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Check if a specific data type needs syncing
    pub fn needs_sync(&self, data_type: &str) -> bool {
        self.missing_ranges
            .get(data_type)
            .map(|ranges| !ranges.is_empty())
            .unwrap_or(false)
    }

    /// Get the missing ranges for a data type
    pub fn get_missing_ranges(&self, data_type: &str) -> &[Range] {
        self.missing_ranges
            .get(data_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_basics() {
        let range = Range::new(0, 999);
        assert_eq!(range.len(), 1000);
        assert!(range.contains(0));
        assert!(range.contains(999));
        assert!(!range.contains(1000));
    }

    #[test]
    fn test_range_empty() {
        let range = Range::new(100, 50); // start > end
        assert!(range.is_empty());
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_range_overlap() {
        let r1 = Range::new(0, 100);
        let r2 = Range::new(50, 150);
        let r3 = Range::new(200, 300);

        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn test_range_conversion() {
        // std::ops::Range to our Range
        let std_range = 0u64..1000u64;
        let our_range: Range = std_range.into();
        assert_eq!(our_range.start, 0);
        assert_eq!(our_range.end, 999); // exclusive -> inclusive

        // Our Range to std::ops::Range
        let back = our_range.to_std_range();
        assert_eq!(back, 0..1000);
    }

    #[test]
    fn test_segment_work_with_custom_types() {
        // Test with Ethereum-style types
        let work =
            SegmentWork::new_full_segment(0, 0, 499_999, &["blocks", "transactions", "logs"]);
        assert!(!work.is_complete());
        assert!(work.needs_sync("blocks"));
        assert!(work.needs_sync("transactions"));
        assert!(work.needs_sync("logs"));
        assert!(!work.needs_sync("proofs")); // Not configured

        // Test with Canton-style types
        let canton_work = SegmentWork::new_full_segment(0, 0, 999, &["events", "domain_updates"]);
        assert!(canton_work.needs_sync("events"));
        assert!(canton_work.needs_sync("domain_updates"));
        assert!(!canton_work.needs_sync("blocks")); // Different chain, different types
    }

    #[test]
    fn test_sync_progress() {
        let mut progress = SyncProgress::new(10, 5_000_000);
        assert_eq!(progress.completion_percentage(), 0.0);

        progress.mark_segment_started();
        progress.mark_segment_completed(500_000, 1_000_000);
        assert_eq!(progress.completion_percentage(), 10.0);
        assert_eq!(progress.completed_segments, 1);
    }

    #[test]
    fn test_worker_progress_with_string_types() {
        let mut wp = WorkerProgress::new(0, 0, 499_999).with_expected_types([
            "blocks",
            "transactions",
            "logs",
        ]);
        assert!(!wp.is_complete());

        wp.mark_completed("blocks");
        assert!(!wp.is_complete());

        wp.mark_completed("transactions");
        assert!(!wp.is_complete());

        wp.mark_completed("logs");
        assert!(wp.is_complete());
    }

    #[test]
    fn test_worker_progress_custom_chain() {
        // Solana-style with account_updates
        let mut wp =
            WorkerProgress::new(0, 0, 999).with_expected_types(["slot_data", "account_updates"]);

        wp.mark_completed("slot_data");
        assert!(!wp.is_complete());

        wp.mark_completed("account_updates");
        assert!(wp.is_complete());
    }
}
