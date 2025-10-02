use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Represents a block range that has been synced
#[derive(Debug, Clone)]
pub struct BlockRange {
    pub start: u64,
    pub end: u64,
}

/// Analysis of what segments need syncing
#[derive(Debug, Clone)]
pub struct GapAnalysis {
    pub total_segments: u64,
    pub complete_segments: Vec<u64>,
    pub missing_segments: Vec<u64>,
    pub incomplete_segments: Vec<(u64, Vec<String>)>, // (segment_num, missing data types)
    pub cleaned_temp_files: usize,
}

impl GapAnalysis {
    pub fn complete_count(&self) -> usize {
        self.complete_segments.len()
    }

    pub fn missing_count(&self) -> usize {
        self.missing_segments.len()
    }

    pub fn completion_percentage(&self) -> f64 {
        if self.total_segments == 0 {
            return 100.0;
        }
        (self.complete_count() as f64 / self.total_segments as f64) * 100.0
    }

    pub fn needs_sync(&self) -> bool {
        !self.missing_segments.is_empty()
    }
}

/// Scanner for detecting existing blockchain data
pub struct DataScanner {
    data_dir: PathBuf,
}

impl DataScanner {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Scans parquet files to find existing block ranges
    pub fn scan_existing_ranges(&self) -> Result<Vec<BlockRange>> {
        let mut ranges = Vec::new();

        if !self.data_dir.exists() {
            return Ok(ranges);
        }

        for entry in fs::read_dir(&self.data_dir).context("Failed to read data directory")? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            // Parse both .parquet and .parquet.tmp files
            if let Some(range) = self.parse_filename(&path)? {
                ranges.push(range);
            }
        }

        // Sort ranges by start block
        ranges.sort_by_key(|r| r.start);

        debug!(
            "Found {} existing block ranges in {:?}",
            ranges.len(),
            self.data_dir
        );
        Ok(ranges)
    }

    /// Parse filename to extract block range
    fn parse_filename(&self, path: &Path) -> Result<Option<BlockRange>> {
        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => return Ok(None),
        };

        // Skip non-parquet files
        if !filename.contains(".parquet") {
            return Ok(None);
        }

        // Parse filenames like:
        // Finalized: blocks_0-499999_from_0_to_499999.parquet
        // Temp: blocks_from_0_to_499999.parquet.tmp
        // Temp (live): blocks_from_485689_to_499999.parquet.tmp

        // Check if this contains the actual block range (both .tmp and finalized files)
        if filename.contains("_from_") && filename.contains("_to_") {
            if let Some(from_idx) = filename.find("_from_") {
                if let Some(to_idx) = filename.find("_to_") {
                    let start_str = &filename[from_idx + 6..to_idx];
                    let end_part = &filename[to_idx + 4..];
                    // Remove .parquet or .parquet.tmp extension
                    let end_str = end_part
                        .trim_end_matches(".parquet.tmp")
                        .trim_end_matches(".parquet");

                    if let (Ok(start), Ok(end)) = (start_str.parse::<u64>(), end_str.parse::<u64>())
                    {
                        // Validate range is sensible
                        if start <= end {
                            return Ok(Some(BlockRange { start, end }));
                        } else {
                            warn!(
                                "Invalid block range in filename {}: start {} > end {}",
                                filename, start, end
                            );
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Find the first gap in block coverage or the start of live sync data
    /// Returns the block number where historical sync can safely backfill up to
    pub fn find_historical_boundary(&self, segment_size: u64) -> Result<Option<u64>> {
        let ranges = self.scan_existing_ranges()?;

        if ranges.is_empty() {
            info!("No existing data found, historical sync can start from genesis");
            return Ok(None);
        }

        // Check for the first gap or find where continuous data starts
        let mut expected_start = 0u64;

        for range in &ranges {
            if range.start > expected_start {
                // Found a gap! Historical sync should backfill up to range.start - 1
                info!(
                    "Found gap in data: blocks {} to {} are missing. Historical sync can backfill up to {}",
                    expected_start,
                    range.start - 1,
                    range.start - 1
                );
                return Ok(Some(range.start - 1));
            }

            // Update expected start to after this range
            expected_start = range.end + 1;
        }

        // No gaps found, but we should find the last contiguous segment boundary
        // The last range.end might be in the middle of a segment
        // Round down to the previous segment boundary
        if let Some(last_range) = ranges.last() {
            let last_segment_boundary = (last_range.end / segment_size) * segment_size;
            if last_segment_boundary > 0 {
                info!(
                    "Found continuous data up to block {}. Historical sync can backfill up to segment boundary {}",
                    last_range.end,
                    last_segment_boundary - 1
                );
                return Ok(Some(last_segment_boundary - 1));
            }
        }

        info!("No clear boundary found for historical sync");
        Ok(None)
    }

    /// Get a summary of data coverage
    pub fn get_coverage_summary(&self) -> Result<String> {
        let ranges = self.scan_existing_ranges()?;

        if ranges.is_empty() {
            return Ok("No data found".to_string());
        }

        let mut summary = String::new();
        summary.push_str(&format!("Found {} block ranges:\n", ranges.len()));

        for (i, range) in ranges.iter().enumerate() {
            let blocks = range.end - range.start + 1;
            summary.push_str(&format!(
                "  {}. Blocks {} to {} ({} blocks)\n",
                i + 1,
                range.start,
                range.end,
                blocks
            ));
        }

        Ok(summary)
    }

    /// Clean temp files that conflict with segments we're about to sync
    /// Only removes .parquet.tmp files for segments in the provided list
    /// This prevents deleting active live streaming temp files
    pub fn clean_conflicting_temp_files(&self, segments: &[u64], segment_size: u64) -> Result<usize> {
        if !self.data_dir.exists() {
            return Ok(0);
        }

        let mut cleaned_count = 0;

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Only process temp files
            if !filename_str.ends_with(".parquet.tmp") {
                continue;
            }

            // Parse the temp file to see what segment it covers
            if let Some(range) = self.parse_filename(&path)? {
                let file_segment = range.start / segment_size;

                // Only delete if this temp file is for a segment we're about to sync
                if segments.contains(&file_segment) {
                    info!(
                        "Cleaning conflicting temp file for segment {} (blocks {}-{}): {}",
                        file_segment,
                        range.start,
                        range.end,
                        path.display()
                    );

                    if let Err(e) = fs::remove_file(&path) {
                        warn!("Failed to remove temp file {}: {}", path.display(), e);
                    } else {
                        cleaned_count += 1;
                    }
                } else {
                    debug!(
                        "Preserving non-conflicting temp file for segment {}: {}",
                        file_segment,
                        path.display()
                    );
                }
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned {} conflicting temp files", cleaned_count);
        }

        Ok(cleaned_count)
    }

    /// Analyze sync range and find gaps
    /// Returns detailed analysis of what needs syncing
    pub fn analyze_sync_range(
        &self,
        from_block: u64,
        to_block: u64,
        segment_size: u64,
    ) -> Result<GapAnalysis> {
        // Calculate total segments in the requested range
        let first_segment = from_block / segment_size;
        let last_segment = to_block / segment_size;
        let total_segments = last_segment - first_segment + 1;

        info!(
            "Analyzing sync range: blocks {}-{} ({} segments)",
            from_block, to_block, total_segments
        );

        let mut missing_segments = Vec::new();
        let mut complete_segments = Vec::new();
        let mut incomplete_segments = Vec::new(); // For detailed logging

        if !self.data_dir.exists() {
            info!("Data directory doesn't exist - all {} segments need syncing", total_segments);
            for segment_num in first_segment..=last_segment {
                missing_segments.push(segment_num);
            }
            return Ok(GapAnalysis {
                total_segments,
                complete_segments: Vec::new(),
                missing_segments,
                incomplete_segments: Vec::new(),
                cleaned_temp_files: 0,
            });
        }

        for segment_num in first_segment..=last_segment {
            let segment_start = segment_num * segment_size;
            let segment_end = segment_start + segment_size - 1;

            // Check for completed segment files (all three data types must exist)
            let blocks_complete =
                self.has_completed_segment("blocks", segment_start, segment_end)?;
            let txs_complete =
                self.has_completed_segment("transactions", segment_start, segment_end)?;
            let logs_complete = self.has_completed_segment("logs", segment_start, segment_end)?;

            if blocks_complete && txs_complete && logs_complete {
                complete_segments.push(segment_num);
                debug!(
                    "Segment {} (blocks {}-{}) is complete",
                    segment_num, segment_start, segment_end
                );
            } else {
                // Track what's missing for better logging
                let mut missing_parts = Vec::new();
                if !blocks_complete {
                    missing_parts.push("blocks");
                }
                if !txs_complete {
                    missing_parts.push("txs");
                }
                if !logs_complete {
                    missing_parts.push("logs");
                }

                info!(
                    "Segment {} (blocks {}-{}) incomplete - missing: {}",
                    segment_num,
                    segment_start,
                    segment_end,
                    missing_parts.join(", ")
                );

                // Clean any temp files for this segment
                self.clean_temp_files_for_segment(segment_start, segment_end)?;
                missing_segments.push(segment_num);
                incomplete_segments.push((
                    segment_num,
                    missing_parts.into_iter().map(|s| s.to_string()).collect(),
                ));
            }
        }

        // Summary of gap analysis
        if complete_segments.is_empty() {
            info!("No existing segments found - full sync required");
        } else {
            info!(
                "Found {} complete segments that overlap with requested range:",
                complete_segments.len()
            );
            // Log ranges of complete segments
            let mut ranges = Vec::new();
            let mut range_start = None;
            let mut range_end = None;

            for &seg in &complete_segments {
                if range_start.is_none() {
                    range_start = Some(seg);
                    range_end = Some(seg);
                } else if range_end == Some(seg - 1) {
                    // Consecutive
                    range_end = Some(seg);
                } else {
                    // Gap found, log previous range
                    if let (Some(start), Some(end)) = (range_start, range_end) {
                        let block_start = start * segment_size;
                        let block_end = (end + 1) * segment_size - 1;
                        ranges.push(format!("  segments {}-{} (blocks {}-{})", start, end, block_start, block_end));
                    }
                    range_start = Some(seg);
                    range_end = Some(seg);
                }
            }
            // Log final range
            if let (Some(start), Some(end)) = (range_start, range_end) {
                let block_start = start * segment_size;
                let block_end = (end + 1) * segment_size - 1;
                ranges.push(format!("  segments {}-{} (blocks {}-{})", start, end, block_start, block_end));
            }

            for range in ranges {
                info!("{}", range);
            }
        }

        if missing_segments.is_empty() {
            info!("All {} segments already synced - nothing to do", total_segments);
        } else {
            info!(
                "Need to sync {} missing segments ({}% of range)",
                missing_segments.len(),
                (missing_segments.len() as f64 / total_segments as f64 * 100.0) as u32
            );
        }

        Ok(GapAnalysis {
            total_segments,
            complete_segments,
            missing_segments,
            incomplete_segments,
            cleaned_temp_files: 0, // Will be filled in by caller
        })
    }

    /// Legacy method - kept for backward compatibility
    /// Use analyze_sync_range() for detailed analysis
    pub fn find_missing_segments(
        &self,
        from_block: u64,
        to_block: u64,
        segment_size: u64,
    ) -> Result<Vec<u64>> {
        let analysis = self.analyze_sync_range(from_block, to_block, segment_size)?;
        Ok(analysis.missing_segments)
    }

    /// Check if a completed parquet file exists for a specific segment
    fn has_completed_segment(
        &self,
        data_type: &str,
        segment_start: u64,
        segment_end: u64,
    ) -> Result<bool> {
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Look for completed files matching this segment
            // Format: {data_type}_{segment_start}-{segment_end}_from_*_to_*.parquet
            if filename_str.starts_with(&format!(
                "{}_{}-{}_from_",
                data_type, segment_start, segment_end
            )) && filename_str.ends_with(".parquet")
                && !filename_str.ends_with(".parquet.tmp")
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Clean temp files for a specific segment
    /// Only removes temp files from failed HISTORICAL syncs (starting at segment boundary)
    /// Preserves temp files from LIVE sync (starting mid-segment)
    fn clean_temp_files_for_segment(&self, segment_start: u64, segment_end: u64) -> Result<()> {
        use std::time::SystemTime;

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Look for temp files for this segment: {type}_from_{X}_to_{segment_end}.parquet.tmp
            if filename_str.ends_with(&format!("_to_{}.parquet.tmp", segment_end)) {
                // Parse to get the start block
                if let Some(range) = self.parse_filename(&path)? {
                    // Only clean if this starts at the segment boundary (failed historical sync)
                    // Skip if it starts mid-segment (active live sync)
                    if range.start == segment_start {
                        // Check if file is recently modified (within 5 seconds)
                        // to avoid race condition with active writes
                        if let Ok(metadata) = fs::metadata(&path) {
                            if let Ok(modified) = metadata.modified() {
                                if let Ok(elapsed) = SystemTime::now().duration_since(modified) {
                                    if elapsed.as_secs() < 5 {
                                        debug!(
                                            "Skipping recently modified temp file ({}s old): {}",
                                            elapsed.as_secs(),
                                            path.display()
                                        );
                                        continue;
                                    }
                                }
                            }
                        }

                        info!(
                            "Cleaning incomplete historical sync temp file: {}",
                            path.display()
                        );
                        fs::remove_file(&path)?;
                    } else {
                        debug!(
                            "Skipping live sync temp file (starts at {}): {}",
                            range.start,
                            path.display()
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    #[test]
    fn test_parse_finalized_filename() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Create a test file
        let test_path = temp_dir
            .path()
            .join("blocks_0-499999_from_0_to_46200.parquet");
        File::create(&test_path).unwrap();

        let range = scanner.parse_filename(&test_path).unwrap();
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 46200);
    }

    #[test]
    fn test_find_gap() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Create files with a gap
        File::create(
            temp_dir
                .path()
                .join("blocks_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();
        File::create(
            temp_dir
                .path()
                .join("blocks_500000-999999_from_500000_to_999999.parquet"),
        )
        .unwrap();
        // Gap here! Missing 1000000-1499999
        File::create(
            temp_dir
                .path()
                .join("blocks_1500000-1999999_from_1500000_to_1999999.parquet"),
        )
        .unwrap();

        let boundary = scanner.find_historical_boundary(500000).unwrap();
        assert_eq!(boundary, Some(1499999));
    }

    #[test]
    fn test_parse_live_sync_temp_file() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Live sync temp file starts mid-segment
        let test_path = temp_dir
            .path()
            .join("blocks_from_23485689_to_23499999.parquet.tmp");
        File::create(&test_path).unwrap();

        let range = scanner.parse_filename(&test_path).unwrap();
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start, 23485689); // Mid-segment
        assert_eq!(range.end, 23499999);
    }

    #[test]
    fn test_parse_historical_temp_file() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Historical sync temp file starts at segment boundary
        let test_path = temp_dir.path().join("blocks_from_0_to_499999.parquet.tmp");
        File::create(&test_path).unwrap();

        let range = scanner.parse_filename(&test_path).unwrap();
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start, 0); // Segment boundary
        assert_eq!(range.end, 499999);
    }

    #[test]
    fn test_invalid_range_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Invalid: start > end
        let test_path = temp_dir.path().join("blocks_from_1000_to_500.parquet.tmp");
        File::create(&test_path).unwrap();

        let range = scanner.parse_filename(&test_path).unwrap();
        // Should return None due to validation
        assert!(range.is_none());
    }

    #[test]
    fn test_clean_preserves_live_sync() {
        use std::io::Write;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Create historical temp file (at segment boundary)
        let hist_path = temp_dir.path().join("blocks_from_0_to_499999.parquet.tmp");
        File::create(&hist_path).unwrap();

        // Wait 6 seconds so historical file is considered "stale"
        // (modification time check requires >5 seconds)
        thread::sleep(std::time::Duration::from_secs(6));

        // Create live sync temp file (mid-segment) - this will be recent
        let live_path = temp_dir
            .path()
            .join("blocks_from_485689_to_499999.parquet.tmp");
        let mut live_file = File::create(&live_path).unwrap();
        live_file.write_all(b"live data").unwrap();

        // Clean should remove historical but preserve live
        let missing = scanner.find_missing_segments(0, 499999, 500000).unwrap();

        // Historical temp should be deleted (was stale)
        assert!(!hist_path.exists());

        // Live sync temp should still exist (mid-segment + recent)
        assert!(live_path.exists());

        // Segment 0 should be in missing (because historical temp was removed)
        assert_eq!(missing, vec![0]);
    }

    #[test]
    fn test_completed_segment_detection() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Create all three data types for segment 0
        File::create(
            temp_dir
                .path()
                .join("blocks_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();
        File::create(
            temp_dir
                .path()
                .join("transactions_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();
        File::create(
            temp_dir
                .path()
                .join("logs_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();

        // Segment 0 should not be in missing segments
        let missing = scanner.find_missing_segments(0, 499999, 500000).unwrap();
        assert!(missing.is_empty());
    }

    #[test]
    fn test_incomplete_segment_detection() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = DataScanner::new(temp_dir.path().to_path_buf());

        // Create only blocks and transactions (missing logs)
        File::create(
            temp_dir
                .path()
                .join("blocks_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();
        File::create(
            temp_dir
                .path()
                .join("transactions_0-499999_from_0_to_499999.parquet"),
        )
        .unwrap();
        // logs missing!

        // Segment 0 should be in missing segments (incomplete)
        let missing = scanner.find_missing_segments(0, 499999, 500000).unwrap();
        assert_eq!(missing, vec![0]);
    }
}
