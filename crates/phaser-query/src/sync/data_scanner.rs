use anyhow::{Context, Result};
use core_executor::ThreadPoolExecutor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// Represents a block range that has been synced
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRange {
    pub start: u64,
    pub end: u64,
}

/// Work needed for a specific segment
#[derive(Debug, Clone)]
pub struct SegmentWork {
    pub segment_num: u64,
    pub segment_from: u64,
    pub segment_to: u64,
    pub missing_blocks: Vec<BlockRange>,
    pub missing_transactions: Vec<BlockRange>,
    pub missing_logs: Vec<BlockRange>,
}

impl SegmentWork {
    pub fn is_complete(&self) -> bool {
        self.missing_blocks.is_empty()
            && self.missing_transactions.is_empty()
            && self.missing_logs.is_empty()
    }

    pub fn missing_types(&self) -> Vec<String> {
        let mut types = Vec::new();
        if !self.missing_blocks.is_empty() {
            types.push("blocks".to_string());
        }
        if !self.missing_transactions.is_empty() {
            types.push("transactions".to_string());
        }
        if !self.missing_logs.is_empty() {
            types.push("logs".to_string());
        }
        types
    }
}

/// Analysis of what segments need syncing
#[derive(Debug, Clone)]
pub struct GapAnalysis {
    pub total_segments: u64,
    pub complete_segments: Vec<u64>,
    pub segments_needing_work: Vec<SegmentWork>,
    pub cleaned_temp_files: usize,
}

impl GapAnalysis {
    pub fn complete_count(&self) -> usize {
        self.complete_segments.len()
    }

    pub fn missing_count(&self) -> usize {
        self.segments_needing_work.len()
    }

    pub fn completion_percentage(&self) -> f64 {
        if self.total_segments == 0 {
            return 100.0;
        }
        (self.complete_count() as f64 / self.total_segments as f64) * 100.0
    }

    pub fn needs_sync(&self) -> bool {
        !self.segments_needing_work.is_empty()
    }
}

/// In-memory catalog of existing data files
/// Maps data_type -> Vec<BlockRange> for fast lookups
#[derive(Debug, Clone)]
struct DataCatalog {
    blocks: Vec<BlockRange>,
    transactions: Vec<BlockRange>,
    logs: Vec<BlockRange>,
}

impl DataCatalog {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            transactions: Vec::new(),
            logs: Vec::new(),
        }
    }

    fn get_ranges(&self, data_type: &str) -> &[BlockRange] {
        match data_type {
            "blocks" => &self.blocks,
            "transactions" => &self.transactions,
            "logs" => &self.logs,
            _ => &[],
        }
    }

    fn add_range(&mut self, data_type: &str, range: BlockRange) {
        match data_type {
            "blocks" => self.blocks.push(range),
            "transactions" => self.transactions.push(range),
            "logs" => self.logs.push(range),
            _ => {}
        }
    }

    fn sort_all(&mut self) {
        self.blocks.sort_by_key(|r| r.start);
        self.transactions.sort_by_key(|r| r.start);
        self.logs.sort_by_key(|r| r.start);
    }
}

/// Scanner for detecting existing blockchain data
pub struct DataScanner {
    data_dir: PathBuf,
    executor: Arc<Mutex<ThreadPoolExecutor>>,
}

impl DataScanner {
    pub fn new(data_dir: PathBuf, executor: Arc<Mutex<ThreadPoolExecutor>>) -> Self {
        Self { data_dir, executor }
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

    /// Read block range from Parquet file statistics
    /// This reads only the metadata (file footer), not the actual data
    ///
    /// NOTE: This is synchronous blocking I/O and should be called from
    /// within the executor thread pool, not directly from async code.
    fn read_block_range_from_parquet(path: &Path) -> Result<Option<BlockRange>> {
        // Only read finalized parquet files (not .tmp files) for statistics
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            return Ok(None);
        }

        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open parquet file {:?}: {}", path, e);
                return Ok(None);
            }
        };

        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to read parquet metadata from {:?}: {}", path, e);
                return Ok(None);
            }
        };

        let parquet_metadata = builder.metadata();
        let arrow_schema = builder.schema();

        // Check if file has any rows - empty files should not be treated as valid data
        let total_rows: i64 = (0..parquet_metadata.num_row_groups())
            .map(|i| parquet_metadata.row_group(i).num_rows())
            .sum();

        if total_rows == 0 {
            debug!("Parquet file {:?} is empty (0 rows), ignoring", path);
            return Ok(None);
        }

        // Find the _block_num column index
        let block_num_col_idx = match arrow_schema.column_with_name("_block_num") {
            Some((idx, _field)) => idx,
            None => {
                debug!("No _block_num column found in {:?}", path);
                return Ok(None);
            }
        };

        // Iterate through row groups and collect min/max statistics
        let mut overall_min: Option<u64> = None;
        let mut overall_max: Option<u64> = None;

        for row_group_idx in 0..parquet_metadata.num_row_groups() {
            let row_group_metadata = parquet_metadata.row_group(row_group_idx);

            if block_num_col_idx < row_group_metadata.num_columns() {
                let column_metadata = row_group_metadata.column(block_num_col_idx);

                if let Some(stats) = column_metadata.statistics() {
                    // Parquet stores UInt64 as Int64 at the physical level
                    // We need to reinterpret the bytes
                    if let Statistics::Int64(int_stats) = stats {
                        if let (Some(&min_val), Some(&max_val)) =
                            (int_stats.min_opt(), int_stats.max_opt())
                        {
                            // Reinterpret as unsigned
                            let min_u64 = min_val as u64;
                            let max_u64 = max_val as u64;

                            overall_min = Some(overall_min.map_or(min_u64, |m| m.min(min_u64)));
                            overall_max = Some(overall_max.map_or(max_u64, |m| m.max(max_u64)));
                        }
                    } else {
                        debug!(
                            "Unexpected statistics type for _block_num column in {:?}",
                            path
                        );
                    }
                }
            }
        }

        if let (Some(start), Some(end)) = (overall_min, overall_max) {
            debug!("Read block range from {:?}: {}-{}", path, start, end);
            Ok(Some(BlockRange { start, end }))
        } else {
            debug!("No statistics found for _block_num in {:?}", path);
            Ok(None)
        }
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

    /// Find where live streaming data starts by detecting temp files
    /// Returns the block number where historical sync can safely backfill up to
    pub fn find_historical_boundary(&self, segment_size: u64) -> Result<Option<u64>> {
        if !self.data_dir.exists() {
            info!("No existing data found, historical sync can start from genesis");
            return Ok(None);
        }

        debug!(
            "Scanning for live streaming temp files in {:?}",
            self.data_dir
        );

        // Find the lowest block number in temp files (indicates live streaming start)
        let mut min_temp_block = None;
        let mut temp_files_found = 0;

        for entry in fs::read_dir(&self.data_dir).context("Failed to read data directory")? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Only look at temp files
            if !filename.ends_with(".parquet.tmp") {
                continue;
            }

            temp_files_found += 1;
            debug!("Found temp file: {}", filename);

            // Parse the block range from temp file
            if let Some(range) = self.parse_filename(&path)? {
                debug!("Parsed range: {}-{}", range.start, range.end);
                min_temp_block =
                    Some(min_temp_block.map_or(range.start, |min: u64| min.min(range.start)));
            } else {
                debug!("Failed to parse range from: {}", filename);
            }
        }

        debug!(
            "Total temp files found: {}, min_temp_block: {:?}",
            temp_files_found, min_temp_block
        );

        if let Some(min_temp) = min_temp_block {
            // Round down to segment boundary
            let segment_boundary = (min_temp / segment_size) * segment_size;
            if segment_boundary > 0 {
                let boundary = segment_boundary.saturating_sub(1);
                info!(
                    "Found live streaming temp files starting at block {}. Historical sync can backfill up to {}",
                    min_temp,
                    boundary
                );
                return Ok(Some(boundary));
            }
        }

        info!("No live streaming temp files found, no boundary needed");
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
    pub fn clean_conflicting_temp_files(
        &self,
        segments: &[u64],
        segment_size: u64,
    ) -> Result<usize> {
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
            if !filename_str.ends_with(".parquet.tmp") && !filename_str.ends_with(".tmp") {
                continue;
            }

            // Skip live streaming temp files - they're actively being written
            if filename_str.starts_with("live_") {
                debug!("Preserving live streaming temp file: {}", path.display());
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

    /// Build in-memory catalog of all existing data files with parallel processing
    /// Uses core-executor to read parquet metadata from multiple files concurrently
    async fn build_catalog(&self) -> Result<DataCatalog> {
        let mut catalog = DataCatalog::new();

        if !self.data_dir.exists() {
            return Ok(catalog);
        }

        info!("Building data catalog from directory scan...");
        let start_time = std::time::Instant::now();

        // First pass: collect all file paths that need processing
        #[derive(Debug)]
        struct FileTask {
            path: PathBuf,
            data_type: String,
            is_empty: bool,
        }

        let mut tasks = Vec::new();

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Skip non-data files
            if !filename_str.contains(".parquet") && !filename_str.ends_with(".empty") {
                continue;
            }

            // Skip temp files
            if filename_str.ends_with(".parquet.tmp") {
                continue;
            }

            // Determine data type from filename
            let data_type = if filename_str.starts_with("blocks_") {
                "blocks"
            } else if filename_str.starts_with("transactions_") {
                "transactions"
            } else if filename_str.starts_with("logs_") {
                "logs"
            } else {
                continue; // Unknown file type
            };

            let is_empty = filename_str.ends_with(".empty");
            tasks.push(FileTask {
                path,
                data_type: data_type.to_string(),
                is_empty,
            });
        }

        let total_files = tasks.len();
        info!(
            "Found {} data files to catalog (blocks, transactions, logs)",
            total_files
        );

        if total_files == 0 {
            return Ok(catalog);
        }

        // Process files in parallel using the executor
        let futures: Vec<_> = {
            let mut executor = self.executor.lock().unwrap();
            tasks
                .into_iter()
                .enumerate()
                .map(|(idx, task)| {
                    let data_dir = self.data_dir.clone();
                    executor.spawn_on_any(async move {
                        // Progress logging every 50 files
                        if idx > 0 && idx % 50 == 0 {
                            info!("Cataloging progress: {}/{} files", idx, total_files);
                        }

                        let range = if task.is_empty {
                            // Empty marker file - parse filename for range
                            DataScanner::parse_filename_static(&task.path)?
                        } else {
                            // Parquet file - read block range from statistics (blocking I/O in executor)
                            Self::read_block_range_from_parquet(&task.path)?
                        };

                        Ok::<_, anyhow::Error>((task.data_type, range))
                    })
                })
                .collect()
        };
        // Executor lock released here

        // Await all futures and collect results
        for future in futures {
            match future.await {
                Ok(Ok((data_type, Some(range)))) => {
                    catalog.add_range(&data_type, range);
                }
                Ok(Ok((_, None))) => {
                    // No range found, skip
                }
                Ok(Err(e)) => {
                    warn!("Failed to read file range: {}", e);
                }
                Err(e) => {
                    warn!("Task execution failed: {:?}", e);
                }
            }
        }

        // Sort all ranges for efficient gap detection
        catalog.sort_all();

        let elapsed = start_time.elapsed();
        info!(
            "Catalog built in {:.2}s: {} blocks, {} transactions, {} logs files",
            elapsed.as_secs_f64(),
            catalog.blocks.len(),
            catalog.transactions.len(),
            catalog.logs.len()
        );

        Ok(catalog)
    }

    /// Static version of parse_filename for use in executor tasks
    fn parse_filename_static(path: &Path) -> Result<Option<BlockRange>> {
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
                        .trim_end_matches(".parquet")
                        .trim_end_matches(".empty");

                    if let (Ok(start), Ok(end)) = (start_str.parse::<u64>(), end_str.parse::<u64>())
                    {
                        return Ok(Some(BlockRange { start, end }));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Compute gaps (missing ranges) for a data type within a segment
    /// Returns Vec of BlockRange representing what needs to be synced
    fn compute_gaps_in_segment(
        catalog: &DataCatalog,
        data_type: &str,
        segment_start: u64,
        segment_end: u64,
    ) -> Vec<BlockRange> {
        let ranges = catalog.get_ranges(data_type);
        if ranges.is_empty() {
            // No data at all - need entire segment
            return vec![BlockRange {
                start: segment_start,
                end: segment_end,
            }];
        }

        let mut gaps = Vec::new();
        let mut covered_up_to = segment_start.saturating_sub(1);

        for range in ranges {
            // Only consider ranges that overlap with our segment
            if range.end < segment_start || range.start > segment_end {
                continue;
            }

            // If there's a gap before this range starts
            if range.start > covered_up_to + 1 {
                let gap_start = covered_up_to + 1;
                let gap_end = (range.start - 1).min(segment_end);
                if gap_start <= gap_end && gap_start >= segment_start {
                    gaps.push(BlockRange {
                        start: gap_start.max(segment_start),
                        end: gap_end,
                    });
                }
            }

            covered_up_to = covered_up_to.max(range.end.min(segment_end));
        }

        // If there's a gap at the end
        if covered_up_to < segment_end {
            gaps.push(BlockRange {
                start: covered_up_to + 1,
                end: segment_end,
            });
        }

        gaps
    }

    /// Check if a segment is complete using the in-memory catalog
    fn is_segment_complete(
        catalog: &DataCatalog,
        data_type: &str,
        segment_start: u64,
        segment_end: u64,
    ) -> bool {
        let ranges = catalog.get_ranges(data_type);

        if ranges.is_empty() {
            return false;
        }

        // Check if the union of ranges covers [segment_start, segment_end]
        let mut covered_up_to = segment_start.saturating_sub(1);

        for range in ranges {
            // Only consider ranges that could cover this segment
            if range.end < segment_start {
                continue; // This range ends before segment starts
            }
            if range.start > segment_end {
                break; // Ranges are sorted, no more can overlap
            }

            // If there's a gap, segment is not complete
            if range.start > covered_up_to + 1 {
                return false;
            }

            // Extend coverage
            covered_up_to = covered_up_to.max(range.end);

            // If we've covered the entire segment, we're done
            if covered_up_to >= segment_end {
                return true;
            }
        }

        false
    }

    /// Analyze sync range and find gaps
    /// Returns detailed analysis of what needs syncing
    pub async fn analyze_sync_range(
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

        let mut segments_needing_work = Vec::new();
        let mut complete_segments = Vec::new();

        if !self.data_dir.exists() {
            info!(
                "Data directory doesn't exist - all {} segments need syncing",
                total_segments
            );
            for segment_num in first_segment..=last_segment {
                let segment_from = segment_num * segment_size;
                let segment_to = (segment_num + 1) * segment_size - 1;
                segments_needing_work.push(SegmentWork {
                    segment_num,
                    segment_from,
                    segment_to,
                    missing_blocks: vec![BlockRange {
                        start: segment_from,
                        end: segment_to,
                    }],
                    missing_transactions: vec![BlockRange {
                        start: segment_from,
                        end: segment_to,
                    }],
                    missing_logs: vec![BlockRange {
                        start: segment_from,
                        end: segment_to,
                    }],
                });
            }
            return Ok(GapAnalysis {
                total_segments,
                complete_segments: Vec::new(),
                segments_needing_work,
                cleaned_temp_files: 0,
            });
        }

        // Build catalog once with parallel directory scan
        let catalog = self.build_catalog().await?;

        // Now check all segments against the catalog
        for segment_num in first_segment..=last_segment {
            let segment_start = segment_num * segment_size;
            let segment_end = segment_start + segment_size - 1;

            // Compute exact missing ranges for each data type
            let missing_blocks =
                Self::compute_gaps_in_segment(&catalog, "blocks", segment_start, segment_end);
            let missing_transactions =
                Self::compute_gaps_in_segment(&catalog, "transactions", segment_start, segment_end);
            let missing_logs =
                Self::compute_gaps_in_segment(&catalog, "logs", segment_start, segment_end);

            if missing_blocks.is_empty()
                && missing_transactions.is_empty()
                && missing_logs.is_empty()
            {
                complete_segments.push(segment_num);
                debug!(
                    "Segment {} (blocks {}-{}) is complete",
                    segment_num, segment_start, segment_end
                );
            } else {
                // Build detailed work description
                let mut missing_parts = Vec::new();
                if !missing_blocks.is_empty() {
                    missing_parts.push(format!("blocks ({} ranges)", missing_blocks.len()));
                }
                if !missing_transactions.is_empty() {
                    missing_parts.push(format!("txs ({} ranges)", missing_transactions.len()));
                }
                if !missing_logs.is_empty() {
                    missing_parts.push(format!("logs ({} ranges)", missing_logs.len()));
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

                segments_needing_work.push(SegmentWork {
                    segment_num,
                    segment_from: segment_start,
                    segment_to: segment_end,
                    missing_blocks,
                    missing_transactions,
                    missing_logs,
                });
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
                        ranges.push(format!(
                            "  segments {}-{} (blocks {}-{})",
                            start, end, block_start, block_end
                        ));
                    }
                    range_start = Some(seg);
                    range_end = Some(seg);
                }
            }
            // Log final range
            if let (Some(start), Some(end)) = (range_start, range_end) {
                let block_start = start * segment_size;
                let block_end = (end + 1) * segment_size - 1;
                ranges.push(format!(
                    "  segments {}-{} (blocks {}-{})",
                    start, end, block_start, block_end
                ));
            }

            for range in ranges {
                info!("{}", range);
            }
        }

        if segments_needing_work.is_empty() {
            info!(
                "All {} segments already synced - nothing to do",
                total_segments
            );
        } else {
            info!(
                "Need to sync {} segments ({}% of range)",
                segments_needing_work.len(),
                (segments_needing_work.len() as f64 / total_segments as f64 * 100.0) as u32
            );
        }

        Ok(GapAnalysis {
            total_segments,
            complete_segments,
            segments_needing_work,
            cleaned_temp_files: 0, // Will be filled in by caller
        })
    }

    /// Legacy method - kept for backward compatibility
    /// Use analyze_sync_range() for detailed analysis
    pub async fn find_missing_segments(
        &self,
        from_block: u64,
        to_block: u64,
        segment_size: u64,
    ) -> Result<Vec<u64>> {
        let analysis = self
            .analyze_sync_range(from_block, to_block, segment_size)
            .await?;
        Ok(analysis
            .segments_needing_work
            .iter()
            .map(|w| w.segment_num)
            .collect())
    }

    /// Check if completed parquet file(s) cover a specific segment
    /// Now uses Parquet statistics instead of filename parsing
    /// One or more files can cover a segment
    fn has_completed_segment(
        &self,
        data_type: &str,
        segment_start: u64,
        segment_end: u64,
    ) -> Result<bool> {
        // Collect all ranges from completed files for this data type
        let mut ranges = Vec::new();

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Look for both parquet files and .empty marker files for this data type
            let matches_format = filename_str.starts_with(&format!("{}_from_", data_type));

            if matches_format {
                let range = if filename_str.ends_with(".empty") {
                    // Empty marker file - parse filename for range
                    debug!("Found empty marker: {}", filename_str);
                    self.parse_filename(&path)?
                } else if filename_str.ends_with(".parquet")
                    && !filename_str.ends_with(".parquet.tmp")
                {
                    // Parquet file - try to read block range from statistics
                    // Empty parquet files (0 rows) will return None and be ignored
                    Self::read_block_range_from_parquet(&path)?
                } else {
                    // Skip temp files and other extensions
                    None
                };

                if let Some(range) = range {
                    // Only consider ranges that overlap with this segment
                    if range.start <= segment_end && range.end >= segment_start {
                        debug!(
                            "File {} covers blocks {}-{} (overlaps segment {}-{})",
                            filename_str, range.start, range.end, segment_start, segment_end
                        );
                        ranges.push(range);
                    }
                }
            }
        }

        if ranges.is_empty() {
            return Ok(false);
        }

        // Sort ranges by start block
        ranges.sort_by_key(|r| r.start);

        // Check if the union of ranges covers [segment_start, segment_end]
        let mut covered_up_to = segment_start.saturating_sub(1);

        for range in ranges {
            // If there's a gap, segment is not complete
            if range.start > covered_up_to + 1 {
                debug!(
                    "Gap found for segment {}-{}: covered up to {}, next range starts at {}",
                    segment_start, segment_end, covered_up_to, range.start
                );
                return Ok(false);
            }

            // Extend coverage
            covered_up_to = covered_up_to.max(range.end);

            // If we've covered the entire segment, we're done
            if covered_up_to >= segment_end {
                debug!(
                    "Segment {}-{} fully covered (up to {})",
                    segment_start, segment_end, covered_up_to
                );
                return Ok(true);
            }
        }

        // Check if we covered the entire segment
        debug!(
            "Segment {}-{} incomplete: only covered up to {}",
            segment_start, segment_end, covered_up_to
        );
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

    /// Find missing block ranges for a specific data type within a segment
    /// Uses parquet statistics to determine what's already been downloaded
    /// Returns list of ranges that still need to be synced
    pub fn find_missing_ranges(
        &self,
        data_type: &str,
        segment_start: u64,
        segment_end: u64,
    ) -> Result<Vec<BlockRange>> {
        info!(
            "find_missing_ranges called for {} in segment {}-{}",
            data_type, segment_start, segment_end
        );
        let mut covered_ranges = Vec::new();

        if !self.data_dir.exists() {
            // Directory doesn't exist - need entire range
            info!("Data directory doesn't exist, returning full range");
            return Ok(vec![BlockRange {
                start: segment_start,
                end: segment_end,
            }]);
        }

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Check for both parquet files and .empty marker files
            if filename_str.starts_with(&format!("{}_from_", data_type)) {
                let range = if filename_str.ends_with(".empty") {
                    // Empty marker file - parse filename for range
                    info!("Found empty marker: {}", filename_str);
                    self.parse_filename(&path)?
                } else if filename_str.ends_with(".parquet") {
                    // Parquet file - try to read block range from statistics
                    // Empty parquet files (0 rows) will return None and be ignored
                    Self::read_block_range_from_parquet(&path)?
                } else {
                    // Skip temp files and other extensions
                    None
                };

                if let Some(range) = range {
                    // Only include if it overlaps with our segment
                    if range.start <= segment_end && range.end >= segment_start {
                        info!(
                            "Found {} range {}-{} from {}",
                            data_type, range.start, range.end, filename_str
                        );
                        covered_ranges.push(range);
                    }
                }
            }
        }

        if covered_ranges.is_empty() {
            // Nothing downloaded - need entire range
            info!(
                "No existing {} data found for segment {}-{}, will sync entire range",
                data_type, segment_start, segment_end
            );
            return Ok(vec![BlockRange {
                start: segment_start,
                end: segment_end,
            }]);
        }

        // Sort ranges by start block
        covered_ranges.sort_by_key(|r| r.start);

        debug!(
            "Found {} {} files covering segment {}-{}",
            covered_ranges.len(),
            data_type,
            segment_start,
            segment_end
        );

        // Find gaps in coverage
        let mut missing = Vec::new();
        let mut current_pos = segment_start;

        for range in &covered_ranges {
            if range.start > current_pos {
                // Gap before this range
                debug!(
                    "Gap in {} data: {}-{}",
                    data_type,
                    current_pos,
                    range.start - 1
                );
                missing.push(BlockRange {
                    start: current_pos,
                    end: range.start - 1,
                });
            }
            current_pos = current_pos.max(range.end + 1);
        }

        // Gap at the end?
        if current_pos <= segment_end {
            debug!(
                "Gap in {} data at end: {}-{}",
                data_type, current_pos, segment_end
            );
            missing.push(BlockRange {
                start: current_pos,
                end: segment_end,
            });
        }

        if missing.is_empty() {
            info!(
                "{} data complete for segment {}-{}",
                data_type, segment_start, segment_end
            );
        } else {
            info!(
                "{} data has {} gaps in segment {}-{}",
                data_type,
                missing.len(),
                segment_start,
                segment_end
            );
        }

        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    // Tests for compute_gaps_in_segment logic (in-memory, no files)
    #[test]
    fn test_compute_gaps_no_data() {
        let catalog = DataCatalog::new();
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start, 0);
        assert_eq!(gaps[0].end, 999);
    }

    #[test]
    fn test_compute_gaps_complete_coverage() {
        let mut catalog = DataCatalog::new();
        catalog.add_range("blocks", BlockRange { start: 0, end: 999 });
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 0, "No gaps when fully covered");
    }

    #[test]
    fn test_compute_gaps_single_gap_at_start() {
        let mut catalog = DataCatalog::new();
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 500,
                end: 999,
            },
        );
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start, 0);
        assert_eq!(gaps[0].end, 499);
    }

    #[test]
    fn test_compute_gaps_single_gap_at_end() {
        let mut catalog = DataCatalog::new();
        catalog.add_range("blocks", BlockRange { start: 0, end: 499 });
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start, 500);
        assert_eq!(gaps[0].end, 999);
    }

    #[test]
    fn test_compute_gaps_middle_gap() {
        let mut catalog = DataCatalog::new();
        catalog.add_range("blocks", BlockRange { start: 0, end: 299 });
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 700,
                end: 999,
            },
        );
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start, 300);
        assert_eq!(gaps[0].end, 699);
    }

    #[test]
    fn test_compute_gaps_multiple_gaps() {
        let mut catalog = DataCatalog::new();
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 100,
                end: 199,
            },
        );
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 400,
                end: 599,
            },
        );
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 800,
                end: 899,
            },
        );
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(
            gaps.len(),
            4,
            "Expected 4 gaps: before 100, 200-399, 600-799, after 899"
        );
        assert_eq!(gaps[0].start, 0);
        assert_eq!(gaps[0].end, 99);
        assert_eq!(gaps[1].start, 200);
        assert_eq!(gaps[1].end, 399);
        assert_eq!(gaps[2].start, 600);
        assert_eq!(gaps[2].end, 799);
        assert_eq!(gaps[3].start, 900);
        assert_eq!(gaps[3].end, 999);
    }

    #[test]
    fn test_compute_gaps_overlapping_ranges() {
        let mut catalog = DataCatalog::new();
        catalog.add_range("blocks", BlockRange { start: 0, end: 500 });
        catalog.add_range(
            "blocks",
            BlockRange {
                start: 300,
                end: 999,
            },
        ); // Overlaps
        let gaps = DataScanner::compute_gaps_in_segment(&catalog, "blocks", 0, 999);
        assert_eq!(gaps.len(), 0, "Overlapping ranges should cover everything");
    }

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
