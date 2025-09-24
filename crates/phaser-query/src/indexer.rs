use crate::catalog::RocksDbCatalog;
use crate::index::{BlockPointer, FileInfo, LogPointer, TransactionPointer};
use crate::PhaserConfig;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Build indexes from Parquet files in the specified directories
pub async fn build_indexes(catalog: &RocksDbCatalog, config: &PhaserConfig) -> Result<()> {
    info!("Building indexes from Parquet files");

    // Index files from historical directory
    let historical_dir = config.historical_dir();
    if historical_dir.exists() {
        info!("Indexing historical data from {:?}", historical_dir);
        index_directory(catalog, &historical_dir).await?;
    }

    // NOTE: We don't index streaming directory here anymore.
    // Streaming files are indexed when they're finalized during file rotation.

    info!("Index building complete");
    Ok(())
}

/// Index only historical directory (used by periodic background task)
pub async fn index_historical_directory(catalog: &RocksDbCatalog, dir: &Path) -> Result<()> {
    info!("Indexing historical data from {:?}", dir);
    index_directory(catalog, dir).await
}

/// Index all Parquet files in a directory
async fn index_directory(catalog: &RocksDbCatalog, dir: &Path) -> Result<()> {
    // Index block files
    let block_files = scan_files_by_type(dir, "blocks")?;
    info!("Found {} block files in {:?}", block_files.len(), dir);
    for path in &block_files {
        index_block_file(catalog, path).await?;
    }

    // Index transaction files
    let tx_files = scan_files_by_type(dir, "transactions")?;
    info!("Found {} transaction files in {:?}", tx_files.len(), dir);
    for path in &tx_files {
        index_transaction_file(catalog, path).await?;
    }

    // Index log files
    let log_files = scan_files_by_type(dir, "logs")?;
    info!("Found {} log files in {:?}", log_files.len(), dir);
    for path in &log_files {
        index_log_file(catalog, path).await?;
    }

    Ok(())
}

/// Scan directory for Parquet files of a specific type
fn scan_files_by_type(dir: &Path, file_type: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !dir.exists() {
        debug!("Directory {:?} does not exist, skipping scan", dir);
        return Ok(files);
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                // Match files like "blocks-*.parquet", "transactions-*.parquet", "logs-*.parquet"
                // Also match chunked files like "blocks-000000-000500-chunk-*.parquet"
                if name.starts_with(file_type) && name.ends_with(".parquet") {
                    files.push(path);
                }
            }
        }
    }

    files.sort(); // Ensure consistent ordering
    Ok(files)
}

/// Index a block Parquet file
async fn index_block_file(catalog: &RocksDbCatalog, path: &PathBuf) -> Result<()> {
    debug!("Indexing block file: {:?}", path);

    // TODO: Parse file name to get block range
    // TODO: Open Parquet file and read metadata
    // TODO: Register file in BLOCK_FILES index
    // TODO: Iterate through rows and add to BLOCKS index

    // Placeholder: Extract block range from filename
    // Expected format: blocks-${segment}.parquet or blocks-${segment}-chunk-${start}-${end}.parquet
    let file_info = FileInfo {
        file_path: path.to_string_lossy().to_string(),
        block_start: 0,    // TODO: Parse from filename
        block_end: 999999, // TODO: Parse from filename
        row_count: 0,      // TODO: Get from Parquet metadata
    };

    catalog.put_block_file(&file_info)?;

    Ok(())
}

/// Index a transaction Parquet file
async fn index_transaction_file(catalog: &RocksDbCatalog, path: &PathBuf) -> Result<()> {
    debug!("Indexing transaction file: {:?}", path);

    // TODO: Similar to block indexing
    // TODO: Register in TX_FILES index
    // TODO: Iterate through rows and add to TRANSACTIONS index

    Ok(())
}

/// Index a log Parquet file
async fn index_log_file(catalog: &RocksDbCatalog, path: &PathBuf) -> Result<()> {
    debug!("Indexing log file: {:?}", path);

    // TODO: Similar to block indexing
    // TODO: Register in LOG_FILES index
    // TODO: Iterate through rows and add to LOGS index

    Ok(())
}
