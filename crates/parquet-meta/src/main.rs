use anyhow::Result;
use clap::{Parser, Subcommand};
use parquet::file::reader::{FileReader, SerializedFileReader};
use phaser_parquet_metadata::PhaserMetadata;
use std::fs::File;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "parquet-meta")]
#[command(about = "Read and write phaser metadata in parquet files", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show file metadata and statistics
    Show {
        /// Path to the parquet file
        file: PathBuf,

        /// Show detailed row group statistics
        #[arg(long, short = 'v')]
        verbose: bool,
    },
    /// Fix/set phaser metadata on a parquet file
    FixMeta {
        /// Path to the parquet file
        file: PathBuf,

        /// Segment start block (the full segment, e.g., 0)
        #[arg(long)]
        segment_start: u64,

        /// Segment end block (the full segment, e.g., 499999)
        #[arg(long)]
        segment_end: u64,

        /// File responsibility start (first block this file covers, e.g., 1)
        #[arg(long)]
        responsibility_start: u64,

        /// File responsibility end (last block this file covers, e.g., 499999)
        #[arg(long)]
        responsibility_end: u64,

        /// Data type (blocks, transactions, logs)
        #[arg(long)]
        data_type: String,

        /// Infer data_start/data_end from parquet statistics
        #[arg(long)]
        infer: bool,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Show { file, verbose } => show_metadata(file, verbose),
        Commands::FixMeta {
            file,
            segment_start,
            segment_end,
            responsibility_start,
            responsibility_end,
            data_type,
            infer,
        } => set_metadata(
            file,
            segment_start,
            segment_end,
            responsibility_start,
            responsibility_end,
            data_type,
            infer,
        ),
    }
}

fn show_metadata(file_path: PathBuf, verbose: bool) -> Result<()> {
    let file = File::open(&file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    println!("File: {}", file_path.display());
    println!();

    // File metadata
    let file_metadata = metadata.file_metadata();
    println!("Schema:");
    println!("  Version: {}", file_metadata.version());
    println!("  Num rows: {}", file_metadata.num_rows());
    println!();

    // Try to parse phaser metadata
    if let Some(kv_metadata) = file_metadata.key_value_metadata() {
        // Debug: show all keys
        if verbose {
            println!("All metadata keys:");
            for kv in kv_metadata {
                println!("  {}", kv.key);
            }
            println!();
        }

        match PhaserMetadata::from_key_value_metadata(kv_metadata) {
            Ok(Some(phaser_meta)) => {
                println!("Phaser metadata:");
                println!("  Version: {}", phaser_meta.version);
                println!(
                    "  Segment: {}-{}",
                    phaser_meta.segment_start, phaser_meta.segment_end
                );
                println!(
                    "  Responsibility: {}-{}",
                    phaser_meta.responsibility_start, phaser_meta.responsibility_end
                );
                println!(
                    "  Data range: {}-{}",
                    phaser_meta.data_start, phaser_meta.data_end
                );
                println!("  Data type: {}", phaser_meta.data_type);
                println!();
            }
            Ok(None) => {
                println!("No phaser.meta key found");
                println!();
            }
            Err(e) => {
                println!("Error parsing phaser metadata: {e}");
                println!();
            }
        }

        // Show other custom metadata (filter out ARROW schema and phaser.meta)
        let other_metadata: Vec<_> = kv_metadata
            .iter()
            .filter(|kv| !kv.key.starts_with("ARROW:") && kv.key != "phaser.meta")
            .collect();

        if !other_metadata.is_empty() {
            println!("Other custom metadata:");
            for kv in other_metadata {
                if let Some(value) = &kv.value {
                    println!("  {}: {}", kv.key, value);
                } else {
                    println!("  {}: (no value)", kv.key);
                }
            }
            println!();
        }
    }

    // Row group statistics
    println!("Row groups: {}", metadata.num_row_groups());

    // Extract block range from first and last row group statistics
    let first_row_group = metadata.row_group(0);
    let last_row_group = metadata.row_group(metadata.num_row_groups() - 1);

    // block_num is typically the first column (index 0)
    if let Some(first_stats) = first_row_group.column(0).statistics() {
        if let Some(last_stats) = last_row_group.column(0).statistics() {
            if let (Some(min_bytes), Some(max_bytes)) =
                (first_stats.min_bytes_opt(), last_stats.max_bytes_opt())
            {
                // Parse as u64 little-endian
                if min_bytes.len() >= 8 && max_bytes.len() >= 8 {
                    let min_block = u64::from_le_bytes([
                        min_bytes[0],
                        min_bytes[1],
                        min_bytes[2],
                        min_bytes[3],
                        min_bytes[4],
                        min_bytes[5],
                        min_bytes[6],
                        min_bytes[7],
                    ]);
                    let max_block = u64::from_le_bytes([
                        max_bytes[0],
                        max_bytes[1],
                        max_bytes[2],
                        max_bytes[3],
                        max_bytes[4],
                        max_bytes[5],
                        max_bytes[6],
                        max_bytes[7],
                    ]);
                    println!(
                        "  Block range (from statistics): {min_block}-{max_block}"
                    );
                }
            }
        }
    }

    if verbose {
        println!();
        for i in 0..metadata.num_row_groups() {
            let row_group = metadata.row_group(i);
            println!(
                "  Row group {}: {} rows, {} columns",
                i,
                row_group.num_rows(),
                row_group.num_columns()
            );

            // Show statistics for first few columns if available
            for col_idx in 0..row_group.num_columns().min(3) {
                let col = row_group.column(col_idx);
                if let Some(stats) = col.statistics() {
                    println!(
                        "    Column {}: min={:?}, max={:?}, null_count={:?}",
                        col_idx,
                        stats.min_bytes_opt(),
                        stats.max_bytes_opt(),
                        stats.null_count_opt()
                    );
                }
            }
        }
    }

    Ok(())
}

fn set_metadata(
    file_path: PathBuf,
    segment_start: u64,
    segment_end: u64,
    responsibility_start: u64,
    responsibility_end: u64,
    data_type: String,
    infer: bool,
) -> Result<()> {
    // Determine data_start and data_end
    let (data_start, data_end) = if infer {
        // Infer from statistics
        let file = File::open(&file_path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let first_row_group = metadata.row_group(0);
        let last_row_group = metadata.row_group(metadata.num_row_groups() - 1);

        let min_block = if let Some(stats) = first_row_group.column(0).statistics() {
            if let Some(min_bytes) = stats.min_bytes_opt() {
                if min_bytes.len() >= 8 {
                    u64::from_le_bytes([
                        min_bytes[0],
                        min_bytes[1],
                        min_bytes[2],
                        min_bytes[3],
                        min_bytes[4],
                        min_bytes[5],
                        min_bytes[6],
                        min_bytes[7],
                    ])
                } else {
                    anyhow::bail!("Cannot infer data_start from statistics");
                }
            } else {
                anyhow::bail!("Cannot infer data_start from statistics");
            }
        } else {
            anyhow::bail!("Cannot infer data_start from statistics");
        };

        let max_block = if let Some(stats) = last_row_group.column(0).statistics() {
            if let Some(max_bytes) = stats.max_bytes_opt() {
                if max_bytes.len() >= 8 {
                    u64::from_le_bytes([
                        max_bytes[0],
                        max_bytes[1],
                        max_bytes[2],
                        max_bytes[3],
                        max_bytes[4],
                        max_bytes[5],
                        max_bytes[6],
                        max_bytes[7],
                    ])
                } else {
                    anyhow::bail!("Cannot infer data_end from statistics");
                }
            } else {
                anyhow::bail!("Cannot infer data_end from statistics");
            }
        } else {
            anyhow::bail!("Cannot infer data_end from statistics");
        };

        println!(
            "Inferred data_start={min_block}, data_end={max_block} from statistics"
        );
        (min_block, max_block)
    } else {
        anyhow::bail!("--infer is required (explicit data_start/data_end not yet supported)");
    };

    // Create metadata struct
    let phaser_meta = PhaserMetadata::new(
        segment_start,
        segment_end,
        responsibility_start,
        responsibility_end,
        data_start,
        data_end,
        data_type,
    );

    println!("Setting phaser metadata on {}", file_path.display());
    println!("  Segment: {segment_start}-{segment_end}");
    println!(
        "  Responsibility: {responsibility_start}-{responsibility_end}"
    );
    println!("  Data range: {data_start}-{data_end}");
    println!("  Data type: {}", phaser_meta.data_type);
    println!();

    // Update the file with new metadata
    print!("Rewriting file with updated metadata... ");
    PhaserMetadata::update_file_metadata(&file_path, &phaser_meta)?;
    println!("done");

    Ok(())
}
