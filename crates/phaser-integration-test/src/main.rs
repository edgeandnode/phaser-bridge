use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber;

mod benchmark;
mod loader;
mod validator;

use benchmark::{BenchmarkResults, BenchmarkRunner};
use loader::{DataInfo, DataLoader};
use validator::{MerkleValidator, ValidationResults};

#[derive(Parser)]
#[command(name = "phaser-test")]
#[command(about = "Integration test and benchmark for Phaser merkle proof validation")]
#[command(version)]
struct Cli {
    /// Path to directory containing parquet files
    #[arg(short, long, default_value = "./test-data")]
    data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate merkle roots for all blocks
    Validate {
        /// Segments to validate (e.g., "0,1,2" or "0-5")
        #[arg(short, long)]
        segments: Option<String>,

        /// Stop on first error
        #[arg(long)]
        fail_fast: bool,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },

    /// Benchmark merkle proof pipeline
    Benchmark {
        /// Segments to benchmark
        #[arg(short, long)]
        segments: Option<String>,

        /// Target transaction counts to test (comma-separated)
        #[arg(short, long, default_value = "100,500,1000")]
        tx_counts: String,

        /// Number of iterations per benchmark
        #[arg(short, long, default_value = "10")]
        iterations: usize,

        /// Output results as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show statistics about available data
    Info,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    // Setup logging
    tracing_subscriber::fmt()
        .with_env_filter(cli.log_level)
        .init();

    // Check data directory exists
    if !cli.data_dir.exists() {
        eprintln!(
            "\n❌ Data directory does not exist: {}\n",
            cli.data_dir.display()
        );
        eprintln!("Run phaser-query sync first to generate parquet files:");
        eprintln!("  cargo run --bin phaser-query -- sync -c 1 -f 0 -t 10000\n");
        std::process::exit(1);
    }

    match cli.command {
        Commands::Validate {
            segments,
            fail_fast,
            json,
        } => {
            let loader = DataLoader::new(cli.data_dir);
            let validator = MerkleValidator::new();

            let segment_list = parse_segments(&segments, &loader)?;

            let results = validator
                .validate_segments(&loader, &segment_list, fail_fast)
                .await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else {
                print_validation_results(&results);
            }

            if results.failed_blocks > 0 {
                std::process::exit(1);
            }
        }

        Commands::Benchmark {
            segments,
            tx_counts,
            iterations,
            json,
        } => {
            let loader = DataLoader::new(cli.data_dir);
            let runner = BenchmarkRunner::new(iterations);

            let segment_list = parse_segments(&segments, &loader)?;
            let tx_count_list = parse_tx_counts(&tx_counts)?;

            let results = runner
                .run_benchmarks(&loader, &segment_list, &tx_count_list)
                .await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else {
                print_benchmark_results(&results);
            }
        }

        Commands::Info => {
            let loader = DataLoader::new(cli.data_dir);
            let info = loader.get_info()?;
            print_data_info(&info);
        }
    }

    Ok(())
}

fn parse_segments(
    segments_arg: &Option<String>,
    loader: &DataLoader,
) -> Result<Vec<u64>, anyhow::Error> {
    match segments_arg {
        Some(s) => {
            // Parse "0,1,2" or "0-5"
            if s.contains('-') {
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() != 2 {
                    anyhow::bail!("Invalid range format: {}", s);
                }
                let start: u64 = parts[0].parse()?;
                let end: u64 = parts[1].parse()?;
                Ok((start..=end).collect())
            } else {
                s.split(',')
                    .map(|seg| seg.trim().parse().map_err(Into::into))
                    .collect()
            }
        }
        None => {
            // Use all available segments
            loader.list_segments()
        }
    }
}

fn parse_tx_counts(tx_counts: &str) -> Result<Vec<usize>, anyhow::Error> {
    tx_counts
        .split(',')
        .map(|c| c.trim().parse().map_err(Into::into))
        .collect()
}

fn print_validation_results(results: &ValidationResults) {
    println!("\n=== Merkle Validation Results ===\n");
    println!("Total blocks:     {}", results.total_blocks);
    println!("Validated:        {}", results.validated_blocks);
    println!("Failed:           {}", results.failed_blocks);
    println!("Success rate:     {:.2}%", results.success_rate());
    println!("Total time:       {:?}", results.total_time);
    println!("Avg time/block:   {:?}", results.avg_time_per_block());

    if !results.failures.is_empty() {
        println!("\n❌ Failed blocks:");
        for failure in &results.failures {
            println!("   Block {}: {}", failure.block_num, failure.error);
        }
    } else {
        println!("\n✅ All blocks validated successfully!");
    }

    println!();
}

fn print_benchmark_results(results: &BenchmarkResults) {
    println!("\n=== Merkle Pipeline Benchmarks ===\n");

    for stage in &results.stages {
        println!("Stage: {}", stage.name);
        println!("  Iterations:  {}", stage.iterations);
        println!("  Mean:        {:?}", stage.mean);
        println!("  Median:      {:?}", stage.median);
        println!("  Std dev:     {:?}", stage.std_dev);
        println!("  Min:         {:?}", stage.min);
        println!("  Max:         {:?}", stage.max);
        println!();
    }
}

fn print_data_info(info: &DataInfo) {
    println!("\n=== Data Directory Info ===\n");
    println!("Path:             {}", info.path.display());
    println!("Total segments:   {}", info.segments.len());
    println!("Total blocks:     {}", info.total_blocks);
    println!("Total txs:        {}", info.total_transactions);

    if info.total_blocks > 0 {
        println!("Block range:      {} - {}", info.min_block, info.max_block);
    }

    if !info.segments.is_empty() {
        println!("\nSegments:");
        for seg in &info.segments {
            println!(
                "  Segment {}: {} blocks, {} txs",
                seg.id, seg.blocks, seg.transactions
            );
        }
    }

    println!();
}
