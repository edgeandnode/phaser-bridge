//! Bridge comparison test tool
//!
//! Tests that phaser-query can work with any bridge implementation by comparing
//! discovery metadata, schemas, and data between two bridge endpoints.

use clap::{Parser, Subcommand};
use futures::StreamExt;
use phaser_client::{GenericQuery, PhaserClient};
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "bridge-test")]
#[command(about = "Test and compare bridge implementations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Discover bridge capabilities and print metadata
    Discover {
        /// Bridge endpoint (e.g., http://127.0.0.1:8090)
        #[arg(short, long)]
        endpoint: String,
    },

    /// Compare discovery metadata between two bridges
    Compare {
        /// First bridge endpoint
        #[arg(long)]
        bridge1: String,
        /// Second bridge endpoint
        #[arg(long)]
        bridge2: String,
        /// Output file for comparison results (JSON)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Fetch data from a bridge and validate schema
    Fetch {
        /// Bridge endpoint
        #[arg(short, long)]
        endpoint: String,
        /// Table to fetch (blocks, transactions, logs)
        #[arg(short, long)]
        table: String,
        /// Start block
        #[arg(long)]
        start: u64,
        /// End block
        #[arg(long)]
        end: u64,
        /// Output file for data (Parquet)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Compare data between two bridges for the same block range
    CompareData {
        /// First bridge endpoint
        #[arg(long)]
        bridge1: String,
        /// Second bridge endpoint
        #[arg(long)]
        bridge2: String,
        /// Table to compare
        #[arg(short, long)]
        table: String,
        /// Start block
        #[arg(long)]
        start: u64,
        /// End block
        #[arg(long)]
        end: u64,
        /// Output directory for comparison results
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Discover { endpoint } => {
            discover_bridge(&endpoint).await?;
        }
        Commands::Compare {
            bridge1,
            bridge2,
            output,
        } => {
            compare_bridges(&bridge1, &bridge2, output).await?;
        }
        Commands::Fetch {
            endpoint,
            table,
            start,
            end,
            output,
        } => {
            fetch_data(&endpoint, &table, start, end, output).await?;
        }
        Commands::CompareData {
            bridge1,
            bridge2,
            table,
            start,
            end,
            output,
        } => {
            compare_data(&bridge1, &bridge2, &table, start, end, output).await?;
        }
    }

    Ok(())
}

async fn discover_bridge(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Discovering bridge at: {}", endpoint);

    let mut client = PhaserClient::connect(endpoint.to_string()).await?;
    let capabilities = client.discover().await?;

    println!("Bridge Discovery Results:");
    println!("========================");
    println!("Name: {}", capabilities.name);
    println!("Version: {}", capabilities.version);
    println!("Protocol: {}", capabilities.protocol);
    println!("Position Label: {}", capabilities.position_label);
    println!(
        "Position Range: {} - {}",
        capabilities.oldest_position, capabilities.current_position
    );
    println!();
    println!("Tables:");
    for table in &capabilities.tables {
        println!("  - {}", table.name);
        println!("    Position column: {}", table.position_column);
        println!("    Supported modes: {:?}", table.supported_modes);
        if !table.sorted_by.is_empty() {
            println!("    Sorted by: {:?}", table.sorted_by);
        }
    }
    println!();
    println!("Metadata:");
    println!("{}", serde_json::to_string_pretty(&capabilities.metadata)?);

    Ok(())
}

async fn compare_bridges(
    bridge1: &str,
    bridge2: &str,
    output: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Comparing bridges: {} vs {}", bridge1, bridge2);

    let mut client1 = PhaserClient::connect(bridge1.to_string()).await?;
    let mut client2 = PhaserClient::connect(bridge2.to_string()).await?;

    let caps1 = client1.discover().await?;
    let caps2 = client2.discover().await?;

    let mut issues: Vec<String> = vec![];

    // Compare protocol
    if caps1.protocol != caps2.protocol {
        issues.push(format!(
            "Protocol mismatch: {} vs {}",
            caps1.protocol, caps2.protocol
        ));
    }

    // Compare tables
    let tables1: std::collections::HashSet<_> = caps1.tables.iter().map(|t| &t.name).collect();
    let tables2: std::collections::HashSet<_> = caps2.tables.iter().map(|t| &t.name).collect();

    for table in tables1.difference(&tables2) {
        issues.push(format!("Table '{table}' only in bridge1"));
    }
    for table in tables2.difference(&tables1) {
        issues.push(format!("Table '{table}' only in bridge2"));
    }

    // Compare common tables
    for table_name in tables1.intersection(&tables2) {
        let t1 = caps1.tables.iter().find(|t| &t.name == *table_name);
        let t2 = caps2.tables.iter().find(|t| &t.name == *table_name);

        if let (Some(t1), Some(t2)) = (t1, t2)
            && t1.position_column != t2.position_column
        {
            issues.push(format!(
                "Table '{}' position column mismatch: {} vs {}",
                table_name, t1.position_column, t2.position_column
            ));
        }
    }

    // Print results
    println!("Bridge Comparison Results:");
    println!("==========================");
    println!("Bridge 1: {} v{} ({})", caps1.name, caps1.version, bridge1);
    println!("Bridge 2: {} v{} ({})", caps2.name, caps2.version, bridge2);
    println!();

    if issues.is_empty() {
        println!("PASS: No differences found in discovery metadata");
    } else {
        println!("DIFFERENCES FOUND ({}):", issues.len());
        for issue in &issues {
            println!("  - {issue}");
        }
    }

    // Save to file if requested
    if let Some(output_path) = output {
        let result = serde_json::json!({
            "bridge1": {
                "endpoint": bridge1,
                "name": caps1.name,
                "version": caps1.version,
                "protocol": caps1.protocol,
                "tables": caps1.tables.iter().map(|t| &t.name).collect::<Vec<_>>(),
            },
            "bridge2": {
                "endpoint": bridge2,
                "name": caps2.name,
                "version": caps2.version,
                "protocol": caps2.protocol,
                "tables": caps2.tables.iter().map(|t| &t.name).collect::<Vec<_>>(),
            },
            "issues": issues,
            "pass": issues.is_empty(),
        });
        std::fs::write(&output_path, serde_json::to_string_pretty(&result)?)?;
        info!("Results written to: {:?}", output_path);
    }

    Ok(())
}

async fn fetch_data(
    endpoint: &str,
    table: &str,
    start: u64,
    end: u64,
    _output: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Fetching {} from {} (blocks {}-{})",
        table, endpoint, start, end
    );

    let mut client = PhaserClient::connect(endpoint.to_string()).await?;

    let query = GenericQuery::historical(table, start, end);

    let mut stream = client.query(query).await?;

    let mut total_rows = 0u64;
    let mut total_batches = 0u64;

    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => {
                total_batches += 1;
                total_rows += batch.num_rows() as u64;
                info!(
                    "Batch {}: {} rows, {} columns",
                    total_batches,
                    batch.num_rows(),
                    batch.num_columns()
                );
            }
            Err(e) => {
                error!("Error fetching batch: {}", e);
                return Err(e.into());
            }
        }
    }

    println!();
    println!("Fetch Results:");
    println!("==============");
    println!("Total batches: {total_batches}");
    println!("Total rows: {total_rows}");

    Ok(())
}

async fn compare_data(
    bridge1: &str,
    bridge2: &str,
    table: &str,
    start: u64,
    end: u64,
    output: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Comparing {} data between bridges (blocks {}-{})",
        table, start, end
    );

    let mut client1 = PhaserClient::connect(bridge1.to_string()).await?;
    let mut client2 = PhaserClient::connect(bridge2.to_string()).await?;

    let query = GenericQuery::historical(table, start, end);

    // Fetch from both bridges
    let stream1 = client1.query(query.clone()).await?;
    let stream2 = client2.query(query).await?;

    let batches1: Vec<_> = stream1
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    let batches2: Vec<_> = stream2
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let rows1: u64 = batches1.iter().map(|b| b.num_rows() as u64).sum();
    let rows2: u64 = batches2.iter().map(|b| b.num_rows() as u64).sum();

    println!();
    println!("Data Comparison Results:");
    println!("========================");
    println!("Table: {table}");
    println!("Block range: {start} - {end}");
    println!();
    println!("Bridge 1: {} batches, {} rows", batches1.len(), rows1);
    println!("Bridge 2: {} batches, {} rows", batches2.len(), rows2);

    let mut issues: Vec<String> = vec![];

    if rows1 != rows2 {
        issues.push(format!("Row count mismatch: {rows1} vs {rows2}"));
    }

    // Compare schemas from first batch
    if let (Some(b1), Some(b2)) = (batches1.first(), batches2.first()) {
        let schema1 = b1.schema();
        let schema2 = b2.schema();

        let fields1: std::collections::HashSet<_> =
            schema1.fields().iter().map(|f| f.name()).collect();
        let fields2: std::collections::HashSet<_> =
            schema2.fields().iter().map(|f| f.name()).collect();

        for field in fields1.difference(&fields2) {
            issues.push(format!("Field '{field}' only in bridge1"));
        }
        for field in fields2.difference(&fields1) {
            issues.push(format!("Field '{field}' only in bridge2"));
        }

        // Check types for common fields
        for field_name in fields1.intersection(&fields2) {
            let f1 = schema1.field_with_name(field_name).ok();
            let f2 = schema2.field_with_name(field_name).ok();

            if let (Some(f1), Some(f2)) = (f1, f2)
                && f1.data_type() != f2.data_type()
            {
                issues.push(format!(
                    "Field '{}' type mismatch: {:?} vs {:?}",
                    field_name,
                    f1.data_type(),
                    f2.data_type()
                ));
            }
        }
    }

    if issues.is_empty() {
        println!();
        println!("PASS: Schema and row counts match");
    } else {
        println!();
        println!("DIFFERENCES FOUND:");
        for issue in &issues {
            println!("  - {issue}");
        }
    }

    // Save to file if requested
    if let Some(output_path) = output {
        let result = serde_json::json!({
            "table": table,
            "start_block": start,
            "end_block": end,
            "bridge1": {
                "endpoint": bridge1,
                "batches": batches1.len(),
                "rows": rows1,
            },
            "bridge2": {
                "endpoint": bridge2,
                "batches": batches2.len(),
                "rows": rows2,
            },
            "issues": issues,
            "pass": issues.is_empty(),
        });
        std::fs::write(&output_path, serde_json::to_string_pretty(&result)?)?;
        info!("Results written to: {:?}", output_path);
    }

    Ok(())
}
