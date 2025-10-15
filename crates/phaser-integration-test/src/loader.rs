use arrow_array::RecordBatch;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use tracing::{debug, info};
use typed_arrow::prelude::*;

pub struct DataLoader {
    data_dir: PathBuf,
}

pub struct DataInfo {
    pub path: PathBuf,
    pub segments: Vec<SegmentInfo>,
    pub total_blocks: usize,
    pub total_transactions: usize,
    pub min_block: u64,
    pub max_block: u64,
}

pub struct SegmentInfo {
    pub id: u64,
    pub blocks: usize,
    pub transactions: usize,
}

pub struct SegmentData {
    pub segment_id: u64,
    pub blocks: HashMap<u64, BlockRecord>,
    pub transactions_by_block: HashMap<u64, Vec<TransactionRecord>>,
}

impl DataLoader {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub fn list_segments(&self) -> Result<Vec<u64>, anyhow::Error> {
        let mut segments = Vec::new();

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("blocks_") && name.ends_with(".parquet") {
                    if let Some(seg) = name
                        .strip_prefix("blocks_")
                        .and_then(|s| s.strip_suffix(".parquet"))
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        segments.push(seg);
                    }
                }
            }
        }

        segments.sort();
        Ok(segments)
    }

    pub fn load_segment(&self, segment_id: u64) -> Result<SegmentData, anyhow::Error> {
        info!("Loading segment {}...", segment_id);

        let mut blocks_map = HashMap::new();
        let mut txs_by_block = HashMap::new();

        // Load blocks
        let block_batches = self.load_parquet("blocks", segment_id)?;
        for batch in block_batches {
            let views = batch.iter_views::<BlockRecord>()?;
            let records: Vec<BlockRecord> = views
                .try_flatten()?
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?;

            for block in records {
                blocks_map.insert(block.block_num, block);
            }
        }

        // Load transactions
        let tx_batches = self.load_parquet("transactions", segment_id)?;
        for batch in tx_batches {
            let views = batch.iter_views::<TransactionRecord>()?;
            let records: Vec<TransactionRecord> = views
                .try_flatten()?
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?;

            for tx in records {
                txs_by_block
                    .entry(tx.block_num)
                    .or_insert_with(Vec::new)
                    .push(tx);
            }
        }

        debug!(
            "Loaded {} blocks, {} transactions",
            blocks_map.len(),
            txs_by_block.values().map(|v| v.len()).sum::<usize>()
        );

        Ok(SegmentData {
            segment_id,
            blocks: blocks_map,
            transactions_by_block: txs_by_block,
        })
    }

    pub fn get_info(&self) -> Result<DataInfo, anyhow::Error> {
        let segments = self.list_segments()?;
        let mut segment_infos = Vec::new();
        let mut total_blocks = 0;
        let mut total_transactions = 0;
        let mut min_block = u64::MAX;
        let mut max_block = 0;

        for seg_id in &segments {
            let seg_data = self.load_segment(*seg_id)?;

            let blocks_count = seg_data.blocks.len();
            let txs_count: usize = seg_data
                .transactions_by_block
                .values()
                .map(|v| v.len())
                .sum();

            total_blocks += blocks_count;
            total_transactions += txs_count;

            if let Some(&min) = seg_data.blocks.keys().min() {
                min_block = min_block.min(min);
            }
            if let Some(&max) = seg_data.blocks.keys().max() {
                max_block = max_block.max(max);
            }

            segment_infos.push(SegmentInfo {
                id: *seg_id,
                blocks: blocks_count,
                transactions: txs_count,
            });
        }

        Ok(DataInfo {
            path: self.data_dir.clone(),
            segments: segment_infos,
            total_blocks,
            total_transactions,
            min_block,
            max_block,
        })
    }

    fn load_parquet(&self, prefix: &str, segment: u64) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let filename = format!("{}_{}.parquet", prefix, segment);
        let path = self.data_dir.join(filename);

        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}
