//! Configuration for sync operations

use std::time::Duration;

/// Retry policy configuration for sync operations
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Initial backoff duration for retries
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Maximum number of retries without making progress before giving up
    pub max_retries_without_progress: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_retries_without_progress: 5,
        }
    }
}

impl RetryPolicy {
    /// Calculate backoff duration for a given retry count
    pub fn backoff_for_retry(&self, retry_count: u32) -> Duration {
        let backoff_secs = self
            .initial_backoff
            .as_secs()
            .saturating_mul(2u64.pow(retry_count.saturating_sub(1)));
        Duration::from_secs(backoff_secs.min(self.max_backoff.as_secs()))
    }
}

/// Configuration for the PhaserSyncer
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Size of each segment in blocks (e.g., 500_000)
    pub segment_size: u64,
    /// Maximum number of concurrent workers
    pub max_workers: u32,
    /// Maximum number of segments that can sync logs concurrently
    /// (logs are typically memory-intensive)
    pub max_concurrent_log_segments: u32,
    /// Retry policy for transient errors
    pub retry_policy: RetryPolicy,
    /// Batch size for queries (number of blocks per request)
    pub batch_size: u32,
    /// Whether to enable trace data in log queries
    pub enable_traces: bool,
    /// Record batch target size in bytes
    pub record_batch_bytes_target: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            segment_size: 500_000,
            max_workers: 4,
            max_concurrent_log_segments: 1,
            retry_policy: RetryPolicy::default(),
            batch_size: 100,
            enable_traces: true,
            record_batch_bytes_target: 64 * 1024 * 1024,
        }
    }
}

impl SyncConfig {
    /// Create a new SyncConfig with the given segment size
    pub fn with_segment_size(segment_size: u64) -> Self {
        Self {
            segment_size,
            ..Default::default()
        }
    }

    /// Set the maximum number of workers
    pub fn with_max_workers(mut self, max_workers: u32) -> Self {
        self.max_workers = max_workers;
        self
    }

    /// Set the maximum concurrent log segments
    pub fn with_max_concurrent_log_segments(mut self, max: u32) -> Self {
        self.max_concurrent_log_segments = max;
        self
    }

    /// Set the retry policy
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Set whether to enable traces
    pub fn with_traces(mut self, enable: bool) -> Self {
        self.enable_traces = enable;
        self
    }

    /// Set the batch size for queries (number of blocks per request)
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the record batch target size in bytes
    pub fn with_record_batch_bytes_target(mut self, target: usize) -> Self {
        self.record_batch_bytes_target = target;
        self
    }

    /// Calculate the segment number for a given block
    pub fn segment_for_block(&self, block: u64) -> u64 {
        block / self.segment_size
    }

    /// Calculate the start block for a segment
    pub fn segment_start(&self, segment_num: u64) -> u64 {
        segment_num * self.segment_size
    }

    /// Calculate the end block for a segment
    pub fn segment_end(&self, segment_num: u64) -> u64 {
        (segment_num + 1) * self.segment_size - 1
    }

    /// Get the first and last segment numbers for a block range
    pub fn segment_range(&self, from_block: u64, to_block: u64) -> (u64, u64) {
        let first_segment = self.segment_for_block(from_block);
        let last_segment = self.segment_for_block(to_block);
        (first_segment, last_segment)
    }

    /// Calculate total segments in a block range
    pub fn total_segments(&self, from_block: u64, to_block: u64) -> u64 {
        let (first, last) = self.segment_range(from_block, to_block);
        last - first + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_calculations() {
        let config = SyncConfig::with_segment_size(500_000);

        assert_eq!(config.segment_for_block(0), 0);
        assert_eq!(config.segment_for_block(499_999), 0);
        assert_eq!(config.segment_for_block(500_000), 1);
        assert_eq!(config.segment_for_block(1_000_000), 2);

        assert_eq!(config.segment_start(0), 0);
        assert_eq!(config.segment_start(1), 500_000);

        assert_eq!(config.segment_end(0), 499_999);
        assert_eq!(config.segment_end(1), 999_999);
    }

    #[test]
    fn test_segment_range() {
        let config = SyncConfig::with_segment_size(500_000);

        assert_eq!(config.segment_range(0, 499_999), (0, 0));
        assert_eq!(config.segment_range(0, 1_000_000), (0, 2));
        assert_eq!(config.segment_range(500_000, 999_999), (1, 1));
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy::default();

        // First retry: 1s
        assert_eq!(policy.backoff_for_retry(1), Duration::from_secs(1));
        // Second retry: 2s
        assert_eq!(policy.backoff_for_retry(2), Duration::from_secs(2));
        // Third retry: 4s
        assert_eq!(policy.backoff_for_retry(3), Duration::from_secs(4));
        // Should cap at max_backoff
        assert_eq!(policy.backoff_for_retry(10), Duration::from_secs(60));
    }
}
