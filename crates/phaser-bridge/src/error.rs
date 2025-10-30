use thiserror::Error;

/// Errors that can occur in bridge stream operations
#[derive(Error, Debug)]
pub enum StreamError {
    /// Bridge yielded batches but client received zero
    /// This indicates the client disconnected or stream was closed prematurely
    #[error("Stream protocol error: segment {start}-{end} yielded {yielded} batches but none were consumed by client. Client may have disconnected or closed stream prematurely.")]
    ZeroBatchesConsumed { start: u64, end: u64, yielded: u64 },

    /// Stream was closed unexpectedly
    #[error("Stream closed unexpectedly during segment {start}-{end}")]
    StreamClosed { start: u64, end: u64 },
}
