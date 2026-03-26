//! Error types for sync operations
//!
//! This module provides rich error types with categorization for intelligent
//! retry decisions. Errors are categorized based on their message content
//! to determine if they are transient (should retry) or permanent (should fail).

use std::fmt;

/// Categorize an error message into an ErrorCategory
/// This is the single source of truth for error categorization based on message content
pub fn categorize_error_message(err_lower: &str) -> ErrorCategory {
    // Connection errors
    if err_lower.contains("connection") || err_lower.contains("connect") {
        return ErrorCategory::Connection;
    }

    // Timeout errors
    if err_lower.contains("timeout") || err_lower.contains("timed out") {
        return ErrorCategory::Timeout;
    }

    // Cancelled errors
    if err_lower.contains("cancelled") || err_lower.contains("canceled") {
        return ErrorCategory::Cancelled;
    }

    // Block/data not found - non-transient for historical sync
    if err_lower.contains("header not found") || err_lower.contains("block not found") {
        return ErrorCategory::NoData;
    }

    // No data / empty responses
    if err_lower.contains("no data") || err_lower.contains("empty") {
        return ErrorCategory::NoData;
    }

    // Stuck worker errors
    if err_lower.contains("failed to make progress") {
        return ErrorCategory::StuckWorker;
    }

    // Protocol errors (bridge returned zero batches, etc.)
    if err_lower.contains("zero batches") || err_lower.contains("protocol error") {
        return ErrorCategory::ProtocolError;
    }

    // Disk I/O errors - check BEFORE validation since some validation errors might mention "file"
    // Include parquet/arrow write errors which are typically disk or serialization issues
    if err_lower.contains("io error")
        || err_lower.contains("disk")
        || err_lower.contains("parquet")
        || err_lower.contains("arrow")
        || err_lower.contains("write error")
        || err_lower.contains("failed to write")
        || (err_lower.contains("file") && !err_lower.contains("validation"))
    {
        return ErrorCategory::DiskIo;
    }

    // Validation errors
    if err_lower.contains("validation") || err_lower.contains("invalid") {
        return ErrorCategory::Validation;
    }

    // Unknown - catch-all for unrecognized errors
    ErrorCategory::Unknown
}

/// Type of data being synced when error occurred
///
/// This is a simple string wrapper - no assumptions about specific table names.
/// Use whatever table names your bridge exposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType(String);

impl DataType {
    /// Create a new DataType from a string
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unknown type (for errors where type cannot be determined)
    pub fn unknown() -> Self {
        Self("unknown".to_string())
    }
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Category of sync error for metrics and monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Connection or networking issues
    Connection,
    /// Request timeout
    Timeout,
    /// Bridge returned no data for a range that should have data
    NoData,
    /// Worker stuck making no progress
    StuckWorker,
    /// Data validation failure
    Validation,
    /// Disk I/O error
    DiskIo,
    /// Operation was cancelled
    Cancelled,
    /// Bridge protocol error (e.g., zero batches returned)
    ProtocolError,
    /// Other/uncategorized error
    Unknown,
}

impl ErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Connection => "connection",
            ErrorCategory::Timeout => "timeout",
            ErrorCategory::NoData => "no_data",
            ErrorCategory::StuckWorker => "stuck_worker",
            ErrorCategory::Validation => "validation",
            ErrorCategory::DiskIo => "disk_io",
            ErrorCategory::Cancelled => "cancelled",
            ErrorCategory::ProtocolError => "protocol_error",
            ErrorCategory::Unknown => "unknown",
        }
    }

    /// Returns true if this error category is transient and should be retried
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            ErrorCategory::Connection | ErrorCategory::Timeout | ErrorCategory::Cancelled
        )
    }

    /// Returns true if this error category is permanent and should not be retried
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            ErrorCategory::Validation | ErrorCategory::StuckWorker | ErrorCategory::NoData
        )
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Structured sync error that preserves error context while providing categorization
#[derive(Debug)]
pub struct SyncError {
    /// What type of data was being synced
    pub data_type: DataType,
    /// Category of error for metrics
    pub category: ErrorCategory,
    /// Block range being synced when error occurred
    pub from_block: u64,
    pub to_block: u64,
    /// Human-readable error message
    pub message: String,
    /// The underlying error source, if any
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Error when multiple data types fail during parallel sync
#[derive(Debug)]
pub struct MultipleDataTypeErrors {
    pub from_block: u64,
    pub to_block: u64,
    pub errors: Vec<(DataType, SyncError)>,
}

impl fmt::Display for MultipleDataTypeErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Multiple data types failed syncing blocks {}-{}: ",
            self.from_block, self.to_block
        )?;
        for (i, (data_type, err)) in self.errors.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{data_type}: {err}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MultipleDataTypeErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Return first error as source
        self.errors.first().and_then(|(_, e)| e.source())
    }
}

impl From<MultipleDataTypeErrors> for SyncError {
    fn from(multi_err: MultipleDataTypeErrors) -> Self {
        // Aggregate into a single SyncError with Unknown data type
        let message = format!("{multi_err}");
        let category = multi_err
            .errors
            .first()
            .map(|(_, e)| e.category)
            .unwrap_or(ErrorCategory::Unknown);

        SyncError {
            data_type: DataType::unknown(),
            category,
            from_block: multi_err.from_block,
            to_block: multi_err.to_block,
            message,
            source: Some(Box::new(multi_err)),
        }
    }
}

impl SyncError {
    pub fn new(
        data_type: DataType,
        category: ErrorCategory,
        from_block: u64,
        to_block: u64,
        message: String,
    ) -> Self {
        Self {
            data_type,
            category,
            from_block,
            to_block,
            message,
            source: None,
        }
    }

    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    /// Create a protocol error (bridge returned zero batches)
    pub fn protocol_error(data_type: DataType, from_block: u64, to_block: u64) -> Self {
        let msg = format!(
            "Bridge returned zero batches for {data_type} {from_block}-{to_block}. This indicates a protocol error."
        );
        Self {
            data_type,
            category: ErrorCategory::ProtocolError,
            from_block,
            to_block,
            message: msg,
            source: None,
        }
    }

    /// Create a validation error (unexpected block range)
    pub fn validation_error(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        message: String,
    ) -> Self {
        Self {
            data_type,
            category: ErrorCategory::Validation,
            from_block,
            to_block,
            message,
            source: None,
        }
    }

    /// Create a disk I/O error
    pub fn disk_io_error(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        message: String,
    ) -> Self {
        Self {
            data_type,
            category: ErrorCategory::DiskIo,
            from_block,
            to_block,
            message,
            source: None,
        }
    }

    /// Create a stuck worker error
    pub fn stuck_worker(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        message: String,
    ) -> Self {
        Self {
            data_type,
            category: ErrorCategory::StuckWorker,
            from_block,
            to_block,
            message,
            source: None,
        }
    }

    /// Wrap an arbitrary error with context
    pub fn from_error<E>(data_type: DataType, from_block: u64, to_block: u64, error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let message = error.to_string();
        let err_lower = message.to_lowercase();

        // Categorize based on error message
        let category = categorize_error_message(&err_lower);

        Self {
            data_type,
            category,
            from_block,
            to_block,
            message,
            source: Some(Box::new(error)),
        }
    }

    /// Wrap an arbitrary error with custom context message
    pub fn from_error_with_context<E>(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        context: impl Into<String>,
        error: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let err_str = error.to_string();
        let err_lower = err_str.to_lowercase();

        // Categorize based on error message
        let category = categorize_error_message(&err_lower);

        let context_str = context.into();
        let message = format!("{context_str}: {err_str}");

        Self {
            data_type,
            category,
            from_block,
            to_block,
            message,
            source: Some(Box::new(error)),
        }
    }

    /// Create from a string message (used when the original error is not available)
    pub fn from_message(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        message: impl Into<String>,
    ) -> Self {
        let message = message.into();
        let err_lower = message.to_lowercase();
        let category = categorize_error_message(&err_lower);

        Self {
            data_type,
            category,
            from_block,
            to_block,
            message,
            source: None,
        }
    }

    /// Wrap an anyhow error with custom context message
    pub fn from_anyhow_with_context(
        data_type: DataType,
        from_block: u64,
        to_block: u64,
        context: impl Into<String>,
        error: anyhow::Error,
    ) -> Self {
        let err_str = error.to_string();
        let err_lower = err_str.to_lowercase();
        let category = categorize_error_message(&err_lower);
        let context_str = context.into();
        let message = format!("{context_str}: {err_str}");

        Self {
            data_type,
            category,
            from_block,
            to_block,
            message,
            source: None, // anyhow doesn't expose its source easily
        }
    }

    /// Check if this error is transient and should trigger a retry
    pub fn is_transient(&self) -> bool {
        self.category.is_transient() || self.message.contains("Timeout expired")
    }

    /// Check if this error is permanent and should not be retried
    pub fn is_permanent(&self) -> bool {
        self.category.is_permanent()
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}/{}] blocks {}-{}: {}",
            self.category, self.data_type, self.from_block, self.to_block, self.message
        )
    }
}

impl std::error::Error for SyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

// Convert from anyhow::Error for backwards compatibility during migration
impl From<anyhow::Error> for SyncError {
    fn from(err: anyhow::Error) -> Self {
        let message = err.to_string();
        let err_lower = message.to_lowercase();

        // Detect data type from error message
        let data_type = if err_lower.contains("blocks") {
            DataType::new("blocks")
        } else if err_lower.contains("transactions") {
            DataType::new("transactions")
        } else if err_lower.contains("logs") {
            DataType::new("logs")
        } else {
            DataType::unknown()
        };

        // Categorize error
        let category = categorize_error_message(&err_lower);

        Self {
            data_type,
            category,
            from_block: 0,
            to_block: 0,
            message,
            source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_categorize_connection_errors() {
        assert_eq!(
            categorize_error_message("connection refused"),
            ErrorCategory::Connection
        );
        assert_eq!(
            categorize_error_message("failed to connect to server"),
            ErrorCategory::Connection
        );
    }

    #[test]
    fn test_categorize_timeout_errors() {
        assert_eq!(
            categorize_error_message("request timeout"),
            ErrorCategory::Timeout
        );
        assert_eq!(
            categorize_error_message("operation timed out"),
            ErrorCategory::Timeout
        );
    }

    #[test]
    fn test_categorize_cancelled_errors() {
        assert_eq!(
            categorize_error_message("operation cancelled"),
            ErrorCategory::Cancelled
        );
        assert_eq!(
            categorize_error_message("request was canceled"),
            ErrorCategory::Cancelled
        );
    }

    #[test]
    fn test_categorize_nodata_errors() {
        assert_eq!(
            categorize_error_message("header not found"),
            ErrorCategory::NoData
        );
        assert_eq!(
            categorize_error_message("block not found"),
            ErrorCategory::NoData
        );
    }

    #[test]
    fn test_categorize_stuck_worker() {
        assert_eq!(
            categorize_error_message("worker failed to make progress"),
            ErrorCategory::StuckWorker
        );
    }

    #[test]
    fn test_categorize_protocol_errors() {
        assert_eq!(
            categorize_error_message("bridge returned zero batches"),
            ErrorCategory::ProtocolError
        );
    }

    #[test]
    fn test_categorize_disk_io_errors() {
        assert_eq!(
            categorize_error_message("io error: disk full"),
            ErrorCategory::DiskIo
        );
        assert_eq!(
            categorize_error_message("parquet write failed"),
            ErrorCategory::DiskIo
        );
        assert_eq!(
            categorize_error_message("arrow serialization error"),
            ErrorCategory::DiskIo
        );
    }

    #[test]
    fn test_categorize_validation_errors() {
        assert_eq!(
            categorize_error_message("validation failed"),
            ErrorCategory::Validation
        );
        assert_eq!(
            categorize_error_message("invalid block range"),
            ErrorCategory::Validation
        );
    }

    #[test]
    fn test_transient_check() {
        assert!(ErrorCategory::Connection.is_transient());
        assert!(ErrorCategory::Timeout.is_transient());
        assert!(ErrorCategory::Cancelled.is_transient());
        assert!(!ErrorCategory::Validation.is_transient());
        assert!(!ErrorCategory::StuckWorker.is_transient());
    }

    #[test]
    fn test_permanent_check() {
        assert!(ErrorCategory::Validation.is_permanent());
        assert!(ErrorCategory::StuckWorker.is_permanent());
        assert!(ErrorCategory::NoData.is_permanent());
        assert!(!ErrorCategory::Connection.is_permanent());
        assert!(!ErrorCategory::Timeout.is_permanent());
    }

    #[test]
    fn test_sync_error_display() {
        let err = SyncError::new(
            DataType::new("blocks"),
            ErrorCategory::Connection,
            0,
            1000,
            "connection refused".to_string(),
        );
        let display = format!("{err}");
        assert!(display.contains("connection"));
        assert!(display.contains("blocks"));
        assert!(display.contains("0-1000"));
    }

    #[test]
    fn test_data_type_from_str() {
        let dt: DataType = "my_custom_table".into();
        assert_eq!(dt.as_str(), "my_custom_table");
    }
}
