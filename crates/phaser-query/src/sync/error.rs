use std::fmt;

/// Type of data being synced when error occurred
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Blocks,
    Transactions,
    Logs,
    Unknown,
}

impl DataType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::Blocks => "blocks",
            DataType::Transactions => "transactions",
            DataType::Logs => "logs",
            DataType::Unknown => "unknown",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Category of sync error for metrics and monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            data_type: DataType::Unknown,
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

    /// Wrap an arbitrary error with context
    pub fn from_error<E>(data_type: DataType, from_block: u64, to_block: u64, error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let message = error.to_string();
        let err_lower = message.to_lowercase();

        // Categorize based on error message
        let category = if err_lower.contains("connection") || err_lower.contains("connect") {
            ErrorCategory::Connection
        } else if err_lower.contains("timeout") || err_lower.contains("timed out") {
            ErrorCategory::Timeout
        } else if err_lower.contains("header not found") || err_lower.contains("block not found") {
            // Block doesn't exist - likely beyond chain tip, not a transient error for historical sync
            ErrorCategory::NoData
        } else if err_lower.contains("validation") || err_lower.contains("invalid") {
            ErrorCategory::Validation
        } else if err_lower.contains("io error") || err_lower.contains("file") {
            ErrorCategory::DiskIo
        } else if err_lower.contains("cancelled") {
            ErrorCategory::Cancelled
        } else {
            ErrorCategory::Unknown
        };

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
        let category = if err_lower.contains("connection") || err_lower.contains("connect") {
            ErrorCategory::Connection
        } else if err_lower.contains("timeout") || err_lower.contains("timed out") {
            ErrorCategory::Timeout
        } else if err_lower.contains("header not found") || err_lower.contains("block not found") {
            // Block doesn't exist - likely beyond chain tip, not a transient error for historical sync
            ErrorCategory::NoData
        } else if err_lower.contains("validation") || err_lower.contains("invalid") {
            ErrorCategory::Validation
        } else if err_lower.contains("io error") || err_lower.contains("file") {
            ErrorCategory::DiskIo
        } else if err_lower.contains("cancelled") {
            ErrorCategory::Cancelled
        } else {
            ErrorCategory::Unknown
        };

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

        // Categorize based on error message
        let category = if err_lower.contains("connection") || err_lower.contains("connect") {
            ErrorCategory::Connection
        } else if err_lower.contains("timeout") || err_lower.contains("timed out") {
            ErrorCategory::Timeout
        } else if err_lower.contains("header not found") || err_lower.contains("block not found") {
            // Block doesn't exist - likely beyond chain tip, not a transient error for historical sync
            ErrorCategory::NoData
        } else if err_lower.contains("validation") || err_lower.contains("invalid") {
            ErrorCategory::Validation
        } else if err_lower.contains("io error") || err_lower.contains("file") {
            ErrorCategory::DiskIo
        } else if err_lower.contains("cancelled") {
            ErrorCategory::Cancelled
        } else {
            ErrorCategory::Unknown
        };

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
}

// Convert from anyhow::Error for backwards compatibility during migration
impl From<anyhow::Error> for SyncError {
    fn from(err: anyhow::Error) -> Self {
        let message = err.to_string();
        let err_lower = message.to_lowercase();

        // Detect data type
        let data_type = if err_lower.contains("blocks") {
            DataType::Blocks
        } else if err_lower.contains("transactions") {
            DataType::Transactions
        } else if err_lower.contains("logs") {
            DataType::Logs
        } else {
            DataType::Unknown
        };

        // Categorize error
        let category = if err_lower.contains("connection") || err_lower.contains("connect") {
            ErrorCategory::Connection
        } else if err_lower.contains("timeout") || err_lower.contains("timed out") {
            ErrorCategory::Timeout
        } else if err_lower.contains("no data") || err_lower.contains("empty") {
            ErrorCategory::NoData
        } else if err_lower.contains("failed to make progress") {
            ErrorCategory::StuckWorker
        } else if err_lower.contains("validation") || err_lower.contains("invalid") {
            ErrorCategory::Validation
        } else if err_lower.contains("io error") || err_lower.contains("file") {
            ErrorCategory::DiskIo
        } else if err_lower.contains("cancelled") {
            ErrorCategory::Cancelled
        } else if err_lower.contains("zero batches") || err_lower.contains("protocol error") {
            ErrorCategory::ProtocolError
        } else {
            ErrorCategory::Unknown
        };

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
