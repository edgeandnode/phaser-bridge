//! Re-exports of sync error types from phaser-client
//!
//! phaser-query uses phaser-client's error types for consistency.
//! All types are re-exported here for backwards compatibility.

pub use phaser_client::sync::{DataType, ErrorCategory, MultipleDataTypeErrors, SyncError};
