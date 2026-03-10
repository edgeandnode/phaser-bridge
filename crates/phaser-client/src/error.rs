//! Error types for the bridge client

use thiserror::Error;

/// Errors that can occur when using the bridge client
#[derive(Error, Debug)]
pub enum BridgeError {
    /// Failed to establish connection to the bridge
    #[error("Connection failed: {0}")]
    Connection(#[from] tonic::transport::Error),

    /// gRPC-level error from the bridge
    #[error("gRPC error: {0}")]
    Grpc(#[source] tonic::Status),

    /// Discovery action failed
    #[error("Discovery failed: {message}")]
    Discovery { message: String },

    /// Schema-related error
    #[error("Schema error: {message}")]
    Schema { message: String },

    /// Error during Flight stream processing
    #[error("Stream error: {0}")]
    Stream(#[from] arrow_flight::error::FlightError),

    /// Protocol-level error (malformed messages, missing metadata, etc.)
    #[error("Protocol error: {message}")]
    Protocol { message: String },

    /// JSON serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Platform not supported (e.g., Unix sockets on Windows)
    #[error("Platform not supported: {message}")]
    PlatformNotSupported { message: String },

    /// Invalid URI
    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
}

impl BridgeError {
    /// Create a discovery error
    pub fn discovery(message: impl Into<String>) -> Self {
        Self::Discovery {
            message: message.into(),
        }
    }

    /// Create a schema error
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema {
            message: message.into(),
        }
    }

    /// Create a protocol error
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    /// Create a platform not supported error
    pub fn platform_not_supported(message: impl Into<String>) -> Self {
        Self::PlatformNotSupported {
            message: message.into(),
        }
    }
}

// Manual impl to avoid conflict with #[from] for tonic::Status
// (tonic::Status is not actually used with #[from] to give us more control)
impl From<tonic::Status> for BridgeError {
    fn from(status: tonic::Status) -> Self {
        Self::Grpc(status)
    }
}
