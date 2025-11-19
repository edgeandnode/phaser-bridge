use thiserror::Error;
use tonic::Status;
use tracing::error;

/// Errors that can occur in the Erigon bridge
#[derive(Error, Debug)]
pub enum ErigonBridgeError {
    #[error("Failed to serialize descriptor: {0}")]
    DescriptorSerialization(#[from] serde_json::Error),

    #[error("Invalid byte array conversion: {0}")]
    ByteConversion(String),

    #[error("Schema creation failed: {0}")]
    SchemaCreation(#[from] arrow::error::ArrowError),

    #[error("Erigon gRPC client error: {0}")]
    ErigonClient(Box<tonic::Status>),

    #[error("RLP decoding error: {0}")]
    RlpDecoding(#[from] alloy_rlp::Error),

    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Conversion error: {0}")]
    ConversionError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Stream protocol error: {0}")]
    StreamProtocol(#[from] phaser_bridge::StreamError),
}

/// Implements conversion from tonic::Status to ErigonBridgeError
impl From<Status> for ErigonBridgeError {
    fn from(status: Status) -> Self {
        ErigonBridgeError::ErigonClient(Box::new(status))
    }
}

/// Implements conversion from our error to a gRPC Status for proper error reporting
impl From<ErigonBridgeError> for Status {
    fn from(err: ErigonBridgeError) -> Self {
        error!("Erigon bridge error: {}", err);
        match err {
            ErigonBridgeError::DescriptorSerialization(_) => {
                Status::invalid_argument("Invalid request descriptor")
            }
            ErigonBridgeError::ByteConversion(_) => Status::internal("Data conversion error"),
            ErigonBridgeError::SchemaCreation(_) => Status::internal("Schema processing error"),
            ErigonBridgeError::ErigonClient(status) => {
                Status::unavailable(format!("Erigon unavailable: {}", status.message()))
            }
            ErigonBridgeError::RlpDecoding(_) => Status::internal("Data decoding error"),
            ErigonBridgeError::Transport(_) => Status::unavailable("Transport connection error"),
            ErigonBridgeError::InvalidUri(_) => Status::invalid_argument("Invalid connection URI"),
            ErigonBridgeError::Internal(_) => Status::internal("Internal bridge error"),
            ErigonBridgeError::InvalidData(msg) => Status::invalid_argument(msg),
            ErigonBridgeError::ConversionError(msg) => Status::internal(msg),
            ErigonBridgeError::ValidationError(msg) => Status::failed_precondition(msg),
            ErigonBridgeError::ConnectionFailed(msg) => {
                Status::unavailable(format!("Connection failed: {msg}"))
            }
            ErigonBridgeError::StreamProtocol(stream_err) => {
                Status::aborted(format!("Stream protocol violation: {stream_err}"))
            }
        }
    }
}
