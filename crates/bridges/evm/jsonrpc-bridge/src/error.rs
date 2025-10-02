use thiserror::Error;
use tonic::Status;
use tracing::error;

/// Errors that can occur in the JSON-RPC bridge
#[derive(Error, Debug)]
pub enum JsonRpcBridgeError {
    #[error("Failed to serialize descriptor: {0}")]
    DescriptorSerialization(#[from] serde_json::Error),

    #[error("Schema creation failed: {0}")]
    SchemaCreation(#[from] arrow::error::ArrowError),

    #[error("JSON-RPC client error: {0}")]
    JsonRpcClient(#[from] anyhow::Error),

    #[error("Subscription error: {0}")]
    Subscription(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<JsonRpcBridgeError> for Status {
    fn from(err: JsonRpcBridgeError) -> Self {
        error!("JSON-RPC bridge error: {}", err);
        match err {
            JsonRpcBridgeError::DescriptorSerialization(_) => {
                Status::invalid_argument("Invalid request descriptor")
            }
            JsonRpcBridgeError::SchemaCreation(_) => Status::internal("Schema processing error"),
            JsonRpcBridgeError::JsonRpcClient(_) => {
                Status::unavailable("JSON-RPC node unavailable")
            }
            JsonRpcBridgeError::Subscription(_) => Status::internal("Subscription error"),
            JsonRpcBridgeError::Transport(_) => Status::unavailable("Transport error"),
            JsonRpcBridgeError::Internal(_) => Status::internal("Internal bridge error"),
        }
    }
}
