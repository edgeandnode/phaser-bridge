/// Generated protobuf code for Erigon gRPC interfaces

// Include the generated code directly from the src/generated directory
pub mod types {
    include!("generated/types.rs");
}

pub mod remote {
    include!("generated/remote.rs");
}

// Re-export commonly used types for convenience
pub use remote::ethbackend_client::EthbackendClient;
pub use remote::{Event, SubscribeRequest, SubscribeReply, BlockRequest, BlockReply};
pub use remote::{LogsFilterRequest, SubscribeLogsReply};