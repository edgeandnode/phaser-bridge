/// Generated protobuf code for Erigon gRPC interfaces

// Include generated code directly
pub mod types {
    include!("generated/types.rs");
}

pub mod remote {
    include!("generated/remote.rs");
}

// Re-export commonly used types
pub use remote::ethbackend_client::EthbackendClient;
pub use remote::{BlockReply, BlockRequest, Event, SubscribeReply, SubscribeRequest};
pub use remote::{LogsFilterRequest, SubscribeLogsReply};
