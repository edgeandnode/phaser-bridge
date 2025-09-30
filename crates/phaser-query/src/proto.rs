/// Generated protobuf code for Erigon gRPC interfaces and admin services

// Include the generated code directly from the src/generated directory
pub mod types {
    include!("generated/types.rs");
}

pub mod remote {
    include!("generated/remote.rs");
}

pub mod admin {
    include!("generated/phaser.admin.rs");
}

// Re-export commonly used types for convenience
pub use remote::ethbackend_client::EthbackendClient;
pub use remote::{BlockReply, BlockRequest, Event, SubscribeReply, SubscribeRequest};
pub use remote::{LogsFilterRequest, SubscribeLogsReply};
