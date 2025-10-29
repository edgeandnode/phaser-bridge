/// Generated protobuf code for Erigon gRPC interfaces
// Include generated code directly
pub mod types {
    include!("generated/types.rs");
}

pub mod remote {
    include!("generated/remote.rs");
}

pub mod custom {
    include!("generated/custom.rs");
}

// Re-export commonly used types
pub use remote::ethbackend_client::EthbackendClient;
pub use remote::{BlockReply, BlockRequest, Event, SubscribeRequest};
