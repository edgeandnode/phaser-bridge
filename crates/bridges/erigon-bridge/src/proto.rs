/// Generated protobuf code for Erigon gRPC interfaces

// Generated modules as submodules
#[path = "generated/types.rs"]
pub mod types;

#[path = "generated/remote.rs"]
pub mod remote;

// Re-export commonly used types
pub use remote::ethbackend_client::EthbackendClient;
pub use remote::{Event, SubscribeRequest, SubscribeReply, BlockRequest, BlockReply};
pub use remote::{LogsFilterRequest, SubscribeLogsReply};