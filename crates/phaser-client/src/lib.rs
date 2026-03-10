//! Flight client for connecting to phaser bridges
//!
//! This crate provides a minimal client for connecting to bridges without
//! pulling in server dependencies. Use this in consumers like flight-streamer.
//!
//! # Example
//!
//! ```ignore
//! use phaser_client::{FlightBridgeClient, BridgeError, GenericQuery};
//!
//! async fn example() -> Result<(), BridgeError> {
//!     let mut client = FlightBridgeClient::connect("http://localhost:50051".into()).await?;
//!
//!     // Discover available tables
//!     let capabilities = client.discover().await?;
//!     println!("Bridge: {} with {} tables", capabilities.name, capabilities.tables.len());
//!
//!     // Query data
//!     let query = GenericQuery::historical("blocks", 0, 100);
//!     let stream = client.query(query).await?;
//!
//!     Ok(())
//! }
//! ```

mod client;
mod error;

pub use client::FlightBridgeClient;
pub use error::BridgeError;

// Re-export core types for convenience
pub use phaser_types::{
    // Subscription
    BackpressureStrategy,
    // Batch metadata
    BatchMetadata,
    BatchWithRange,
    BlockRange,
    // Descriptors
    BlockchainDescriptor,
    BridgeInfo,
    Compression,
    ControlAction,
    DataAvailability,
    DataSource,
    // Discovery
    DiscoveryCapabilities,
    FilterDescriptor,
    FilterSpec,
    GenericQuery,
    GenericQueryMode,
    QueryMode,
    ResponsibilityRange,
    StreamPreferences,
    StreamType,
    SubscriptionInfo,
    SubscriptionOptions,
    TableDescriptor,
    ValidationStage,
    ACTION_DESCRIBE,
};
