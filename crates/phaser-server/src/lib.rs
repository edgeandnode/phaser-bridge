//! Flight server implementation for phaser bridges
//!
//! This crate provides the server-side implementation for bridges.
//! Use this in bridge implementations like erigon-bridge.
//!
//! # Example
//!
//! ```ignore
//! use phaser_server::{FlightBridge, FlightBridgeServer, BridgeCapabilities};
//! use std::sync::Arc;
//!
//! struct MyBridge { /* ... */ }
//!
//! #[async_trait::async_trait]
//! impl FlightBridge for MyBridge {
//!     // implement trait methods...
//! }
//!
//! async fn run_server(bridge: MyBridge) {
//!     let server = FlightBridgeServer::new(Arc::new(bridge));
//!     // Use server.into_service() with tonic
//! }
//! ```

mod bridge;
mod error;
mod server;

pub use bridge::{BridgeCapabilities, FlightBridge};
pub use error::StreamError;
pub use server::FlightBridgeServer;

// Re-export core types for convenience
pub use phaser_types::{
    // Subscription
    BackpressureStrategy,
    // Batch metadata
    BatchMetadata,
    BatchWithRange,
    // Descriptors
    BlockchainDescriptor,
    BridgeInfo,
    Compression,
    ControlAction,
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
