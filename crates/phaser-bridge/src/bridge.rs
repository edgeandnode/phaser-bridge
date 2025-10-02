use arrow_array::RecordBatch;
use arrow_flight::{
    Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

use crate::descriptors::BridgeInfo;

/// Capabilities that a bridge can advertise
#[derive(Debug, Clone)]
pub struct BridgeCapabilities {
    pub supports_historical: bool,
    pub supports_streaming: bool,
    pub supports_reorg_notifications: bool,
    pub supports_filters: bool,
    pub max_batch_size: usize,
}

/// Trait for blockchain data bridges to implement
#[async_trait]
pub trait FlightBridge: Send + Sync + 'static {
    /// Get information about this bridge
    async fn get_info(&self) -> Result<BridgeInfo, Status>;

    /// Get capabilities of this bridge
    async fn get_capabilities(&self) -> Result<BridgeCapabilities, Status>;

    /// Handshake for authentication/authorization
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    >;

    /// Get flight information for a descriptor
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get schema for a stream type
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status>;

    /// List available flights
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>>, Status>;

    /// Stream data for a ticket
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>;

    /// Bidirectional streaming for real-time subscriptions
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>;

    /// Check health/readiness of the bridge
    async fn health_check(&self) -> Result<bool, Status>;
}

/// Helper trait for converting node-specific data to Arrow format
#[async_trait]
pub trait DataConverter: Send + Sync {
    /// Convert blocks to Arrow RecordBatch
    async fn blocks_to_batch(&self, blocks: Vec<impl Send>) -> Result<RecordBatch, anyhow::Error>;

    /// Convert transactions to Arrow RecordBatch
    async fn transactions_to_batch(
        &self,
        txs: Vec<impl Send>,
    ) -> Result<RecordBatch, anyhow::Error>;

    /// Convert logs to Arrow RecordBatch
    async fn logs_to_batch(&self, logs: Vec<impl Send>) -> Result<RecordBatch, anyhow::Error>;
}
