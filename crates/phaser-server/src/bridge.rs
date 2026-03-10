//! FlightBridge trait for implementing blockchain data bridges

use arrow_flight::{
    Action, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

use phaser_types::{BridgeInfo, DiscoveryCapabilities, ACTION_DESCRIBE};

/// Capabilities that a bridge can advertise
#[derive(Debug, Clone)]
pub struct BridgeCapabilities {
    pub supports_historical: bool,
    pub supports_streaming: bool,
    pub supports_reorg_notifications: bool,
    pub supports_filters: bool,
    pub supports_validation: bool,
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

    /// Get protocol-agnostic discovery capabilities
    ///
    /// Returns information about available tables, position semantics,
    /// and supported filters. Clients use this to query without
    /// protocol-specific knowledge.
    ///
    /// Default implementation returns an error. Bridges should override
    /// this to enable protocol-agnostic discovery.
    async fn get_discovery_capabilities(&self) -> Result<DiscoveryCapabilities, Status> {
        Err(Status::unimplemented(
            "Discovery not implemented for this bridge",
        ))
    }

    /// Handle a Flight Action
    ///
    /// Built-in actions:
    /// - "describe": Returns DiscoveryCapabilities as JSON
    ///
    /// Bridges can override to add custom actions.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>>,
        Status,
    > {
        use futures::stream;

        let action = request.into_inner();

        match action.r#type.as_str() {
            ACTION_DESCRIBE => {
                let capabilities = self.get_discovery_capabilities().await?;
                let json = serde_json::to_vec(&capabilities).map_err(|e| {
                    Status::internal(format!("Failed to serialize capabilities: {e}"))
                })?;

                let result = arrow_flight::Result { body: json.into() };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            other => Err(Status::unimplemented(format!("Unknown action: {other}"))),
        }
    }

    /// List available actions
    ///
    /// Default implementation lists built-in actions.
    /// Bridges can override to add custom actions.
    async fn list_actions(&self) -> Result<Vec<arrow_flight::ActionType>, Status> {
        Ok(vec![arrow_flight::ActionType {
            r#type: ACTION_DESCRIBE.to_string(),
            description: "Get protocol-agnostic bridge capabilities".to_string(),
        }])
    }
}
