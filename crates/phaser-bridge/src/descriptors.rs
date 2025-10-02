use crate::subscription::{QueryMode, SubscriptionOptions};
use anyhow;
use arrow_flight::{FlightDescriptor, Ticket};
use serde::{Deserialize, Serialize};

/// Types of blockchain data streams
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamType {
    Blocks,
    Transactions,
    Logs,
    Trie, // Raw trie nodes for state reconstruction
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Blocks => write!(f, "blocks"),
            StreamType::Transactions => write!(f, "transactions"),
            StreamType::Logs => write!(f, "logs"),
            StreamType::Trie => write!(f, "trie"),
        }
    }
}

/// Enhanced descriptor for blockchain data requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainDescriptor {
    pub stream_type: StreamType,
    pub chain_id: Option<u64>,
    pub query_mode: QueryMode,
    pub subscription_options: Option<SubscriptionOptions>,
    pub include_reorgs: bool,
}

impl BlockchainDescriptor {
    /// Create a descriptor for historical query
    pub fn historical(stream_type: StreamType, start: u64, end: u64) -> Self {
        Self {
            stream_type,
            chain_id: None,
            query_mode: QueryMode::Historical { start, end },
            subscription_options: None,
            include_reorgs: false,
        }
    }

    /// Create a descriptor for live subscription
    pub fn live(stream_type: StreamType, from_block: Option<u64>) -> Self {
        Self {
            stream_type,
            chain_id: None,
            query_mode: QueryMode::Live {
                from_block,
                buffer_size: 100,
            },
            subscription_options: None,
            include_reorgs: false,
        }
    }

    /// Create a hybrid descriptor (historical then live)
    pub fn hybrid(stream_type: StreamType, historical_start: u64) -> Self {
        Self {
            stream_type,
            chain_id: None,
            query_mode: QueryMode::Hybrid {
                historical_start,
                then_follow: true,
            },
            subscription_options: None,
            include_reorgs: false,
        }
    }

    /// For backward compatibility - create from old-style parameters
    pub fn from_legacy(
        stream_type: StreamType,
        chain_id: Option<u64>,
        start_block: Option<u64>,
        end_block: Option<u64>,
        follow_head: bool,
        include_reorgs: bool,
    ) -> Self {
        let query_mode = match (start_block, end_block, follow_head) {
            (Some(start), Some(end), false) => QueryMode::Historical { start, end },
            (Some(start), None, true) => QueryMode::Hybrid {
                historical_start: start,
                then_follow: true,
            },
            (None, None, true) | (_, _, true) => QueryMode::Live {
                from_block: start_block,
                buffer_size: 100,
            },
            (Some(start), None, false) => QueryMode::Historical {
                start,
                end: u64::MAX,
            },
            _ => QueryMode::Live {
                from_block: None,
                buffer_size: 100,
            },
        };

        Self {
            stream_type,
            chain_id,
            query_mode,
            subscription_options: None,
            include_reorgs,
        }
    }
}

impl BlockchainDescriptor {
    pub fn to_flight_descriptor(&self) -> FlightDescriptor {
        let json = serde_json::to_string(self).unwrap();
        FlightDescriptor::new_path(vec![json])
    }

    pub fn from_flight_descriptor(desc: &FlightDescriptor) -> Result<Self, anyhow::Error> {
        if let Some(path) = desc.path.first() {
            Ok(serde_json::from_str(path)?)
        } else {
            Err(anyhow::anyhow!("No path in descriptor"))
        }
    }

    pub fn to_ticket(&self) -> Ticket {
        let json = serde_json::to_vec(self).unwrap();
        Ticket::new(json)
    }

    pub fn from_ticket(ticket: &Ticket) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(&ticket.ticket)
    }
}

/// Information about a data stream endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointInfo {
    pub location: String,
    pub ticket: Vec<u8>,
}

/// Capabilities advertised by a bridge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeInfo {
    pub name: String,
    pub node_type: String, // "erigon", "reth", "besu", etc.
    pub version: String,
    pub chain_id: u64,
    pub capabilities: Vec<String>,
    pub current_block: u64,
    pub oldest_block: u64,
}
