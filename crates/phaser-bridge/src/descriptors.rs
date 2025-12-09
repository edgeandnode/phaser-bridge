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

/// Compression options for data transfer (maps to gRPC compression)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Compression {
    /// No compression
    #[default]
    None,
    /// Gzip compression (fast, good compression ratio)
    Gzip,
    /// ZSTD compression (better compression ratio, slightly slower)
    Zstd,
}

/// Stream preferences for negotiating transfer settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPreferences {
    /// Maximum message size in bytes (default: 4MB)
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,

    /// Compression method (default: None)
    #[serde(default)]
    pub compression: Compression,

    /// Hint for batch size in blocks (default: 100)
    /// Bridge may adjust based on max_message_bytes
    #[serde(default = "default_batch_size_hint")]
    pub batch_size_hint: u32,
}

fn default_max_message_bytes() -> usize {
    32 * 1024 * 1024 // 32MB - handles large transaction batches
}

fn default_batch_size_hint() -> u32 {
    100
}

impl Default for StreamPreferences {
    fn default() -> Self {
        Self {
            max_message_bytes: default_max_message_bytes(),
            compression: Compression::None,
            batch_size_hint: default_batch_size_hint(),
        }
    }
}

/// Validation stages for blockchain data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum ValidationStage {
    /// No validation
    #[default]
    None,
    /// Validate RLP at ingestion (node → bridge)
    Ingestion,
    /// Validate records after conversion (conversion → storage)
    Conversion,
    /// Validate at both stages
    Both,
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
    #[serde(default)]
    pub validation: ValidationStage,
    /// Stream preferences for transfer settings
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferences: Option<StreamPreferences>,
    /// Enable transaction traces (callTracer) for Logs stream (default: false, 2-5x slower)
    #[serde(default)]
    pub enable_traces: bool,
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
            validation: ValidationStage::None,
            preferences: None,
            enable_traces: false,
        }
    }

    /// Create a descriptor for live subscription (starts from current head)
    pub fn live(stream_type: StreamType) -> Self {
        Self {
            stream_type,
            chain_id: None,
            query_mode: QueryMode::Live,
            subscription_options: None,
            include_reorgs: false,
            validation: ValidationStage::None,
            preferences: None,
            enable_traces: false,
        }
    }

    /// Set validation stage for this descriptor
    pub fn with_validation(mut self, stage: ValidationStage) -> Self {
        self.validation = stage;
        self
    }

    /// Enable ingestion validation (validates RLP from node)
    pub fn with_ingestion_validation(mut self) -> Self {
        self.validation = ValidationStage::Ingestion;
        self
    }

    /// Enable conversion validation (validates records after conversion)
    pub fn with_conversion_validation(mut self) -> Self {
        self.validation = ValidationStage::Conversion;
        self
    }

    /// Enable both ingestion and conversion validation
    pub fn with_full_validation(mut self) -> Self {
        self.validation = ValidationStage::Both;
        self
    }

    /// Set stream preferences for this descriptor
    pub fn with_preferences(mut self, preferences: StreamPreferences) -> Self {
        self.preferences = Some(preferences);
        self
    }

    /// Enable transaction traces (callTracer) for Logs stream
    pub fn with_traces(mut self, enable: bool) -> Self {
        self.enable_traces = enable;
        self
    }

    /// Get stream preferences or return default
    pub fn get_preferences(&self) -> StreamPreferences {
        self.preferences.clone().unwrap_or_default()
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
