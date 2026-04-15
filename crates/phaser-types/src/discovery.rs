//! Protocol-agnostic bridge discovery types
//!
//! These types allow clients to discover bridge capabilities without
//! hardcoded knowledge of specific protocols (EVM, Canton, etc.).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Action name for the describe endpoint
pub const ACTION_DESCRIBE: &str = "describe";

/// Protocol-agnostic bridge capabilities
///
/// Bridges return this from the "describe" Flight Action.
/// Clients use this to understand what tables are available
/// and how to query them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryCapabilities {
    /// Human-readable bridge name (e.g., "erigon-bridge", "canton-bridge")
    pub name: String,

    /// Bridge version
    pub version: String,

    /// Protocol type for routing (e.g., "evm", "canton", "solana")
    /// Clients may use this to select appropriate schema crates
    pub protocol: String,

    /// Label for the position dimension (e.g., "block_number", "offset")
    /// This is purely descriptive - clients should treat positions as opaque u64
    pub position_label: String,

    /// Current position (e.g., latest block number, current offset)
    pub current_position: u64,

    /// Oldest available position
    pub oldest_position: u64,

    /// Available tables/streams
    pub tables: Vec<TableDescriptor>,

    /// Optional protocol-specific metadata
    /// Clients can inspect this if they know the protocol
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Description of a table available from the bridge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescriptor {
    /// Table name (used in queries)
    pub name: String,

    /// Column name containing the position (e.g., "_block_num", "_offset")
    pub position_column: String,

    /// Columns the data is sorted by (for client optimization hints)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sorted_by: Vec<String>,

    /// Supported query modes for this table
    #[serde(default)]
    pub supported_modes: Vec<String>,

    /// Filters that must be provided
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_filters: Vec<FilterDescriptor>,

    /// Optional filters the bridge can apply
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optional_filters: Vec<FilterDescriptor>,
}

impl TableDescriptor {
    /// Create a simple table descriptor with minimal metadata
    pub fn new(name: impl Into<String>, position_column: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            position_column: position_column.into(),
            sorted_by: Vec::new(),
            supported_modes: vec!["historical".to_string(), "live".to_string()],
            required_filters: Vec::new(),
            optional_filters: Vec::new(),
        }
    }

    /// Add supported modes
    pub fn with_modes(mut self, modes: Vec<&str>) -> Self {
        self.supported_modes = modes.into_iter().map(String::from).collect();
        self
    }

    /// Add sort columns
    pub fn with_sorted_by(mut self, columns: Vec<&str>) -> Self {
        self.sorted_by = columns.into_iter().map(String::from).collect();
        self
    }
}

/// Description of a filter parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterDescriptor {
    /// Filter name (e.g., "party_id", "addresses")
    pub name: String,

    /// Human-readable description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// JSON schema type (e.g., "string", "array", "number")
    #[serde(default = "default_filter_type")]
    pub value_type: String,
}

fn default_filter_type() -> String {
    "string".to_string()
}

fn default_batch_size() -> usize {
    100
}

impl FilterDescriptor {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            value_type: "string".to_string(),
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_type(mut self, t: impl Into<String>) -> Self {
        self.value_type = t.into();
        self
    }
}

/// Protocol-agnostic query format
///
/// Clients use this to request data from any bridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericQuery {
    /// Table/stream name (from TableDescriptor.name)
    pub table: String,

    /// Query mode
    pub mode: GenericQueryMode,

    /// Query batch size in blocks
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Pass-through filters (bridge validates against TableDescriptor)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub filters: HashMap<String, serde_json::Value>,
}

impl GenericQuery {
    /// Create a historical range query
    pub fn historical(table: impl Into<String>, start: u64, end: u64) -> Self {
        Self {
            table: table.into(),
            mode: GenericQueryMode::Range { start, end },
            batch_size: default_batch_size(),
            filters: HashMap::new(),
        }
    }

    /// Create a live subscription query
    pub fn live(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            mode: GenericQueryMode::Live,
            batch_size: default_batch_size(),
            filters: HashMap::new(),
        }
    }

    /// Create a snapshot query at a specific position
    pub fn snapshot(table: impl Into<String>, at: u64) -> Self {
        Self {
            table: table.into(),
            mode: GenericQueryMode::Snapshot { at },
            batch_size: default_batch_size(),
            filters: HashMap::new(),
        }
    }

    /// Set the query batch size in blocks
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Add a filter
    pub fn with_filter(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.filters.insert(key.into(), value);
        self
    }

    /// Convert to Flight Ticket
    pub fn to_ticket(&self) -> arrow_flight::Ticket {
        let json = serde_json::to_vec(self).expect("GenericQuery serialization should not fail");
        arrow_flight::Ticket::new(json)
    }

    /// Parse from Flight Ticket
    pub fn from_ticket(ticket: &arrow_flight::Ticket) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(&ticket.ticket)
    }

    /// Convert to FlightDescriptor for GetSchema
    pub fn to_flight_descriptor(&self) -> arrow_flight::FlightDescriptor {
        let json = serde_json::to_string(self).expect("GenericQuery serialization should not fail");
        arrow_flight::FlightDescriptor::new_path(vec![json])
    }

    /// Parse from FlightDescriptor
    pub fn from_flight_descriptor(
        desc: &arrow_flight::FlightDescriptor,
    ) -> Result<Self, ParseError> {
        if let Some(path) = desc.path.first() {
            Ok(serde_json::from_str(path)?)
        } else {
            Err(ParseError::NoPath)
        }
    }
}

/// Query mode for protocol-agnostic queries
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum GenericQueryMode {
    /// Query a range of positions
    Range {
        /// Start position (inclusive)
        start: u64,
        /// End position (inclusive)
        end: u64,
    },

    /// Subscribe to live data from current head
    Live,

    /// Snapshot at a specific position
    Snapshot {
        /// Position to snapshot at
        at: u64,
    },
}

/// Errors that can occur when parsing descriptors
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("No path in descriptor")]
    NoPath,

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_capabilities_serde() {
        let caps = DiscoveryCapabilities {
            name: "erigon-bridge".to_string(),
            version: "0.1.0".to_string(),
            protocol: "evm".to_string(),
            position_label: "block_number".to_string(),
            current_position: 19_000_000,
            oldest_position: 0,
            tables: vec![
                TableDescriptor::new("blocks", "_block_num"),
                TableDescriptor::new("transactions", "_block_num")
                    .with_sorted_by(vec!["_block_num", "_tx_idx"]),
                TableDescriptor::new("logs", "_block_num").with_sorted_by(vec![
                    "_block_num",
                    "_tx_idx",
                    "_log_idx",
                ]),
            ],
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string_pretty(&caps).unwrap();
        let parsed: DiscoveryCapabilities = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, "erigon-bridge");
        assert_eq!(parsed.tables.len(), 3);
        assert_eq!(parsed.tables[1].sorted_by, vec!["_block_num", "_tx_idx"]);
    }

    #[test]
    fn test_generic_query_serde() {
        let query = GenericQuery::historical("transactions", 1000, 2000)
            .with_filter("addresses", serde_json::json!(["0xabc", "0xdef"]));

        let json = serde_json::to_string(&query).unwrap();
        let parsed: GenericQuery = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.table, "transactions");
        assert!(matches!(
            parsed.mode,
            GenericQueryMode::Range {
                start: 1000,
                end: 2000
            }
        ));
        assert!(parsed.filters.contains_key("addresses"));
    }

    #[test]
    fn test_generic_query_to_ticket() {
        let query = GenericQuery::live("logs");
        let ticket = query.to_ticket();
        let parsed = GenericQuery::from_ticket(&ticket).unwrap();

        assert_eq!(parsed.table, "logs");
        assert!(matches!(parsed.mode, GenericQueryMode::Live));
    }
}
