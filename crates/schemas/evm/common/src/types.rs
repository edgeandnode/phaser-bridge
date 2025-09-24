use alloy_primitives::{Address, FixedBytes, U256};
use serde::{Deserialize, Serialize};
use typed_arrow::Record;

/// 32-byte hash used in EVM chains
#[derive(Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hash32 {
    pub bytes: [u8; 32],
}

impl From<FixedBytes<32>> for Hash32 {
    fn from(b: FixedBytes<32>) -> Self {
        Hash32 { bytes: b.0 }
    }
}

impl AsRef<[u8]> for Hash32 {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// 20-byte address used in EVM chains
#[derive(Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Address20 {
    pub bytes: [u8; 20],
}

impl From<Address> for Address20 {
    fn from(a: Address) -> Self {
        Address20 { bytes: a.0 .0 }
    }
}

impl AsRef<[u8]> for Address20 {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Wei value represented as a byte array for Decimal256
#[derive(Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wei {
    pub bytes: [u8; 32],
}

impl From<U256> for Wei {
    fn from(u: U256) -> Self {
        Wei {
            bytes: u.to_le_bytes(),
        }
    }
}

impl From<u64> for Wei {
    fn from(value: u64) -> Self {
        let u256 = U256::from(value);
        Wei {
            bytes: u256.to_le_bytes(),
        }
    }
}

impl From<u128> for Wei {
    fn from(value: u128) -> Self {
        let u256 = U256::from(value);
        Wei {
            bytes: u256.to_le_bytes(),
        }
    }
}
