use super::errors::Errors;

const ONE: i16 = 0x0001;
const QUORUM: i16 = 0x0004;
const ALL: i16 = 0x0005;

use ConsistencyLevel::*;

#[derive(Debug, PartialEq)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All,
}

impl ConsistencyLevel {
    pub fn from_i16(value: i16) -> Result<ConsistencyLevel, Errors> {
        let consistency = match value {
            ONE => One,
            QUORUM => Quorum,
            ALL => All,
            _ => {
                return Err(Errors::ProtocolError(format!(
                    "Unknown consistency level: {}",
                    value
                )))
            }
        };
        Ok(consistency)
    }

    pub fn get_consistency(&self, replication_factor: usize) -> usize {
        match self {
            One => 1,
            Quorum => (replication_factor / 2) + 1,
            All => replication_factor,
        }
    }

    pub fn to_i16(&self) -> i16 {
        match self {
            ConsistencyLevel::One => 0x0001,
            ConsistencyLevel::Quorum => 0x0004,
            ConsistencyLevel::All => 0x0005,
        }
    }
}
