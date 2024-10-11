use super::errors::Errors;

const ONE: i16 = 0x0001;
const QUORUM: i16 = 0x0004;
const ALL: i16 = 0x0005;

#[derive(Debug, PartialEq)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All
}

impl ConsistencyLevel {
    pub fn from_i16(value: i16) -> Result<ConsistencyLevel, Errors> {
        let consistency = match value {
            ONE => ConsistencyLevel::One,
            QUORUM => ConsistencyLevel::Quorum,
            ALL => ConsistencyLevel::All,
            _ => return Err(Errors::ProtocolError(format!("Unknown consistency level: {}", value))),
        };
        Ok(consistency)
    }
}