use crate::queries::query::{Query, QueryEnum};
use crate::utils::errors::Errors;
use rmp_serde::{to_vec, from_slice};

pub struct QuerySerializer;

impl QuerySerializer {
    pub fn serialize(query: &QueryEnum) -> Result<Vec<u8>, Errors> {
        let Ok(serialized) = to_vec(query) else {
            return Err(Errors::ServerError(String::from("Failed to serialize query")));
        };
        Ok(serialized)
    }

    pub fn deserialize(serialized: &[u8]) -> Result<QueryEnum, Errors> {
        let Ok(query) = from_slice(serialized) else {
            return Err(Errors::ServerError(String::from("Failed to deserialize query")));
        };
        Ok(query)
    }
}