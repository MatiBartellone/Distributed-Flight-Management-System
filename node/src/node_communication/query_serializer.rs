use crate::queries::query::{Query, QueryEnum};
use crate::utils::errors::Errors;
use rmp_serde::{to_vec, from_slice};

pub struct QuerySerializer;

impl QuerySerializer {
    pub fn serialize(query: &Box<dyn Query>) -> Result<Vec<u8>, Errors> {
        let Some(query_enum) = QueryEnum::from_query(query) else {
            return Err(Errors::ServerError(String::from("")))
        };
        let Ok(serialized) = to_vec(&query_enum) else {
            return Err(Errors::ServerError(String::from("Failed to serialize query")));
        };
        Ok(serialized)
    }

    pub fn deserialize(serialized: &[u8]) -> Result<Box<dyn Query>, Errors> {
        let Ok(query_enum) = from_slice(serialized) else {
            return Err(Errors::ServerError(String::from("Failed to deserialize query")));
        };
        Ok(QueryEnum::into_query(query_enum))
    }
}