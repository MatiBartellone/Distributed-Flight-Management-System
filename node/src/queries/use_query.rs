use super::query::Query;
use crate::utils::errors::Errors;
use crate::utils::functions::get_long_string_from_str;
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct UseQuery {
    pub keyspace_name: String,
}

impl UseQuery {
    pub fn new() -> Self {
        Self {
            keyspace_name: String::new(),
        }
    }
}

impl Default for UseQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for UseQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        Ok(get_long_string_from_str("respuesta"))
    }

    fn get_primary_key(&self) -> Option<String> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
