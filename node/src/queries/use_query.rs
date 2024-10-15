use std::any::Any;
use serde::{Deserialize, Serialize};
use crate::utils::errors::Errors;
use super::query::Query;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct UseQuery{
    pub keyspace_name: String,
}

impl UseQuery{
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
    fn run(&self) -> Result<String, Errors> {
        Ok("response_from_second_node".to_string())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}