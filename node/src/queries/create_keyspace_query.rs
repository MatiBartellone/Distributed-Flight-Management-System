use crate::queries::query::Query;
use crate::utils::errors::Errors;
use std::any::Any;
use std::collections::HashMap;

#[derive(PartialEq, Debug)]
pub struct CreateKeyspaceQuery {
    pub keyspace: String,
    pub replication: HashMap<String, String>,
}

impl Default for CreateKeyspaceQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateKeyspaceQuery {
    pub fn new() -> Self {
        Self {
            keyspace: String::new(),
            replication: HashMap::<String, String>::new(),
        }
    }
}

impl Query for CreateKeyspaceQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        todo!()
    }

    fn get_primary_key(&self) -> Option<String> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
