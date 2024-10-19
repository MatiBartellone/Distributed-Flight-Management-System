use std::collections::HashMap;

use crate::utils::errors::Errors;

use super::query::Query;

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
    fn run(&self) -> Result<(), Errors> {
        todo!()
    }
}
