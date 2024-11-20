use crate::queries::query::Query;
use crate::utils::constants::{KEYSPACE_METADATA_PATH, REPLICATION, STRATEGY};
use crate::utils::errors::Errors;
use crate::utils::functions::{get_long_string_from_str, use_keyspace_meta_data};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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

    fn get_replication(&self) -> Option<usize> {
        if let Some(replication_str) = self.replication.get(REPLICATION) {
            if let Ok(replication) = replication_str.parse::<usize>() {
                return Some(replication);
            }
        }
        None
    }

    fn get_strategy(&self) -> Option<String> {
        if let Some(strategy) = self.replication.get(STRATEGY) {
            return Some(strategy.to_string());
        }
        None
    }
}

impl Query for CreateKeyspaceQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        use_keyspace_meta_data(|handler| {
            handler.add_keyspace(
                KEYSPACE_METADATA_PATH.to_owned(),
                &self.keyspace,
                self.get_strategy(),
                self.get_replication(),
            )
        })?;
        Ok(get_long_string_from_str("Create keyspace was successful"))
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
    }

    fn get_keyspace(&self) -> Result<String, Errors> {
        Ok(self.keyspace.to_string())
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
