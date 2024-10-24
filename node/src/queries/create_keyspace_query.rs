use crate::queries::query::Query;
use crate::utils::constants::REPLICATION;
use crate::utils::errors::Errors;
use std::any::Any;
use std::collections::HashMap;
use std::intrinsics::mir::Return;

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

    fn get_replication(&self) -> Option<usize> {
        if self.replication.contains_key(REPLICATION) {
            return self.replication.get(REPLICATION);
        }
        return None;
    }

    fn run(&self) -> Result<Vec<u8>, Errors> {
        todo!()
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
