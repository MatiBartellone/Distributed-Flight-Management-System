use super::query::Query;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::utils::constants::nodes_meta_data_path;
use crate::utils::errors::Errors;
use crate::utils::functions::{get_long_string_from_str, split_keyspace_table};
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
        let mut stream = MetaDataHandler::establish_connection()?;
        let nodes_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        let msg = format!(
            "respuesta desde {}",
            nodes_meta_data.get_own_ip(nodes_meta_data_path().as_ref())?
        );
        Ok(get_long_string_from_str(msg.as_ref()))
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        let rng: u8 = rand::random();
        Ok(Some(vec![format!("{}", rng)]))
    }

    fn get_keyspace(&self) -> Result<String, Errors> {
        Ok(self.keyspace_name.to_string())
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
