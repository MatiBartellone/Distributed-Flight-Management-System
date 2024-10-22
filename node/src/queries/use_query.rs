use super::query::Query;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::constants::nodes_meta_data_path;
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
        let msg = format!(
            "respuesta desde {}",
            NodesMetaDataAccess::get_own_ip(nodes_meta_data_path().as_ref())?
        );
        Ok(get_long_string_from_str(msg.as_ref()))
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        let rng: u8 = rand::random();
        Some(vec![format!("{}", rng)])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
