use super::query::Query;
use crate::meta_data::meta_data_handler::{use_client_meta_data, use_keyspace_meta_data};
use crate::utils::constants::CLIENT_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::response::Response;
use serde::{Deserialize, Serialize};
use std::any::Any;
use serde::de::Unexpected::Str;

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
        if !use_keyspace_meta_data(|handler| handler.exists_keyspace(&self.keyspace_name))? {
            return Err(Errors::Invalid(String::from("keyspace not found")));
        }
        use_client_meta_data(|handler| {
            handler.use_keyspace(CLIENT_METADATA_PATH.to_owned(), &self.keyspace_name)
        })?;
        Response::set_keyspace(&self.keyspace_name)
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
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
