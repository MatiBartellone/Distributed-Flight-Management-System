use super::query::Query;
use crate::{meta_data::meta_data_handler::MetaDataHandler, utils::constants::CLIENT_METADATA_PATH};
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
        let mut stream = MetaDataHandler::establish_connection()?;
        let client_meta_data = MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
        client_meta_data.use_keyspace(CLIENT_METADATA_PATH.to_owned(), &self.keyspace_name)?;
        Ok(get_long_string_from_str("Use was successful"))
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
