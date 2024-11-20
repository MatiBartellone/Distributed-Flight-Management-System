use crate::utils::functions::{get_long_string_from_str, use_keyspace_meta_data};
use crate::{
    data_access::data_access_handler::DataAccessHandler,
    queries::query::Query,
    utils::{constants::KEYSPACE_METADATA_PATH, errors::Errors},
};
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct DropKeySpaceQuery {
    pub keyspace: String,
    pub if_exist: Option<bool>,
}

impl DropKeySpaceQuery {
    pub fn new() -> Self {
        Self {
            keyspace: String::new(),
            if_exist: None,
        }
    }

    fn push_on_meta_data(&self) -> Result<Vec<String>, Errors> {
        use_keyspace_meta_data(|keyspace_meta_data| {
            let tables = keyspace_meta_data
                .get_tables_from_keyspace(KEYSPACE_METADATA_PATH.to_owned(), &self.keyspace)?;
            keyspace_meta_data.drop_keyspace(KEYSPACE_METADATA_PATH.to_owned(), &self.keyspace)?;
            Ok(tables)
        })
    }
}

impl Query for DropKeySpaceQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let tables = self.push_on_meta_data()?;
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        for table in tables {
            let table_id = format!("{}.{}", self.keyspace, table);
            data_access.drop_table(table_id)?;
        }
        Ok(get_long_string_from_str("Drop keyspace was successful"))
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

impl Default for DropKeySpaceQuery {
    fn default() -> Self {
        Self::new()
    }
}
