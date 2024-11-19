use crate::utils::response::Response;
use crate::{
    data_access::data_access_handler::DataAccessHandler,
    meta_data::meta_data_handler::MetaDataHandler,
    queries::query::Query,
    utils::{constants::KEYSPACE_METADATA, errors::Errors},
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
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        let tables = keyspace_meta_data
            .get_tables_from_keyspace(KEYSPACE_METADATA.to_owned(), &self.keyspace)?;
        keyspace_meta_data.drop_keyspace(KEYSPACE_METADATA.to_owned(), &self.keyspace)?;
        Ok(tables)
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
        Response::schema_change("DROPPED", "KEYSPACE", &self.keyspace)
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
