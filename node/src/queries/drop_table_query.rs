use crate::data_access::data_access_handler::use_data_access;
use crate::meta_data::meta_data_handler::use_keyspace_meta_data;
use crate::utils::constants::KEYSPACE_METADATA_PATH;
use crate::utils::functions::{check_table_name, split_keyspace_table};
use crate::utils::response::Response;
use crate::{queries::query::Query, utils::errors::Errors};
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct DropTableQuery {
    pub table_name: String,
    pub if_exist: Option<bool>,
}

impl DropTableQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            if_exist: None,
        }
    }

    fn push_on_meta_data(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        use_keyspace_meta_data(|handler| {
            handler.delete_table(KEYSPACE_METADATA_PATH.to_owned(), keyspace_name, table)
        })
    }

    fn push_on_data_acces(&self) -> Result<(), Errors> {
        use_data_access(|data_access| data_access.drop_table(self.table_name.to_string()))
    }
}

impl Query for DropTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        self.push_on_data_acces()?;
        self.push_on_meta_data()?;
        Response::schema_change("DROPPED", "TABLE", &self.table_name)
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
    }

    fn get_keyspace(&self) -> Result<String, Errors> {
        let (kp, _) = split_keyspace_table(&self.table_name)?;
        Ok(kp.to_string())
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Default for DropTableQuery {
    fn default() -> Self {
        Self::new()
    }
}
