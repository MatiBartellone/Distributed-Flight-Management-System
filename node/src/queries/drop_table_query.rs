use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::{queries::query::Query, utils::errors::Errors};
use std::any::Any;
use std::process;

#[derive(PartialEq, Debug)]
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

    pub fn set_table(&mut self) -> Result<(), Errors> {
        if self.table_name.is_empty() {
            return Err(Errors::SyntaxError(String::from("Table is empty")));
        }
        if !self.table_name.contains('.')
            && ClientMetaDataAcces::get_keyspace(process::id().to_string())?.is_none()
        {
            return Err(Errors::SyntaxError(String::from(
                "Keyspace not defined and non keyspace in usage",
            )));
        } else {
            let Some(kp) = ClientMetaDataAcces::get_keyspace(process::id().to_string())? else {
                return Err(Errors::SyntaxError(String::from("Keyspace not in usage")));
            };
            self.table_name = format!("{}.{}", kp, self.table_name);
        }
        Ok(())
    }
}

impl Query for DropTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        unimplemented!()
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
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
