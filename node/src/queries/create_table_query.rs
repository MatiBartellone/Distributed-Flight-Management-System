use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::parsers::tokens::data_type::DataType;
use crate::queries::query::Query;
use crate::utils::errors::Errors;
use std::any::Any;
use std::collections::HashMap;
use std::process;

#[derive(PartialEq, Debug)]
pub struct CreateTableQuery {
    pub table_name: String,
    pub columns: HashMap<String, DataType>,
    pub primary_key: String,
}

impl CreateTableQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            columns: HashMap::new(),
            primary_key: String::new(),
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

impl Default for CreateTableQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for CreateTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        todo!()
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
