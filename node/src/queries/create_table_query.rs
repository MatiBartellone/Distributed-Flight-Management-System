use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::tokens::data_type::DataType;
use crate::queries::query::Query;
use crate::utils::constants::KEYSPACE_METADATA;
use crate::utils::errors::Errors;
use crate::utils::functions::{check_table_name, get_long_string_from_str};
use std::any::Any;
use std::collections::HashMap;

#[derive(PartialEq, Debug)]
pub struct CreateTableQuery {
    pub table_name: String,
    pub columns: HashMap<String, DataType>,
    pub primary_key: Vec<String>,
}

impl CreateTableQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            columns: HashMap::new(),
            primary_key: Vec::new(),
        }
    }

    fn split_keyspace_table(input: &str) -> Result<(&str, &str), Errors> {
        let mut parts = input.split('.');
        let keyspace = parts.next().ok_or_else(|| Errors::SyntaxError("Missing keyspace".to_string()))?;
        let table = parts.next().ok_or_else(|| Errors::SyntaxError("Missing table".to_string()))?;
        if parts.next().is_some() {
            return Err(Errors::SyntaxError("Too many parts, expected only keyspace and table".to_string()));
        }
        Ok((keyspace, table))
    }
}

impl Default for CreateTableQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for CreateTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let (kesypace_name, table) = Self::split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.add_table(KEYSPACE_METADATA.to_owned(), kesypace_name, table, self.primary_key.clone(), self.columns.clone())?;
        Ok(get_long_string_from_str("Create table was successful"))
    }

    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
