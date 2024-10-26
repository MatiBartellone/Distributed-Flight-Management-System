use crate::data_access::data_access_handler::DataAccessHandler;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::tokens::data_type::DataType;
use crate::queries::query::Query;
use crate::utils::constants::KEYSPACE_METADATA;
use crate::utils::errors::Errors;
use crate::utils::functions::{check_table_name, get_long_string_from_str, split_keyspace_table};
use std::any::Any;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::utils::primary_key::PrimaryKey;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableQuery {
    pub table_name: String,
    pub columns: HashMap<String, DataType>,
    pub primary_key: PrimaryKey,
}

impl CreateTableQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            columns: HashMap::new(),
            primary_key: PrimaryKey::new_empty(),
        }
    }


    fn push_on_meta_data(&self) -> Result<(), Errors>{ 
        let (kesypace_name, table) = split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.add_table(KEYSPACE_METADATA.to_owned(), kesypace_name, table, self.primary_key.to_owned(), self.columns.to_owned())?;
        Ok(())
    }

    fn push_on_data_acces(&self) -> Result<(), Errors> {
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        data_access.create_table(&self.table_name)?;
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
        self.push_on_data_acces()?;
        self.push_on_meta_data()?;
        println!("run create_table_query");
        Ok(get_long_string_from_str("Create table was successful"))
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
