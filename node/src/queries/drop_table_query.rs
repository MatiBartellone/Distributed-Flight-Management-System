use crate::data_access::data_access_handler::DataAccessHandler;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::utils::functions::{check_table_name, split_keyspace_table, get_long_string_from_str};
use crate::{queries::query::Query, utils::errors::Errors};
use crate::utils::constants::KEYSPACE_METADATA;
use std::any::Any;

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

    fn push_on_meta_data(&self) -> Result<(), Errors>{ 
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.delete_table(KEYSPACE_METADATA.to_owned(), keyspace_name, table)?;
        Ok(())
    }

    fn push_on_data_acces(&self) -> Result<(), Errors> {
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        data_access.drop_table(self.table_name.to_string())?;
        Ok(())
    }
}

impl Query for DropTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        self.push_on_data_acces()?;
        self.push_on_meta_data()?;
        Ok(get_long_string_from_str("Drop table was successful"))
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
