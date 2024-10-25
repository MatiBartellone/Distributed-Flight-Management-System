use super::query::Query;
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::utils::constants::KEYSPACE_METADATA;
use crate::utils::functions::{check_table_name, get_long_string_from_str, split_keyspace_table};
use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use std::any::Any;

#[derive(PartialEq, Debug)]
pub struct AlterTableQuery {
    pub table_name: String,
    pub operation: Option<Operations>,
    pub first_column: String,
    pub second_column: String,
    pub data: DataType,
}
#[derive(Debug, PartialEq)]
pub enum Operations {
    ADD,
    ALTER,
    RENAME,
    DROP,
    WITH,
}

impl Default for AlterTableQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl AlterTableQuery {
    pub fn new() -> AlterTableQuery {
        AlterTableQuery {
            table_name: String::new(),
            operation: None,
            first_column: String::new(),
            second_column: String::new(),
            data: DataType::Int,
        }
    }

    fn add(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.new_column(KEYSPACE_METADATA.to_owned(), keyspace_name, table, &self.first_column, self.data.clone())?;
        Ok(())
    }

    fn drop(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.drop_column(KEYSPACE_METADATA.to_owned(), keyspace_name, table, &self.first_column)?;
        Ok(())
    }

    fn rename(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        let mut stream = MetaDataHandler::establish_connection()?;
        let meta_data_handler = MetaDataHandler::get_instance(&mut stream)?;
        let keyspace_meta_data = meta_data_handler.get_keyspace_meta_data_access();
        keyspace_meta_data.rename_column(KEYSPACE_METADATA.to_owned(), keyspace_name, table, &self.first_column, &self.second_column)?;
        Ok(())
    }

    
}

impl Query for AlterTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        if let Some(operation) = &self.operation{
            match operation {
            Operations::ADD => self.add()?,
            Operations::DROP =>self.drop()?,
            Operations::RENAME => self.rename()?, //Faltaria agregarle logica para el dataAcces
            _=> {return Err(Errors::SyntaxError("Invalid Operation to Alter Table".to_string()));}
        }
        } else {
            return Err(Errors::SyntaxError("Invalid Operation to Alter Table".to_string()));
        }
        
        Ok(get_long_string_from_str("Alter table was successful"))
    }

    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors> {
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
