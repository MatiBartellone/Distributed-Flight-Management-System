use super::query::Query;
use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use std::any::Any;
use std::process;

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

impl Query for AlterTableQuery {
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
