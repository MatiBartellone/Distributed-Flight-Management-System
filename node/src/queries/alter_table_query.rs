use super::query::Query;
use crate::utils::functions::check_table_name;
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
}

impl Query for AlterTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        todo!()
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
