use crate::parsers::tokens::data_type::DataType;
use crate::queries::query::Query;
use crate::utils::errors::Errors;
use std::any::Any;
use std::collections::HashMap;

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

    fn get_primary_key(&self) -> Option<String> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
