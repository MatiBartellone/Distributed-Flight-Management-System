use crate::parsers::tokens::data_type::DataType;
use std::collections::HashMap;

use super::query::Query;

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
    fn run(&self) -> Result<(), crate::utils::errors::Errors> {
        todo!()
    }
}