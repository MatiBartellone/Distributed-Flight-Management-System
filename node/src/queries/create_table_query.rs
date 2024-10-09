use std::collections::HashMap;
use crate::parsers::tokens::token::DataType;


pub struct CreateTableQuery {
    pub table_name: String,
    pub columns: HashMap<String, DataType>,
    pub primary_key: String,
}

impl CreateTableQuery {
    pub fn new() -> Self{
        Self{
            table_name: String::new(),
            columns: HashMap::new(),
            primary_key: String::new(),
        }
    }
}
