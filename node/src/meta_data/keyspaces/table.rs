use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::parsers::tokens::data_type::DataType;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub primary_key: Vec<String>,
    pub columns: HashMap<String, DataType>,
} 

impl Table {
    pub fn new(primary_key: Vec<String>, columns: HashMap<String, DataType>) -> Self {
        Table {
            primary_key,
            columns,
        }
    }
}