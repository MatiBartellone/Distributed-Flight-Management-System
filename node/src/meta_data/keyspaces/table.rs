use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::parsers::tokens::data_type::DataType;
use crate::utils::primary_key::PrimaryKey;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub primary_key: PrimaryKey,
    pub columns: HashMap<String, DataType>,
} 

impl Table {
    pub fn new(primary_key: PrimaryKey, columns: HashMap<String, DataType>) -> Self {
        Table {
            primary_key,
            columns,
        }
    }
}