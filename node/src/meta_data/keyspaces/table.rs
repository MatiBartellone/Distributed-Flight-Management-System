use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::parsers::tokens::data_type::DataType;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub primary_key: String,
    pub columns: HashMap<String, DataType>, //Podria ser <Token::Identifier, DataType>
} //Como prefieran

impl Table {
    pub fn new(primary_key: String, columns: HashMap<String, DataType>) -> Self {
        Table {
            primary_key,
            columns,
        }
    }
}
