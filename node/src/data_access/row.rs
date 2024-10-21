use std::collections::HashMap;
use crate::parsers::tokens::literal::Literal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub columns: Vec<Column>,
    pub primary_keys: Vec<String>,
}

impl Row {
    pub fn get_row_hash(&self) -> HashMap<String, Literal> {
        let mut hash: HashMap<String, Literal> = HashMap::new();
        for column in &self.columns {
            let literal = Literal{
                value: String::from(&column.value.value),
                data_type: column.value.data_type.clone(),
            };
            hash.insert(String::from(&column.column_name), literal);
        }
        hash
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Column {
    column_name: String,
    value: Literal,
    time_stamp: String,
}
