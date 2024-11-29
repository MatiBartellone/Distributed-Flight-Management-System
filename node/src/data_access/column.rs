use serde::{Deserialize, Serialize};
use crate::parsers::tokens::literal::Literal;
use crate::utils::types::timestamp::Timestamp;

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Column {
    pub column_name: String,
    pub value: Literal,
    pub timestamp: Timestamp,
}

impl Column {
    pub fn new(column_name: &String, value: &Literal) -> Self {
        Self {
            column_name: String::from(column_name),
            value: Literal {
                value: String::from(&value.value),
                data_type: value.data_type.clone(),
            },
            timestamp: Timestamp::new(),
        }
    }

    pub fn new_from_column(column: &Column) -> Self {
        Self {
            column_name: column.column_name.to_string(),
            value: Literal {
                value: column.value.value.to_string(),
                data_type: column.value.data_type.clone(),
            },
            timestamp: Timestamp::new_from_timestamp(&column.timestamp),
        }
    }
}
