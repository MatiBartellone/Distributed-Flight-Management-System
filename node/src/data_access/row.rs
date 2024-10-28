use crate::parsers::tokens::data_type::DataType;
use crate::parsers::tokens::literal::Literal;
use crate::utils::errors::Errors;
use crate::utils::functions::get_timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::option::Option;

pub const EQUAL: i8 = 0;
pub const GREATER: i8 = 1;
pub const LOWER: i8 = -1;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Row {
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    deleted: Option<Column>,
}

impl Row {
    pub fn new(columns: Vec<Column>, primary_keys: Vec<String>) -> Self {
        Self {
            columns,
            primary_key: primary_keys,
            deleted: None,
        }
    }

    pub fn new_deleted_row() -> Result<Self, Errors> {
        Ok(Self {
            columns: Vec::new(),
            primary_key: Vec::new(),
            deleted: Some(Column::new(
                &"deleted".to_string(),
                &Literal::new("true".to_string(), DataType::Boolean),
                get_timestamp()?,
            )),
        })
    }
    pub fn get_row_hash(&self) -> HashMap<String, Literal> {
        let mut hash: HashMap<String, Literal> = HashMap::new();
        for column in &self.columns {
            let literal = Literal {
                value: String::from(&column.value.value),
                data_type: column.value.data_type.clone(),
            };
            hash.insert(String::from(&column.column_name), literal);
        }
        hash
    }

    pub fn cmp(row1: &Row, row2: &Row, column_name: &String) -> i8 {
        let column_opt1 = row1.get_column(column_name);
        let column_opt2 = row2.get_column(column_name);

        match (column_opt1, column_opt2) {
            (Some(col1), Some(col2)) => match col1.value.value.cmp(&col2.value.value) {
                std::cmp::Ordering::Equal => EQUAL,
                std::cmp::Ordering::Greater => GREATER,
                std::cmp::Ordering::Less => LOWER,
            },
            (Some(_), None) => GREATER,
            (None, Some(_)) => LOWER,
            (None, None) => EQUAL,
        }
    }

    fn get_column(&self, column_name: &String) -> Option<Column> {
        let mut column: Option<&Column> = None;
        for col in &self.columns {
            if &col.column_name == column_name {
                column = Some(col);
            }
        }
        if let Some(column) = column {
            return Some(Column::new_from_column(column));
        }
        None
    }
    pub fn get_some_column(&self, column_name: &String) -> Result<Column, Errors> {
        let mut column: Option<&Column> = None;
        for col in &self.columns {
            if &col.column_name == column_name {
                column = Some(col);
            }
        }
        let Some(col) = column else {
            return Err(Errors::ServerError(format!(
                "Column {} not found",
                column_name
            )));
        };
        Ok(Column::new_from_column(col))
    }

    pub fn get_value(&self, column_name: &String) -> Result<Option<String>, Errors> {
        let hash = self.get_row_hash();
        let Some(literal) = hash.get(&column_name.to_string()) else {
            return Ok(None);
        };
        Ok(Some(literal.value.to_string()))
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Column {
    pub(crate) column_name: String,
    pub(crate) value: Literal,
    pub(crate) time_stamp: String,
}

impl Column {
    pub fn new(column_name: &String, value: &Literal, time_stamp: String) -> Self {
        Self {
            column_name: String::from(column_name),
            value: Literal {
                value: String::from(&value.value),
                data_type: value.data_type.clone(),
            },
            time_stamp,
        }
    }

    pub fn new_from_column(column: &Column) -> Self {
        Self {
            column_name: column.column_name.to_string(),
            value: Literal {
                value: column.value.value.to_string(),
                data_type: column.value.data_type.clone(),
            },
            time_stamp: column.time_stamp.to_string(),
        }
    }
}
