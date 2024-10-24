use crate::parsers::tokens::literal::Literal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::option::Option;

pub const EQUAL: i8 = 0;
pub const GREATER: i8 = 1;
pub const LOWER: i8 = -1;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Row {
    pub columns: Vec<Column>,
    pub primary_keys: Vec<String>,
    deleted: Option<bool>,
}

impl Row {
    pub fn new(columns: Vec<Column>, primary_keys: Vec<String>) -> Self {
        Self {
            columns,
            primary_keys,
            deleted: None,
        }
    }

    pub fn new_deleted_row() -> Self {
        Self {
            columns: Vec::new(),
            primary_keys: Vec::new(),
            deleted: Some(true),
        }
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
        let column_opt1 = Self::get_column(row1, column_name);
        let column_opt2 = Self::get_column(row2, column_name);

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

    fn get_column<'a>(row: &'a Row, column_name: &String) -> Option<&'a Column> {
        let mut column: Option<&Column> = None;
        for col in &row.columns {
            if &col.column_name == column_name {
                column = Some(col);
            }
        }
        column
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
}
