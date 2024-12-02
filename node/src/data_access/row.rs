use crate::data_access::column::Column;
use crate::parsers::tokens::literal::Literal;
use crate::queries::set_logic::assigmente_value::AssignmentValue;
use crate::utils::errors::Errors;
use crate::utils::types::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::option::Option;

pub const EQUAL: i8 = 0;
pub const GREATER: i8 = 1;
pub const LOWER: i8 = -1;

/// Represents a row in a table. columns and primary key are the values of the row.
/// deleted is a tombstone that indicates if deleted.
/// timestamp indicates the last updated time.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Row {
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub deleted: bool,
    timestamp: Timestamp,
}

impl Row {
    pub fn new(columns: Vec<Column>, primary_keys: Vec<String>) -> Self {
        Self {
            columns,
            primary_key: primary_keys,
            deleted: false,
            timestamp: Timestamp::new(),
        }
    }

    pub fn set_deleted(&mut self) {
        self.timestamp = Timestamp::new();
        self.deleted = true;
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// get_row_hash returns the vec of columns in hash format.
    /// The hash is structured <column_name, Literal values>
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

    /// get_row_hash_assignment returns the vec of columns as Simple AssignmentValue in hash format.
    /// The hash is structured <column_name, AssignmentValue>
    pub fn get_row_hash_assigment(&self) -> HashMap<String, AssignmentValue> {
        let mut hash: HashMap<String, AssignmentValue> = HashMap::new();
        for column in &self.columns {
            let literal = Literal {
                value: String::from(&column.value.value),
                data_type: column.value.data_type.clone(),
            };
            hash.insert(
                String::from(&column.column_name),
                AssignmentValue::Simple(literal),
            );
        }
        hash
    }

    /// Compares two rows by column_name.
    /// 0 if EQUAL
    /// 1 if row1 > row2
    /// -1 if row2 > row1
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

    /// get_some_column searches for column_name in the row
    /// If the column was find returns a Column, else Error
    pub fn get_some_column(&self, column_name: &String) -> Result<Column, Errors> {
        let mut column: Option<&Column> = None;
        for col in &self.columns {
            if &col.column_name == column_name {
                column = Some(col);
            }
        }
        let Some(col) = column else {
            return Err(Errors::Invalid(format!("Column {} not found", column_name)));
        };
        Ok(Column::new_from_column(col))
    }

    /// searches for column_name in row
    /// if found, returns the column value in string format.
    pub fn get_value(&self, column_name: &String) -> Result<Option<String>, Errors> {
        let hash = self.get_row_hash();
        let Some(literal) = hash.get(&column_name.to_string()) else {
            return Ok(None);
        };
        Ok(Some(literal.value.to_string()))
    }
}
