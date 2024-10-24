use super::{
    if_clause::IfClause, query::Query, set_logic::assigmente_value::AssignmentValue,
    where_logic::where_clause::WhereClause,
};
use crate::data_access::data_access_handler::DataAccessHandler;
use crate::parsers::tokens::data_type::DataType;
use crate::parsers::tokens::literal::Literal;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    check_table_name, get_columns_from_table, get_long_string_from_str, get_table_pk,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::PartialEq;
use std::collections::HashMap;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct UpdateQuery {
    pub table_name: String,
    pub changes: HashMap<String, AssignmentValue>,
    pub where_clause: Option<WhereClause>,
    pub if_clause: Option<IfClause>,
}

impl UpdateQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            changes: HashMap::new(),
            where_clause: None,
            if_clause: None,
        }
    }

    fn check_values(&self) -> Result<(), Errors> {
        let columns = get_columns_from_table(&self.table_name)?;
        for column in self.changes.keys() {
            if !columns.contains_key(column) {
                return Err(Errors::SyntaxError(format!(
                    "Column {} not defined in table data",
                    column
                )));
            }
        }
        self.check_no_pk_updated()?;
        self.check_assignments(columns)?;
        Ok(())
    }

    fn check_no_pk_updated(&self) -> Result<(), Errors> {
        let table_primary_keys = get_table_pk(&self.table_name)?;
        for column in self.changes.keys() {
            if table_primary_keys.contains(column) {
                return Err(Errors::SyntaxError(String::from("Cannot change primary keys")));
            }
        }
        Ok(())
    }

    fn check_assignments(&self, columns: HashMap<String, DataType>) -> Result<(), Errors> {
        for (set_col, assignment) in self.changes.iter() {
            match assignment {
                AssignmentValue::Column(column) => self.check_column_existance(column, &columns)?,
                AssignmentValue::Simple(literal) => {
                    self.check_data_type_matching(set_col, &columns, literal)?
                }
                AssignmentValue::Arithmetic(column, _, literal) => {
                    self.check_column_existance(column, &columns)?;
                    self.check_data_type_matching(set_col, &columns, literal)?;
                    self.check_data_type_matching(column, &columns, literal)?;
                }
            }
        }
        Ok(())
    }
    fn check_column_existance(
        &self,
        column: &String,
        columns: &HashMap<String, DataType>,
    ) -> Result<(), Errors> {
        if !columns.contains_key(column) {
            return Err(Errors::SyntaxError(format!(
                "Column {} not defined in table data",
                column
            )));
        }
        Ok(())
    }

    fn check_data_type_matching(
        &self,
        column: &String,
        columns: &HashMap<String, DataType>,
        literal: &Literal,
    ) -> Result<(), Errors> {
        if let Some(data_type) = columns.get(column) {
            if data_type != &literal.data_type {
                return Err(Errors::SyntaxError(format!(
                    "Value to set ({}) does not match the column defined type",
                    literal.value
                )));
            }
        }
        Ok(())
    }
}

impl Default for UpdateQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for UpdateQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        self.check_values()?;
        let Some(where_clause) = &self.where_clause else {
            return Err(Errors::SyntaxError(String::from(
                "Where clause must be defined",
            )));
        };
        data_access.update_row(&self.table_name, &self.changes, where_clause)?;
        Ok(get_long_string_from_str("Update was successful"))
    }

    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors> {
        let Some(where_clause) = &self.where_clause else {
            return Err(Errors::SyntaxError(String::from(
                "Where clause must be defined",
            )));
        };
        let mut primary_key = Vec::new();
        let table_pk = get_table_pk(&self.table_name)?;
        if where_clause.get_primary_key(&mut primary_key, &table_pk)? {
            if primary_key.len() != table_pk.len() {
                return Err(Errors::SyntaxError(String::from(
                    "Full primary key must be defined in where clause",
                )));
            }
            Ok(Some(primary_key))
        } else {
            Ok(None)
        }
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
