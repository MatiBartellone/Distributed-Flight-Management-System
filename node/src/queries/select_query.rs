use super::query::Query;
use super::where_logic::where_clause::WhereClause;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    check_table_name, get_columns_from_table,
    get_partition_key_from_where, split_keyspace_table, use_data_access,
};
use crate::utils::response::Response;
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub where_clause: Option<WhereClause>,
    pub order_clauses: Option<Vec<OrderByClause>>,
}

impl SelectQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            columns: Vec::new(),
            where_clause: None,
            order_clauses: None,
        }
    }

    fn check_columns(&self) -> Result<(), Errors> {
        let table_columns = get_columns_from_table(&self.table_name)?;
        if self.columns.contains(&'*'.to_string()) {
            if self.columns.len() != 1 {
                return Err(Errors::SyntaxError(String::from(
                    "If * was used, no other columns must be given",
                )));
            }
            return Ok(());
        }
        for column in &self.columns {
            if !table_columns.contains_key(column.to_string().as_str()) {
                return Err(Errors::SyntaxError(format!(
                    "Column {} not found in table columns",
                    column
                )));
            }
        }
        Ok(())
    }

    /*fn get_rows_string(&self, rows: Vec<Row>) -> Result<String, Errors> {
        let mut display_columns = Vec::new();
        if self.columns.contains(&'*'.to_string()) {
            display_columns = get_columns_from_table(&self.table_name)?
                .keys()
                .map(|x| x.to_string())
                .collect::<Vec<String>>();
        } else {
            for column in &self.columns {
                display_columns.push(column.to_string());
            }
        }
        let mut text = "".to_string();
        text.push_str(&display_columns.join(", "));
        text.push('\n');
        for row in rows {
            let mut displayed_values = Vec::new();
            for column in &display_columns {
                if let Some(value) = row.get_value(column)? {
                    displayed_values.push(value);
                }
            }
            text.push_str(&displayed_values.join(", "));
            text.push('\n');
        }
        Ok(text)
    }*/
}

impl Default for SelectQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for SelectQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        self.check_columns()?;
        let Some(where_clause) = &self.where_clause else {
            return Err(Errors::SyntaxError(String::from(
                "Where clause must be defined",
            )));
        };
        let rows = use_data_access(|data_access| {
            data_access.select_rows(&self.table_name, where_clause, &self.order_clauses)
        })?;
        let (kesypace_name, table) = split_keyspace_table(&self.table_name)?;
        Response::rows(rows, kesypace_name, table, &self.columns)
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(Some(get_partition_key_from_where(
            &self.table_name,
            &self.where_clause,
        )?))
    }

    fn get_keyspace(&self) -> Result<String, Errors> {
        let (kp, _) = split_keyspace_table(&self.table_name)?;
        Ok(kp.to_string())
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
