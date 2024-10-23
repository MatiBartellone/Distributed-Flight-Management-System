use super::query::Query;
use super::where_logic::where_clause::WhereClause;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;
use crate::utils::functions::check_table_name;
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
}

impl Default for SelectQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for SelectQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        todo!()
    }

    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
