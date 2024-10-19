use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;

use super::query::Query;

use super::where_logic::where_clause::WhereClause;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct SelectQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Option<WhereClause>,
    pub order_clauses: Option<Vec<OrderByClause>>,
}

impl SelectQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
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

    fn get_primary_key(&self) -> Option<String> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

