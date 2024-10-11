use crate::parsers::query_parsers::where_clause_::where_clause::WhereClause;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;

use super::query::Query;

#[derive(PartialEq, Debug)]
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
    fn run(&self) -> Result<(), Errors> {
        todo!()
    }
}