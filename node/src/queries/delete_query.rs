use crate::{parsers::query_parsers::where_clause_::where_clause::WhereClause, utils::errors::Errors};

use super::query::Query;


#[derive(PartialEq, Debug)]
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

impl DeleteQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            where_clause: None,
        }
    }
}

impl Default for DeleteQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for DeleteQuery {
    fn run(&self) -> Result<(), Errors> {
        todo!()
    }
}