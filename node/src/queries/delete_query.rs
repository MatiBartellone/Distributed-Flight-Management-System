use super::where_logic::where_clause::WhereClause;
use crate::utils::errors::Errors;

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
        unimplemented!()
    }
}
