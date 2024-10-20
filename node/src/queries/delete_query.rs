use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;

use super::{query::Query, where_logic::where_clause::WhereClause};

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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
