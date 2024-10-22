use super::{query::Query, where_logic::where_clause::WhereClause};
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use crate::utils::functions::check_table_name;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DeleteQuery {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
}

impl DeleteQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            where_clause: None,
        }
    }

    pub fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
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

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
