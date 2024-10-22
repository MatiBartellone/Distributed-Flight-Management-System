use super::{
    if_clause::IfClause, query::Query, set_logic::assigmente_value::AssignmentValue,
    where_logic::where_clause::WhereClause,
};
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use crate::utils::functions::check_table_name;

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

    pub fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
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
        todo!()
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
