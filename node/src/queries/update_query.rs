use super::{
    if_clause::IfClause, query::Query, set_logic::assigmente_value::AssignmentValue,
    where_logic::where_clause::WhereClause,
};
use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::process;

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
        if self.table_name.is_empty() {
            return Err(Errors::SyntaxError(String::from("Table is empty")));
        }
        if !self.table_name.contains('.')
            && ClientMetaDataAcces::get_keyspace(process::id().to_string())?.is_none()
        {
            return Err(Errors::SyntaxError(String::from(
                "Keyspace not defined and non keyspace in usage",
            )));
        } else {
            let Some(kp) = ClientMetaDataAcces::get_keyspace(process::id().to_string())? else {
                return Err(Errors::SyntaxError(String::from("Keyspace not in usage")));
            };
            self.table_name = format!("{}.{}", kp, self.table_name);
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
        todo!()
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
