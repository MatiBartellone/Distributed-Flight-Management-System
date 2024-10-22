use super::{query::Query, where_logic::where_clause::WhereClause};
use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::process;

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
