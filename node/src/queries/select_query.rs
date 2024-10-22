use super::query::Query;
use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::queries::order_by_clause::OrderByClause;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::process;

use super::where_logic::where_clause::WhereClause;

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

impl Default for SelectQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for SelectQuery {
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
