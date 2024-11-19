use super::{query::Query, where_logic::where_clause::WhereClause};
use crate::data_access::data_access_handler::DataAccessHandler;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    check_table_name, get_long_string_from_str, get_partition_key_from_where, split_keyspace_table,
};
use crate::utils::response::Response;
use serde::{Deserialize, Serialize};
use std::any::Any;

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
}

impl Default for DeleteQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for DeleteQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        let Some(where_clause) = &self.where_clause else {
            return Err(Errors::SyntaxError(String::from(
                "Where clause must be defined",
            )));
        };
        data_access.set_deleted_rows(&self.table_name, where_clause)?;
        Response::void()
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(Some(get_partition_key_from_where(
            &self.table_name,
            &self.where_clause,
        )?))
    }

    fn get_keyspace(&self) -> Result<String, Errors> {
        let (kp, _) = split_keyspace_table(&self.table_name)?;
        Ok(kp.to_string())
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
