use super::if_clause::IfClause;
use super::{query::Query, where_logic::where_clause::WhereClause};
use crate::utils::errors::Errors;
use crate::utils::functions::{
    check_table_name, get_partition_key_from_where, split_keyspace_table,
    use_data_access,
};
use crate::utils::response::Response;
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(Default, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DeleteQuery {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
    pub if_clause: Option<IfClause>,
}

impl DeleteQuery {
    pub fn new(
        table_name: String,
        where_clause: Option<WhereClause>,
        if_clause: Option<IfClause>,
    ) -> Self {
        Self {
            table_name,
            where_clause,
            if_clause,
        }
    }
}

impl Query for DeleteQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let Some(where_clause) = &self.where_clause else {
            return Err(Errors::SyntaxError(String::from(
                "Where clause must be defined",
            )));
        };
        let _apllied = use_data_access(|data_access| {
            data_access.set_deleted_rows(&self.table_name, where_clause, &self.if_clause)
        })?;
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
