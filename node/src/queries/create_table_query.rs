use crate::parsers::tokens::data_type::DataType;
use crate::queries::query::Query;
use crate::utils::constants::KEYSPACE_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    check_table_name, get_long_string_from_str, split_keyspace_table, use_data_access,
    use_keyspace_meta_data,
};
use crate::utils::primary_key::PrimaryKey;
use crate::utils::response::Response;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableQuery {
    pub table_name: String,
    pub columns: HashMap<String, DataType>,
    pub primary_key: PrimaryKey,
}

impl CreateTableQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            columns: HashMap::new(),
            primary_key: PrimaryKey::new_empty(),
        }
    }

    fn push_on_meta_data(&self) -> Result<(&str, &str), Errors> {
        let (kesypace_name, table) = split_keyspace_table(&self.table_name)?;
        use_keyspace_meta_data(|handler| {
            handler.add_table(
                KEYSPACE_METADATA_PATH.to_owned(),
                kesypace_name,
                table,
                self.primary_key.to_owned(),
                self.columns.to_owned(),
            )
        })
    }

    fn push_on_data_acces(&self) -> Result<(), Errors> {
        use_data_access(|data_access| data_access.create_table(&self.table_name))
    }
}

impl Default for CreateTableQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for CreateTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        self.push_on_data_acces()?;
        let (kesypace_name, table) = self.push_on_meta_data()?;
        let options = format!("{}.{}", kesypace_name, table);
        Response::schema_change("CREATED", "TABLE", &options)
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        Ok(None)
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
