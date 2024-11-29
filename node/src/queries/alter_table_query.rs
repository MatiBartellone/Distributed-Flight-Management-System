use super::query::Query;
use crate::utils::constants::KEYSPACE_METADATA_PATH;
use crate::utils::functions::{
    check_table_name, split_keyspace_table, use_keyspace_meta_data,
};
use crate::utils::response::Response;
use crate::{parsers::tokens::data_type::DataType, utils::errors::Errors};
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct AlterTableQuery {
    pub table_name: String,
    pub operation: Option<Operations>,
    pub first_column: String,
    pub second_column: String,
    pub data: DataType,
}
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Operations {
    ADD,
    ALTER,
    RENAME,
    DROP,
    WITH,
}

impl Default for AlterTableQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl AlterTableQuery {
    pub fn new() -> AlterTableQuery {
        AlterTableQuery {
            table_name: String::new(),
            operation: None,
            first_column: String::new(),
            second_column: String::new(),
            data: DataType::Int,
        }
    }

    fn add(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        use_keyspace_meta_data(|handler| {
            handler.new_column(
                KEYSPACE_METADATA_PATH.to_owned(),
                keyspace_name,
                table,
                &self.first_column,
                self.data.to_owned(),
            )
        })
    }

    fn drop(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        use_keyspace_meta_data(|handler| {
            handler.drop_column(
                KEYSPACE_METADATA_PATH.to_owned(),
                keyspace_name,
                table,
                &self.first_column,
            )
        })
    }

    fn rename(&self) -> Result<(), Errors> {
        let (keyspace_name, table) = split_keyspace_table(&self.table_name)?;
        use_keyspace_meta_data(|handler| {
            handler.rename_column(
                KEYSPACE_METADATA_PATH.to_owned(),
                keyspace_name,
                table,
                &self.first_column,
                &self.second_column,
            )
        })
    }
}

impl Query for AlterTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        if let Some(operation) = &self.operation {
            let (change_type, target, options) = match operation {
                Operations::ADD => {
                    self.add().map_err(|e| Errors::ServerError(format!("Failed to add: {}", e)))?;
                    let options = format!("{} {}", self.table_name, "ADD column_name column_type");
                    ("ALTERED", "TABLE", options)
                }
                Operations::DROP => {
                    self.drop().map_err(|e| Errors::ServerError(format!("Failed to drop: {}", e)))?;
                    let options = format!("{} {}", self.table_name, "DROP column_name");
                    ("ALTERED", "TABLE", options)
                }
                Operations::RENAME => {
                    self.rename().map_err(|e| Errors::ServerError(format!("Failed to rename: {}", e)))?;
                    let options = format!(
                        "{} RENAME {} TO {}",
                        self.table_name, self.first_column, self.second_column
                    );
                    ("ALTERED", "TABLE", options)
                }
                _ => return Err(Errors::SyntaxError("Invalid Operation to Alter Table".to_string())),
            };
            
            Response::schema_change(change_type, target, &options)
        } else {
            Err(Errors::SyntaxError("Invalid Operation to Alter Table".to_string()))
        }
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
