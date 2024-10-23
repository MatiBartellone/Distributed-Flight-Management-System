use crate::data_access::data_access_handler::DataAccessHandler;
use crate::data_access::row::{Column, Row};
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::utils::constants::KEYSPACE_METADATA;
use crate::utils::functions::{check_table_name, get_long_string_from_str, get_timestamp};
use crate::{parsers::tokens::literal::Literal, queries::query::Query, utils::errors::Errors};
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InsertQuery {
    pub table_name: String,
    pub headers: Vec<String>,
    pub values_list: Vec<Vec<Literal>>,
}

impl InsertQuery {
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            headers: Vec::new(),
            values_list: Vec::new(),
        }
    }

    fn check_columns(&self) -> Result<(), Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let keyspace_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
        let binding = self.table_name.split('.').collect::<Vec<&str>>();
        let identifiers = &binding.as_slice();
        let columns = keyspace_meta_data.get_columns_type(
            KEYSPACE_METADATA.to_string(),
            identifiers[0],
            identifiers[1],
        )?;
        if columns.len() < self.headers.len() {
            return Err(Errors::SyntaxError(String::from(
                "More columns given than defined in table",
            )));
        }
        for header in &self.headers {
            if !columns.contains_key(header) {
                return Err(Errors::SyntaxError(format!(
                    "Column {} is not defined",
                    header
                )));
            }
        }
        for values in self.values_list.iter() {
            if values.len() != self.headers.len() {
                return Err(Errors::SyntaxError(String::from(
                    "Values doesnt match given headers",
                )));
            }
            for (value, header) in values.iter().zip(self.headers.iter()) {
                if let Some(column_data_type) = columns.get(header) {
                    if &value.data_type != column_data_type {
                        return Err(Errors::SyntaxError(format!(
                            "Value datatype for {} do not match the defined column",
                            header
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn build_row(&self, values: &[Literal]) -> Result<Row, Errors> {
        let mut row_values = Vec::new();
        for (value, header) in values.iter().zip(self.headers.iter()) {
            row_values.push(Column::new(header, value, get_timestamp()?));
        }
        let Some(primary_keys) = self.get_primary_key() else {
            return Err(Errors::SyntaxError(String::from(
                "Primary keys not defined",
            )));
        };
        Ok(Row::new(row_values, primary_keys))
    }
}

impl Default for InsertQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for InsertQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        let mut stream = DataAccessHandler::establish_connection()?;
        let data_access = DataAccessHandler::get_instance(&mut stream)?;
        self.check_columns()?;
        for values in self.values_list.iter() {
            let row = self.build_row(values)?;
            data_access.insert(&self.table_name, &row)?
        }
        Ok(get_long_string_from_str("Insertion was successful"))
    }

    fn get_primary_key(&self) -> Option<Vec<String>> {
        None
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
