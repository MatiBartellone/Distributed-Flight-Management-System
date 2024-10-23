use crate::data_access::data_access_handler::DataAccessHandler;
use crate::data_access::row::{Column, Row};
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::tokens::data_type::DataType;
use crate::utils::constants::KEYSPACE_METADATA;
use crate::utils::functions::{check_table_name, get_long_string_from_str, get_timestamp};
use crate::{parsers::tokens::literal::Literal, queries::query::Query, utils::errors::Errors};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};

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
        self.check_different_values()?;
        if columns.len() < self.headers.len() {
            return Err(Errors::SyntaxError(String::from(
                "More columns given than defined in table",
            )));
        }
        let Some(values) = self.values_list.first() else {
            return Err(Errors::SyntaxError("No values provided".to_string()));
        };
        self.check_data_types_and_existance(values, &columns)?;

        Ok(())
    }

    fn check_different_values(&self) -> Result<(), Errors> {
        let set : &HashSet<_> = &self.headers.iter().collect();
        if set.len() != self.headers.len() {
            return Err(Errors::SyntaxError(String::from("There is a header repeated")))
        }
        Ok(())
    }

    fn check_data_types_and_existance(
        &self,
        values: &[Literal],
        columns: &HashMap<String, DataType>,
    ) -> Result<(), Errors> {
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
            } else {
                return Err(Errors::SyntaxError(format!(
                    "Column {} is not defined",
                    header
                )));
            }
        }
        Ok(())
    }

    fn build_row(&self, values: &[Literal]) -> Result<Row, Errors> {
        let mut row_values = Vec::new();
        for (value, header) in values.iter().zip(self.headers.iter()) {
            row_values.push(Column::new(header, value, get_timestamp()?));
        }
        let Some(primary_keys) = self.get_primary_key()? else {
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

    fn get_primary_key(&self) -> Result<Option<Vec<String>>, Errors> {
        let Some(row) = self.values_list.first() else {
            return Err(Errors::SyntaxError("No values provided".to_string()));
        };
        self.check_different_values()?;
        let binding = self.table_name.split('.').collect::<Vec<&str>>();
        let identifiers = &binding.as_slice();
        let mut stream = MetaDataHandler::establish_connection()?;
        let keyspace_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_keyspace_meta_data_access();
        let table_primary_keys : HashSet<_> = keyspace_meta_data.get_primary_key(
            KEYSPACE_METADATA.to_string(),
            identifiers[0],
            identifiers[1],
        )?.into_iter().collect();
        let mut primary_keys = Vec::new();
        if row.len() != self.headers.len() {
            return Err(Errors::SyntaxError(String::from(
                "Values doesnt match given headers",
            )));
        }
        for (value, header) in row.iter().zip(self.headers.iter()) {
            if table_primary_keys.contains(header) {
                primary_keys.push(value.value.to_string());
            }
        }
        if primary_keys.len() != table_primary_keys.len() {
            return Err(Errors::SyntaxError(String::from("Missing primary keys")));
        }
        Ok(Some(primary_keys))
    }

    fn set_table(&mut self) -> Result<(), Errors> {
        self.table_name = check_table_name(&self.table_name)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
