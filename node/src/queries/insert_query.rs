use crate::data_access::row::{Column, Row};
use crate::parsers::tokens::data_type::DataType;
use crate::utils::functions::{
    check_table_name, get_columns_from_table, get_long_string_from_str,
    get_table_clustering_columns, get_table_partition, split_keyspace_table, use_data_access,
};
use crate::{parsers::tokens::literal::Literal, queries::query::Query, utils::errors::Errors};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InsertQuery {
    pub table_name: String,
    pub headers: Vec<String>,
    pub values_list: Vec<Vec<Literal>>,
    pub if_exists: Option<bool>,
}

impl InsertQuery {
    fn check_columns(&self) -> Result<(), Errors> {
        let columns = get_columns_from_table(&self.table_name)?;
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
        let set: &HashSet<_> = &self.headers.iter().collect();
        if set.len() != self.headers.len() {
            return Err(Errors::SyntaxError(String::from(
                "There is a header repeated",
            )));
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
            row_values.push(Column::new(header, value));
        }
        let Some(partition_keys) = self.get_partition()? else {
            return Err(Errors::SyntaxError(String::from(
                "Primary keys not defined",
            )));
        };
        let Some(clustering_columns) = self.get_clustering_columns()? else {
            return Err(Errors::SyntaxError(String::from(
                "Primary keys not defined",
            )));
        };
        Ok(Row::new(
            row_values,
            [&partition_keys[..], &clustering_columns[..]].concat(),
        ))
    }

    fn get_keys(&self, set: HashSet<String>) -> Result<Option<Vec<String>>, Errors> {
        let Some(row) = self.values_list.first() else {
            return Err(Errors::SyntaxError("No values provided".to_string()));
        };
        self.check_different_values()?;
        let mut partition_keys = Vec::new();
        if row.len() != self.headers.len() {
            return Err(Errors::SyntaxError(String::from(
                "Values doesnt match given headers",
            )));
        }
        for (value, header) in row.iter().zip(self.headers.iter()) {
            if set.contains(header) {
                partition_keys.push(value.value.to_string());
            }
        }
        if partition_keys.len() != set.len() {
            return Err(Errors::SyntaxError(String::from("Missing primary keys")));
        }
        Ok(Some(partition_keys))
    }

    fn get_clustering_columns(&self) -> Result<Option<Vec<String>>, Errors> {
        self.get_keys(get_table_clustering_columns(&self.table_name)?)
    }
}

impl Query for InsertQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        self.check_columns()?;
        let _applied = use_data_access(|data_access| {
            let mut applied = None;
            for values in self.values_list.iter() {
                let row = self.build_row(values)?;
                if let Some(if_exists) = self.if_exists {
                    if if_exists {
                        applied = Some(data_access.simple_update_row(&self.table_name, &row)?);
                    } else {
                        applied = Some(data_access.insert(&self.table_name, &row)?);
                    }
                } else {
                    data_access.insert_or_update(&self.table_name, &row)?;
                }
            }
            Ok(applied)
        })?;
        Ok(get_long_string_from_str("Insertion was successful"))
    }

    fn get_partition(&self) -> Result<Option<Vec<String>>, Errors> {
        self.get_keys(get_table_partition(&self.table_name)?)
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
