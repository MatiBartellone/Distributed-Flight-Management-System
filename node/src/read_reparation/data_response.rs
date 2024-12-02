use std::collections::HashMap;

use crate::parsers::tokens::data_type::DataType;

pub struct DataResponse {
    headers_pks: HashMap<String, DataType>,
    table: String,
    keyspace: String,
    columns: Vec<String>,
}

impl DataResponse {
    pub fn new(
        headers_pks: HashMap<String, DataType>,
        table: String,
        keyspace: String,
        columns: Vec<String>,
    ) -> Self {
        Self {
            headers_pks,
            table,
            keyspace,
            columns,
        }
    }

    pub fn headers_pks(&self) -> HashMap<String, DataType> {
        self.headers_pks.clone()
    }

    pub fn table(&self) -> &String {
        &self.table
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    pub fn get_keyspace_table(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    pub fn colums(&self) -> Vec<String> {
        self.columns.to_vec()
    }
}
