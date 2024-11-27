use std::collections::HashMap;

use crate::parsers::tokens::data_type::DataType;



pub struct DataResponse {
    headers_pks: HashMap<String, DataType>,
    table: String,
    keyspace: String
}


impl DataResponse {
    pub fn new(headers_pks: HashMap<String, DataType>, table: String, keyspace: String) -> Self {
        Self {
            headers_pks,
            table,
            keyspace,
        }
    }

    pub fn headers_pks(&self) -> &HashMap<String, DataType> {
        &self.headers_pks
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
}