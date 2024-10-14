use serde::{Deserialize, Serialize};
use crate::{parsers::tokens::literal::Literal, queries::query::Query, utils::errors::Errors};


#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub struct InsertQuery {
    pub table: String,
    pub headers: Vec<String>,
    pub values_list: Vec<Vec<Literal>>,
}

impl InsertQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            headers: Vec::new(),
            values_list: Vec::new(),
        }
    }
}

impl Default for InsertQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl Query for InsertQuery {
    fn run(&self) -> Result<String, Errors> {
        unimplemented!()
    }
}