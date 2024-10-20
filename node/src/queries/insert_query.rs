use crate::data_access::data_access::DataAccess;
use crate::{parsers::tokens::literal::Literal, queries::query::Query, utils::errors::Errors};
#[derive(PartialEq, Debug)]
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
impl Query for InsertQuery {
    fn run(&self) -> Result<(), Errors> {
        Ok(())
    }
}

impl Default for InsertQuery {
    fn default() -> Self {
        Self::new()
    }
}
