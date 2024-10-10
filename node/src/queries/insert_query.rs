use crate::parsers::tokens::token::Literal;
use crate::queries::query::Query;

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
    fn run() {
        unimplemented!()
    }
}

impl Default for InsertQuery {
    fn default() -> Self {
        Self::new()
    }
}
