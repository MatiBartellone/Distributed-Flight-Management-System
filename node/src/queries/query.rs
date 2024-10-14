use serde::{Deserialize, Serialize};
use crate::queries::insert_query::InsertQuery;
use crate::queries::select_query::SelectQuery;
use crate::utils::errors::Errors;

pub trait Query {
    fn run(&self) -> Result<String, Errors>;
}

#[derive(Serialize, Deserialize)]
pub enum QueryEnum{
    Insert(InsertQuery),
}

impl Query for QueryEnum {
    fn run(&self) -> Result<String, Errors> {
        match self {
            QueryEnum::Insert(q) => q.run(),
        }
    }
}