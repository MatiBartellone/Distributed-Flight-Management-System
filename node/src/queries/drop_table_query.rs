
use crate::{queries::query::Query, utils::errors::Errors};
use std::any::Any;

#[derive(PartialEq, Debug)]
pub struct DropTableQuery {
    pub table: String,
    pub if_exist: Option<bool>,
}

impl DropTableQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            if_exist: None,
        }
    }
}

impl Query for DropTableQuery {
    fn run(&self) -> Result<Vec<u8>, Errors> {
        unimplemented!()
    }

    fn get_primary_key(&self) -> Option<String> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Default for DropTableQuery {
    fn default() -> Self {
        Self::new()
    }
}
