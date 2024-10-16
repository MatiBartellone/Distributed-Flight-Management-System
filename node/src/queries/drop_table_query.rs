use crate::{queries::query::Query, utils::errors::Errors};

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
    fn run(&self) -> Result<(), Errors> {
        unimplemented!()
    }
}

impl Default for DropTableQuery {
    fn default() -> Self {
        Self::new()
    }
}
