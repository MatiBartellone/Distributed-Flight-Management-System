
use crate::{queries::query::Query, utils::errors::Errors};
use std::any::Any;

#[derive(PartialEq, Debug)]
pub struct DropKeySpaceQuery {
    pub keyspace: String,
    pub if_exist: Option<bool>,
}

impl DropKeySpaceQuery {
    pub fn new() -> Self {
        Self {
            keyspace: String::new(),
            if_exist: None,
        }
    }
}

impl Query for DropKeySpaceQuery {
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

impl Default for DropKeySpaceQuery {
    fn default() -> Self {
        Self::new()
    }
}
