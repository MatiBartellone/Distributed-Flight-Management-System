
use crate::queries::query::Query;

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
    fn run(&mut self) {
        unimplemented!()
    }
}

impl Default for DropTableQuery {
    fn default() -> Self {
         Self::new()
    }
}