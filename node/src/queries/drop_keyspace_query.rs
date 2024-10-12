
use crate::queries::query::Query;

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
    fn run(&mut self) {
        unimplemented!()
    }
}

impl Default for DropKeySpaceQuery {
    fn default() -> Self {
         Self::new()
    }
}
