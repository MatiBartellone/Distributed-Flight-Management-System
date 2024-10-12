use super::{drop_table_query::DropTableQuery, drop_keyspace_query::DropKeySpaceQuery, query::Query};

pub struct DropQuery{
    pub table: Option<DropTableQuery>,
    pub keyspace: Option<DropKeySpaceQuery>,
}

impl DropQuery {
    pub fn new() -> Self {
        Self {
            table: None,
            keyspace: None,
        }
    }
    pub fn set_table_query(&mut self, sub_query: DropTableQuery) {
        self.table = Some(sub_query);
    }
    pub fn set_keyspace_query(&mut self, sub_query: DropKeySpaceQuery) {
        self.keyspace = Some(sub_query);
    }

}

impl Query for DropQuery{

    fn run(&mut self) {
        unimplemented!()
        /*if self.table.is_some(){
            self.table.run()
        } else {
            self.keyspace.run()
        }*/
    }
}

impl Default for DropQuery {
    fn default() -> Self {
         Self::new()
    }
}