use std::collections::HashMap;

pub struct CreateKeyspaceQuery {
    pub table: String,
    pub replication: HashMap<String, String>,
}

impl CreateKeyspaceQuery {
    pub fn new() -> Self {
        Self {
            table: String::new(),
            replication: HashMap::<String, String>::new(),
        }
    }
}
