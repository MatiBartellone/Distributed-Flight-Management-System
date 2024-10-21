use std::thread::{self};

use serde::{Serialize, Deserialize};


#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Client {
    id: String,
    startup: bool,
    authorized: bool,
    keyspace: Option<String>
}

impl Client {
    pub fn new() -> Self {
        let thread_id = thread::current().id();
        let id = format!("{:?}", thread_id);
        Client {
            id,
            startup: false,
            authorized: false,
            keyspace: None,
        }
    }

    pub fn has_started(&self) -> bool {
        self.startup
    }

    pub fn is_authorized(&self) -> bool {
        self.authorized
    }

    pub fn get_keyspace(&self) -> Option<String> {
        self.keyspace.to_owned()
    }

    pub fn authorize(&mut self) {
        self.authorized = true;
    }

    pub fn start_up(&mut self) {
        self.startup = true;
    }

    pub fn set_keyspace(&mut self, keyspace: String) {
        self.keyspace = Some(keyspace);
    }

    pub fn is_id(&self, searched_id: &str) -> bool {
        self.id == searched_id
    }
}


