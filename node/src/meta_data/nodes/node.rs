use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    ip: String,
    position: usize,
}

impl Node {
    pub fn new(ip: String, position: usize) -> Self {
        Node { ip, position }
    }

    pub fn get_ip(&self) -> &str {
        &self.ip
    }

    pub fn get_pos(&self) -> usize {
        self.position
    }
}
