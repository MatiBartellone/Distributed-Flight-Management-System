use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub ip: String,
    pub port: String,
    pub position: usize,
}

impl Node {
    pub fn new(ip: String, port: String, position: usize) -> Self {
        Self { ip, port, position }
    }

    pub fn get_ip(&self) -> &str {
        &self.ip
    }

    pub fn get_port(&self) -> &str {
        &self.port
    }

    pub fn get_pos(&self) -> usize {
        self.position
    }

    pub fn get_full_ip(&self, port_modifier: i32) -> Result<String, Errors> {
        let port = self
            .port
            .parse::<i32>()
            .map_err(|_| Errors::ServerError(String::from("Failed to parse port")))?
            + port_modifier;
        Ok(format!("{}:{}", self.ip, port))
    }
}
