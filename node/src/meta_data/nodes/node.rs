use crate::utils::errors::Errors;
use crate::utils::functions::get_timestamp;
use serde::{Deserialize, Serialize};
use crate::utils::node_ip::NodeIp;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    pub ip: NodeIp,
    pub position: usize,
    pub is_seed: bool,
    pub is_active: bool,
    pub timestamp: u64,
}

impl Node {
    pub fn new(ip: NodeIp, position: usize, is_seed: bool) -> Result<Self, Errors> {
        Ok(Self {
            ip,
            position,
            is_seed,
            is_active: true,
            timestamp: get_timestamp()?,
        })
    }

    pub fn new_from_node(node: &Node) -> Self {
        Self {
            ip: NodeIp::new_from_ip(&node.ip),
            position: node.position,
            is_seed: node.is_seed,
            is_active: node.is_active,
            timestamp: node.timestamp,
        }
    }

    pub fn get_ip(&self) -> &NodeIp {
        &self.ip
    }

    pub fn get_pos(&self) -> usize {
        self.position
    }

    pub fn is_seed(&self) -> bool {
        self.is_seed
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn update_timestamp(&mut self) -> Result<(), Errors> {
        self.timestamp = get_timestamp()?;
        Ok(())
    }

    pub fn set_inactive(&mut self) {
        self.is_active = false
    }

    pub fn set_active(&mut self) {
        self.is_active = true
    }
}
