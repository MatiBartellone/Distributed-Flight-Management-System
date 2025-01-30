use crate::meta_data::nodes::node::State::{
    Active, Booting, Inactive, Recovering, ShuttingDown, StandBy,
};
use crate::utils::errors::Errors;
use crate::utils::types::node_ip::NodeIp;
use crate::utils::types::range::Range;
use crate::utils::types::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum State {
    Active,
    Inactive,
    Booting,
    StandBy,
    ShuttingDown,
    Recovering,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Active => write!(f, "Active"),
            Inactive => write!(f, "Inactive"),
            Booting => write!(f, "Booting"),
            StandBy => write!(f, "StandBy"),
            ShuttingDown => write!(f, "ShuttingDown"),
            Recovering => write!(f, "Recovering"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    pub ip: NodeIp,
    pub position: usize,
    pub range: Range,
    pub is_seed: bool,
    pub state: State,
    pub timestamp: Timestamp,
}

impl Node {
    pub fn new(ip: &NodeIp, position: usize, is_seed: bool, range: Range) -> Result<Self, Errors> {
        Ok(Self {
            ip: NodeIp::new_from_ip(ip),
            position,
            range,
            is_seed,
            state: Active,
            timestamp: Timestamp::new(),
        })
    }

    pub fn new_from_node(node: &Node) -> Self {
        Self {
            ip: NodeIp::new_from_ip(&node.ip),
            position: node.position,
            range: node.range.clone(),
            is_seed: node.is_seed,
            state: node.state.clone(),
            timestamp: Timestamp::new_from_timestamp(&node.timestamp),
        }
    }

    pub fn get_ip(&self) -> &NodeIp {
        &self.ip
    }

    pub fn get_pos(&self) -> usize {
        self.position
    }

    pub fn set_pos(&mut self, pos: usize) {
        self.position = pos;
        self.update_timestamp()
    }

    pub fn is_seed(&self) -> bool {
        self.is_seed
    }

    pub fn get_timestamp(&self) -> Timestamp {
        Timestamp::new_from_timestamp(&self.timestamp)
    }

    pub fn update_timestamp(&mut self) {
        self.timestamp = Timestamp::new()
    }

    pub fn set_inactive(&mut self) {
        self.set_state(&Inactive)
    }

    pub fn set_active(&mut self) {
        self.set_state(&Active)
    }

    pub fn set_booting(&mut self) {
        self.set_state(&Booting)
    }

    pub fn set_stand_by(&mut self) {
        self.set_state(&StandBy)
    }

    pub fn set_shutting_down(&mut self) {
        self.set_state(&ShuttingDown)
    }

    pub fn set_recovering(&mut self) {
        self.set_state(&Recovering)
    }

    pub fn get_range(&self) -> Range {
        self.range.clone()
    }

    pub fn set_range(&mut self, range: Range) {
        self.range = range;
        self.update_timestamp()
    }

    pub fn set_range_by_pos(&mut self, nodes_quantity: usize) {
        self.range = Range::from_fraction(self.position, nodes_quantity);
        self.update_timestamp()
    }

    pub fn set_nonexistent_range(&mut self) {
        self.range = Range::new_nonexistent();
        self.update_timestamp()
    }

    pub fn set_state(&mut self, state: &State) {
        self.state = state.clone();
        self.update_timestamp()
    }
}
