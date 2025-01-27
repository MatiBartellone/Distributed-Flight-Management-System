use crate::meta_data::nodes::node::State::{Active, Booting, Inactive};
use crate::utils::errors::Errors;
use crate::utils::types::node_ip::NodeIp;
use crate::utils::types::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use crate::utils::types::range::Range;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum State {
    Active,
    Inactive,
    Booting,
    StandBy,
    ShuttingDown,
    Recovering,
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
        self.state = Inactive;
    }

    pub fn set_active(&mut self) {
        self.state = Active
    }

    pub fn set_booting(&mut self) {
        self.state = Booting
    }

    pub fn set_stand_by(&mut self) {
        self.state = State::StandBy;
    }

    pub fn set_shutting_down(&mut self) {
        self.state = State::ShuttingDown;
    }

    pub fn set_recovering(&mut self) {
        self.state = State::Recovering;
    }

    pub fn get_range(&self) -> Range {
        self.range.clone()
    }

    pub fn set_range(&mut self, range: Range) {
        self.range = range;
    }

    pub fn set_range_by_pos(&mut self, nodes_quantity: usize) {
        self.range = Range::new(self.position, nodes_quantity);
    }

    pub fn set_state(&mut self, state: &State) {
        self.state = state.clone();
    }
}
