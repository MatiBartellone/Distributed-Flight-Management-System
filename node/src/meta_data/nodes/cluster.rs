use super::node::Node;
use crate::utils::constants::QUERY_DELEGATION_PORT_MOD;
use crate::utils::errors::Errors;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Cluster {
    own_node: Node,
    other_nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(special_node: Node, other_nodes: Vec<Node>) -> Self {
        Cluster {
            own_node: special_node,
            other_nodes,
        }
    }

    pub fn get_own_ip(&self) -> &str {
        self.own_node.get_ip()
    }
    pub fn get_own_port(&self) -> &str {
        self.own_node.get_port()
    }

    pub fn len_nodes(&self) -> usize {
        self.other_nodes.len() + 1
    }

    pub fn append_new_node(&mut self, node: Node) {
        self.other_nodes.push(node)
    }

    pub fn get_nodes(&self, position: usize, replication: usize) -> Result<Vec<String>, Errors> {
        let end_position = position + replication;
        let mut ips = Vec::new();
        let total_nodes = self.len_nodes();
        for node in self.other_nodes.iter() {
            let node_pos = node.get_pos();
            if Self::is_in_range(position, end_position, node_pos, total_nodes) {
                ips.push(node.get_full_ip(QUERY_DELEGATION_PORT_MOD)?);
            }
        }
        let own_pos = self.own_node.get_pos();
        if Self::is_in_range(position, end_position, own_pos, total_nodes) {
            ips.push(self.own_node.get_full_ip(QUERY_DELEGATION_PORT_MOD)?);
        }
        Ok(ips)
    }

    fn is_in_range(start: usize, end: usize, position: usize, maximum: usize) -> bool {
        if end <= maximum {
            position >= start && position < end
        } else {
            let modified_end = end - maximum;
            position >= start || position < modified_end
        }
    }

    pub fn get_all_ips(&self) -> Result<Vec<String>, Errors> {
        let mut ips = Vec::new();
        ips.push(self.own_node.get_full_ip(QUERY_DELEGATION_PORT_MOD)?);
        for node in self.other_nodes.iter() {
            ips.push(node.get_full_ip(QUERY_DELEGATION_PORT_MOD)?);
        }
        Ok(ips)
    }
}
