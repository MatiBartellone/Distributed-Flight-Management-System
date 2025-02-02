use super::node::Node;
use crate::utils::errors::Errors;
use crate::utils::types::node_ip::NodeIp;
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

    pub fn get_own_node(&self) -> &Node {
        &self.own_node
    }

    pub fn get_other_nodes(&self) -> &Vec<Node> {
        &self.other_nodes
    }

    pub fn get_own_ip(&self) -> &NodeIp {
        self.own_node.get_ip()
    }

    pub fn len_nodes(&self) -> usize {
        self.other_nodes.len() + 1
    }

    pub fn append_new_node(&mut self, node: Node) {
        if !self.other_nodes.contains(&node) {
            self.other_nodes.push(node)
        }
    }

    pub fn get_nodes(&self, position: usize, replication: usize) -> Result<Vec<NodeIp>, Errors> {
        let end_position = position + replication;
        let mut ips: Vec<NodeIp> = Vec::new();
        let total_nodes = self.len_nodes();
        for node in self.other_nodes.iter() {
            let node_pos = node.get_pos();
            if Self::is_in_range(position, end_position, node_pos, total_nodes) {
                ips.push(NodeIp::new_from_ip(node.get_ip()));
            }
        }
        let own_pos = self.own_node.get_pos();
        if Self::is_in_range(position, end_position, own_pos, total_nodes) {
            ips.push(NodeIp::new_from_ip(self.own_node.get_ip()));
        }
        Ok(ips)
    }

    pub fn get_node_pos_by_range(&self, range: usize) -> Result<usize, Errors> {
        if self.own_node.get_range().is_in_range(range) {
            return Ok(self.own_node.get_pos());
        } else {
            for node in self.other_nodes.iter() {
                if node.get_range().is_in_range(range) {
                    return Ok(node.get_pos());
                }
            }
        }
        Ok(1)
    }

    fn is_in_range(start: usize, end: usize, position: usize, maximum: usize) -> bool {
        if end <= maximum {
            position >= start && position < end
        } else {
            let modified_end = end - maximum;
            position >= start || position < modified_end
        }
    }

    pub fn get_all_ips(&self) -> Result<Vec<NodeIp>, Errors> {
        let mut ips = Vec::new();
        ips.push(NodeIp::new_from_ip(self.own_node.get_ip()));
        for node in self.other_nodes.iter() {
            ips.push(NodeIp::new_from_ip(node.get_ip()));
        }
        Ok(ips)
    }
}
