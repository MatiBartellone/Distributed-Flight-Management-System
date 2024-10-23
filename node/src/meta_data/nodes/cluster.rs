use serde::{Deserialize, Serialize};

use super::node::Node;

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

    pub fn len_nodes(&self) -> usize {
        self.other_nodes.len() + 1
    }

    pub fn get_nodes(&self, position: usize, replication: usize) -> Vec<String> {
        let end_position = position + replication;
        let mut ips = Vec::new();
        let total_nodes = self.len_nodes();
        for node in self.other_nodes.iter() {
            let node_pos = node.get_pos() % total_nodes;
            if (position <= node_pos && node_pos < total_nodes)
                || (node_pos < end_position % total_nodes)
            {
                ips.push(node.get_ip().to_string());
            }
        }
        let own_pos = self.own_node.get_pos() % total_nodes;
        if (position <= own_pos && own_pos < total_nodes) || (own_pos < end_position % total_nodes)
        {
            ips.push(self.own_node.get_ip().to_string());
        }
        ips
    }

    pub fn get_all_ips(&self) -> Vec<String> {
        let mut ips = Vec::new();
        ips.push(self.get_own_ip().to_string());
        for node in self.other_nodes.iter() {
            ips.push(node.get_ip().to_string());
        }
        ips
    }
}
