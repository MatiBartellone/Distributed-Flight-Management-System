use serde::{Serialize, Deserialize};

use super::node::Node;


#[derive(Serialize, Deserialize, Debug)]
pub struct Cluster {
    special_node: Node,
    other_nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(special_node: Node, other_nodes: Vec<Node>) -> Self {
        Cluster {
            special_node,
            other_nodes,
        }
    }

    pub fn get_own_ip(&self) -> &str {
        self.special_node.get_ip()
    }

    pub fn len_nodes(&self) ->  usize {
        self.other_nodes.len()+1
    }

    pub fn get_nodes(&self, position: usize, replication: usize) -> Vec<String> {
        let end_position = position + replication;
        self.other_nodes
            .iter()
            .filter(|node| node.get_pos() >= position && node.get_pos() < end_position)
            .map(|node| node.get_ip().to_string()) 
            .collect()
    }
}