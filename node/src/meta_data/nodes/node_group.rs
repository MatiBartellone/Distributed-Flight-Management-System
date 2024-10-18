use serde::{Serialize, Deserialize};

use super::node::Node;


#[derive(Serialize, Deserialize, Debug)]
pub struct NodeGroup {
    special_node: Node,
    other_nodes: Vec<Node>,
}

impl NodeGroup {
    pub fn new(special_node: Node, other_nodes: Vec<Node>) -> Self {
        NodeGroup {
            special_node,
            other_nodes,
        }
    }

    pub fn ip_principal(&self) -> &str {
        self.special_node.get_ip()
    }

    pub fn len_nodes(&self) ->  usize {
        self.other_nodes.len()+1
    }

    pub fn get_node(&self, position: usize) -> Option<Node> {
        let node = self.other_nodes.iter().find(|node| node.get_pos() == position);
        if let Some(node) = node {
            return Some(Node::new(node.get_ip().to_string(), node.get_pos()));
        }
        None
    }
}