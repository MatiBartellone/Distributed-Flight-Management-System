use crate::meta_data::meta_data_handler::use_node_meta_data;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::{Node, State};
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    deserialize_from_slice, read_exact_from_stream, serialize_to_string, start_listener,
    write_to_stream,
};
use crate::utils::types::node_ip::NodeIp;
use std::collections::HashMap;
use std::net::TcpStream;

pub struct GossipListener;

impl GossipListener {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_gossip_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let buf = read_exact_from_stream(stream)?;
        let received_nodes_list: Vec<Node> = deserialize_from_slice(buf.as_slice())?;
        let received_nodes: HashMap<NodeIp, Node> = received_nodes_list
            .into_iter()
            .map(|node| (node.get_ip().clone(), node))
            .collect();
        let cluster = Self::get_cluster()?;
        let own_node = cluster.get_own_node();

        let mut emitter_required_changes = Self::get_emitter_missing_nodes(&received_nodes)?;
        let mut new_nodes = Self::get_listener_missing_nodes(&received_nodes)?;
        Self::check_differences(
            &cluster,
            received_nodes,
            &mut emitter_required_changes,
            &mut new_nodes,
        );
        Self::eliminate_shutting_down_nodes(&mut new_nodes);

        use_node_meta_data(|handler| {
            handler.set_new_cluster(
                NODES_METADATA_PATH,
                &Cluster::new(Node::new_from_node(own_node), new_nodes),
            )?;
            handler.update_ranges(NODES_METADATA_PATH)
        })?;
        Self::send_required_changes(stream, emitter_required_changes)
    }

    fn send_required_changes(
        stream: &mut TcpStream,
        required_changes: Vec<Node>,
    ) -> Result<(), Errors> {
        let serialized = serialize_to_string(&required_changes)?;
        write_to_stream(stream, serialized.as_bytes())?;
        Ok(())
    }

    fn get_emitter_missing_nodes(
        received_nodes: &HashMap<NodeIp, Node>,
    ) -> Result<Vec<Node>, Errors> {
        let mut missing_nodes = Vec::new();
        let nodes_list =
            use_node_meta_data(|handler| handler.get_full_nodes_list(NODES_METADATA_PATH))?;
        for registered_node in nodes_list {
            if registered_node.state != State::ShuttingDown
                && !received_nodes.contains_key(&registered_node.get_ip())
            {
                missing_nodes.push(registered_node);
            }
        }
        Ok(missing_nodes)
    }

    fn get_listener_missing_nodes(
        received_nodes: &HashMap<NodeIp, Node>,
    ) -> Result<Vec<Node>, Errors> {
        let mut missing_nodes = Vec::new();
        let nodes_list =
            use_node_meta_data(|handler| handler.get_full_nodes_list(NODES_METADATA_PATH))?;
        for (node_ip, node) in received_nodes {
            if node.state != State::ShuttingDown
                && nodes_list.iter().find(|n| n.get_ip() == node_ip).is_none()
            {
                missing_nodes.push(Node::new_from_node(node));
            }
        }
        Ok(missing_nodes)
    }

    fn check_differences(
        cluster: &Cluster,
        received_nodes: HashMap<NodeIp, Node>,
        required_changes: &mut Vec<Node>,
        new_nodes: &mut Vec<Node>,
    ) {
        let (own_node, nodes_list) = (cluster.get_own_node(), cluster.get_other_nodes());
        for (node_ip, node) in received_nodes {
            if &node_ip != own_node.get_ip() {
                if let Some(registered_node) = nodes_list.iter().find(|n| n.get_ip() == &node_ip) {
                    match Self::needs_to_update(&registered_node, &node) {
                        1 => {
                            required_changes.push(Node::new_from_node(&registered_node));
                            new_nodes.push(Node::new_from_node(&registered_node));
                        }
                        -1 => new_nodes.push(node),
                        _ => new_nodes.push(Node::new_from_node(&registered_node)),
                    }
                }
            } else if Self::needs_to_update(own_node, &node) == -1 {
                required_changes.push(Node::new_from_node(own_node));
            }
        }
    }

    fn get_cluster() -> Result<Cluster, Errors> {
        use_node_meta_data(|handler| handler.get_cluster(NODES_METADATA_PATH))
    }

    // 1 yes (node 1 newer)
    // 0 no
    // -1 yes (node 2 newer)
    fn needs_to_update(node1: &Node, node2: &Node) -> i8 {
        if node1.get_pos() != node2.get_pos()
            || node1.get_ip() != node2.get_ip()
            || node1.state != node2.state
            || node1.is_seed != node2.is_seed
        {
            if node1.get_timestamp().is_newer_than(node2.get_timestamp()) {
                return 1;
            }
            return -1;
        }
        0
    }

    fn eliminate_shutting_down_nodes(nodes: &mut Vec<Node>) {
        nodes.retain(|node| node.state != State::ShuttingDown);
    }
}
