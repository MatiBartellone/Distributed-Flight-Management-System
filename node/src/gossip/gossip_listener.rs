use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::Node;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{deserialize_from_slice, serialize_to_string, start_listener, write_to_stream};
use crate::utils::node_ip::NodeIp;
use std::io::{Read, Write};
use std::net::TcpStream;

pub struct GossipListener;

impl GossipListener {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_gossip_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut buffer = [0; 1024];
        let size = stream
            .read(&mut buffer)
            .map_err(|_| Errors::ServerError(String::from("Failed to read data")))?;
        let received_nodes: Vec<Node> = deserialize_from_slice(&buffer[..size])?;
        let cluster = Self::get_cluster()?;
        let own_node = cluster.get_own_node();
        let (mut new_nodes, mut required_changes) = (Vec::new(), Vec::new());

        Self::check_missing_nodes(&cluster, &received_nodes, &mut required_changes);

        Self::check_differences(
            &cluster,
            received_nodes,
            &mut required_changes,
            &mut new_nodes,
        );
        {
            let mut meta_data_stream = MetaDataHandler::establish_connection()?;
            let node_meta_data =
                MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
            node_meta_data.set_new_cluster(
                NODES_METADATA_PATH,
                &Cluster::new(Node::new_from_node(own_node), new_nodes),
            )?;
        }
        Self::send_required_changes(stream, required_changes)
    }

    fn send_required_changes(
        stream: &mut TcpStream,
        required_changes: Vec<Node>,
    ) -> Result<(), Errors> {
        let serialized = serialize_to_string(&required_changes)?;
        write_to_stream(stream, &serialized.as_bytes())?;
        Ok(())
    }

    fn check_missing_nodes(
        cluster: &Cluster,
        received_nodes: &Vec<Node>,
        required_changes: &mut Vec<Node>,
    ) {
        let (own_node, nodes_list) = (cluster.get_own_node(), cluster.get_other_nodes());
        for registered_node in nodes_list {
            if Self::get_node(registered_node, received_nodes).is_none() {
                required_changes.push(Node::new_from_node(registered_node));
            }
        }
        if Self::get_node(own_node, received_nodes).is_none() {
            required_changes.push(Node::new_from_node(own_node));
        }
    }

    fn check_differences(
        cluster: &Cluster,
        received_nodes: Vec<Node>,
        required_changes: &mut Vec<Node>,
        new_nodes: &mut Vec<Node>,
    ) {
        let (own_node, nodes_list) = (cluster.get_own_node(), cluster.get_other_nodes());
        for received_node in received_nodes {
            if received_node.get_pos() != own_node.get_pos() {
                match Self::get_node(&received_node, nodes_list) {
                    Some(registered_node) => {
                        match Self::needs_to_update(&registered_node, &received_node) {
                            1 => {
                                required_changes.push(Node::new_from_node(&registered_node));
                                new_nodes.push(Node::new_from_node(&registered_node));
                            }
                            -1 => new_nodes.push(received_node),
                            _ => new_nodes.push(registered_node),
                        }
                    }
                    None => new_nodes.push(received_node),
                }
            } else if Self::needs_to_update(own_node, &received_node) == -1 {
                required_changes.push(Node::new_from_node(own_node));
            }
        }
    }

    fn get_cluster() -> Result<Cluster, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        node_meta_data.get_cluster(NODES_METADATA_PATH)
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
            if node1.get_timestamp() > node2.get_timestamp() {
                return 1;
            }
            return -1;
        }
        0
    }

    fn get_node(node: &Node, nodes_list: &Vec<Node>) -> Option<Node> {
        for n in nodes_list {
            if n.get_pos() == node.get_pos() {
                return Some(Node::new_from_node(n));
            }
        }
        None
    }
}
