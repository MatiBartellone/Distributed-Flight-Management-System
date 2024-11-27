use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::Node;
use crate::meta_data::nodes::node::State::Booting;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{deserialize_from_slice, serialize_to_string};
use crate::utils::types::node_ip::NodeIp;
use crate::utils::tls_stream::{connect_to_socket, read_exact_from_stream, use_node_meta_data, write_to_stream};
use rand::seq::SliceRandom;
use rustls::{ServerConnection, StreamOwned};
use std::net::TcpStream;

pub struct GossipEmitter;

impl GossipEmitter {
    pub fn start_gossip() -> Result<(), Errors> {
        let Some(ip) = Self::get_random_ip()? else {
            return Ok(());
        };
        if let Ok(mut stream) = connect_to_socket(ip.get_gossip_socket()) {
            Self::send_nodes_list(&mut stream)?;
            Self::get_nodes_list(&mut stream)
        } else {
            Self::set_inactive(ip)
        }
    }

    fn get_random_ip() -> Result<Option<NodeIp>, Errors> {
        use_node_meta_data(|node_meta_data| {
            if node_meta_data.get_nodes_quantity(NODES_METADATA_PATH)? == 1 {
                return Ok(None);
            }
            let mut rng = rand::thread_rng();
            let cluster = node_meta_data.get_cluster(NODES_METADATA_PATH)?;
            let nodes = cluster.get_other_nodes();
            if let Some(random_node) = nodes.choose(&mut rng) {
                if random_node.state != Booting {
                    return Ok(Some(NodeIp::new_from_ip(random_node.get_ip())));
                }
            }
            Ok(None)
        })
    }

    fn set_inactive(ip: NodeIp) -> Result<(), Errors> {
        use_node_meta_data(|handler| handler.set_inactive(NODES_METADATA_PATH, &ip))
    }

    fn send_nodes_list(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<(), Errors> {
        let nodes_list =
            use_node_meta_data(|handler| handler.get_full_nodes_list(NODES_METADATA_PATH))?;
        let serialized = serialize_to_string(&nodes_list)?;
        write_to_stream(stream, serialized.as_bytes())
    }

    fn get_nodes_list(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<(), Errors> {
        let buf = read_exact_from_stream(stream)?;
        let received_nodes: Vec<Node> = deserialize_from_slice(buf.as_slice())?;
        let new_cluster = Self::get_new_cluster(received_nodes)?;
        use_node_meta_data(|handler| handler.set_new_cluster(NODES_METADATA_PATH, &new_cluster))
    }

    fn get_new_cluster(received_nodes: Vec<Node>) -> Result<Cluster, Errors> {
        let cluster = use_node_meta_data(|handler| handler.get_cluster(NODES_METADATA_PATH))?;
        let mut new_list = Vec::new();
        for node in received_nodes {
            if node.get_pos() != cluster.get_own_node().get_pos() {
                new_list.push(node);
            }
        }
        for node in cluster.get_other_nodes() {
            if !Self::is_node_in_list(&new_list, node) {
                new_list.push(Node::new_from_node(node));
            }
        }
        Ok(Cluster::new(
            Node::new_from_node(cluster.get_own_node()),
            new_list,
        ))
    }

    fn is_node_in_list(node_list: &[Node], node: &Node) -> bool {
        for n in node_list.iter() {
            if n.get_pos() == node.get_pos() {
                return true;
            }
        }
        false
    }
}
