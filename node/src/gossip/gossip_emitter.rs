use crate::meta_data::meta_data_handler::use_node_meta_data;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::State::{Booting, Recovering};
use crate::meta_data::nodes::node::{Node, State};
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    deserialize_from_slice, read_exact_from_stream, serialize_to_string, write_to_stream,
};
use crate::utils::types::node_ip::NodeIp;
use rand::seq::SliceRandom;
use std::net::TcpStream;

pub struct GossipEmitter;

impl GossipEmitter {
    /// Starts Gossip process
    /// It connects to a rando ip from the cluster and exchanges information from node metadata
    pub fn start_gossip() -> Result<bool, Errors> {
        let Some(ip) = Self::get_random_ip()? else {
            return Ok(false);
        };
        if let Ok(mut stream) = TcpStream::connect(ip.get_gossip_socket()) {
            Self::send_nodes_list(&mut stream)?;
            let node_added_or_removed = Self::get_nodes_list(&mut stream)?;
            if node_added_or_removed {
                use_node_meta_data(|handler| handler.update_ranges(NODES_METADATA_PATH))?;
            }
            Ok(node_added_or_removed)
        } else {
            Self::set_inactive(ip)?;
            Ok(false)
        }
    }

    /// Retrieves a random node IP from the cluster, excluding `Booting` or `Recovering` nodes.
    fn get_random_ip() -> Result<Option<NodeIp>, Errors> {
        use_node_meta_data(|node_meta_data| {
            if node_meta_data.get_nodes_quantity(NODES_METADATA_PATH)? == 1 {
                return Ok(None);
            }
            let mut rng = rand::thread_rng();
            let cluster = node_meta_data.get_cluster(NODES_METADATA_PATH)?;
            let nodes = cluster.get_other_nodes();
            if let Some(random_node) = nodes.choose(&mut rng) {
                if random_node.state != Booting
                    && random_node.state != Recovering
                    && random_node.state != State::ShuttingDown
                {
                    return Ok(Some(NodeIp::new_from_ip(random_node.get_ip())));
                }
            }
            Ok(None)
        })
    }

    fn set_inactive(ip: NodeIp) -> Result<(), Errors> {
        use_node_meta_data(|handler| handler.set_inactive(NODES_METADATA_PATH, &ip))
    }

    fn send_nodes_list(stream: &mut TcpStream) -> Result<(), Errors> {
        let nodes_list =
            use_node_meta_data(|handler| handler.get_full_nodes_list(NODES_METADATA_PATH))?;
        let serialized = serialize_to_string(&nodes_list)?;
        write_to_stream(stream, serialized.as_bytes())
    }

    fn get_nodes_list(stream: &mut TcpStream) -> Result<bool, Errors> {
        let buf = read_exact_from_stream(stream)?;
        let received_nodes: Vec<Node> = deserialize_from_slice(buf.as_slice())?;
        let cluster = use_node_meta_data(|handler| handler.get_cluster(NODES_METADATA_PATH))?;
        let mut node_added = false;
        for node in &received_nodes {
            if !Self::is_node_in_list(cluster.get_other_nodes(), node) {
                node_added = true;
            }
        }
        let (new_cluster, node_removed) = Self::get_new_cluster(&cluster, received_nodes)?;
        use_node_meta_data(|handler| handler.set_new_cluster(NODES_METADATA_PATH, &new_cluster))?;
        Ok(node_added || node_removed)
    }

    fn get_new_cluster(
        cluster: &Cluster,
        received_nodes: Vec<Node>,
    ) -> Result<(Cluster, bool), Errors> {
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
        let node_removed = Self::shutting_down_count(&new_list)
            > Self::shutting_down_count(cluster.get_other_nodes());
        Ok((
            Cluster::new(Node::new_from_node(cluster.get_own_node()), new_list),
            node_removed,
        ))
    }

    fn is_node_in_list(node_list: &[Node], node: &Node) -> bool {
        for n in node_list.iter() {
            if n.get_ip().get_string_ip() == node.get_ip().get_string_ip() {
                return true;
            }
        }
        false
    }

    fn shutting_down_count(new_nodes: &[Node]) -> usize {
        new_nodes
            .iter()
            .filter(|n| n.state == State::ShuttingDown)
            .count()
    }
}
