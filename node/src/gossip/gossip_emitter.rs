use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::cluster::Cluster;
use crate::meta_data::nodes::node::Node;
use crate::utils::constants::NODES_METADATA;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::generate_random_number;
use crate::utils::node_ip::NodeIp;
use std::io::{Read, Write};
use std::net::TcpStream;

pub struct GossipEmitter;

impl GossipEmitter {
    pub fn start_gossip() -> Result<(), Errors> {
        let Some(ip) = Self::get_random_ip()? else {
            return Ok(());
        };

        if let Ok(mut stream) = TcpStream::connect(ip.get_gossip_socket()) {
            Self::send_nodes_list(&mut stream)?;
            Self::get_nodes_list(&mut stream)
        } else {
            Self::set_inactive(ip)
        }
    }

    fn get_random_ip() -> Result<Option<NodeIp>, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        if node_meta_data.get_nodes_quantity(NODES_METADATA)? == 1 {
            return Ok(None);
        }
        let node_number =
            generate_random_number(node_meta_data.get_nodes_quantity(NODES_METADATA)? as u64, 1)?
                as usize;
        for node in node_meta_data
            .get_cluster(NODES_METADATA)?
            .get_other_nodes()
        {
            if node.get_pos() == node_number {
                return Ok(Some(NodeIp::new_from_ip(node.get_ip())));
            }
        }
        Err(ServerError(String::from("Could not pick node")))
    }

    fn set_inactive(ip: NodeIp) -> Result<(), Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        node_meta_data.set_inactive(NODES_METADATA, &ip)
    }

    fn send_nodes_list(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut meta_data_stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
        let serialized =
            serde_json::to_string(&node_meta_data.get_full_nodes_list(NODES_METADATA)?)
                .map_err(|_| ServerError(String::from("Error serializing nodes list")))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| ServerError(String::from("Error sending nodes list")))?;
        Ok(())
    }

    fn get_nodes_list(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut buffer = [0; 1024];
        let size = stream
            .read(&mut buffer)
            .map_err(|_| Errors::ServerError(String::from("Failed to read data")))?;
        let received_nodes: Vec<Node> =
            serde_json::from_slice(&buffer[..size]).expect("Failed to deserialize json");
        let new_cluster = Self::get_new_cluster(received_nodes)?;

        let mut stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        node_meta_data.set_new_cluster(NODES_METADATA, &new_cluster)?;
        Ok(())
    }

    fn get_new_cluster(received_nodes: Vec<Node>) -> Result<Cluster, Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let node_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        let cluster = node_meta_data.get_cluster(NODES_METADATA)?;
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
