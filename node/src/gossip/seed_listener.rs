use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::node::Node;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::start_listener;
use crate::utils::node_ip::NodeIp;
use std::io::{Read, Write};
use std::net::TcpStream;

pub struct SeedListener;

impl SeedListener {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_seed_listener_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        Self::send_nodes_list(stream)?;
        let new_node = Self::get_new_node(stream)?;
        Self::set_new_node(new_node)
    }

    fn send_nodes_list(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut meta_data_stream = MetaDataHandler::establish_connection()?;
        let node_metadata =
            MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
        let cluster = node_metadata.get_full_nodes_list(NODES_METADATA_PATH)?;
        let serialized = serde_json::to_string(&cluster)
            .map_err(|_| Errors::ServerError("Failed to serialize data access".to_string()))?;
        stream
            .flush()
            .map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| Errors::ServerError("Error writing to stream".to_string()))?;
        Ok(())
    }

    fn get_new_node(stream: &mut TcpStream) -> Result<Node, Errors> {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(n) => Ok(serde_json::from_slice(&buffer[..n]).expect("Failed to deserialize json")),
            _ => Err(Errors::ServerError(String::from(
                "Error reading from stream",
            ))),
        }
    }

    fn set_new_node(new_node: Node) -> Result<(), Errors> {
        let mut meta_data_stream = MetaDataHandler::establish_connection()?;
        let node_metadata =
            MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
        let cluster = node_metadata.get_cluster(NODES_METADATA_PATH)?;
        for node in cluster.get_other_nodes().iter() {
            if node.get_ip() == new_node.get_ip() {
                node_metadata.set_booting(NODES_METADATA_PATH, new_node.get_ip())?;
                return Ok(());
            }
        }
        node_metadata.append_new_node(NODES_METADATA_PATH, new_node)?;
        Ok(())
    }
}
