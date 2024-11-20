use crate::meta_data::nodes::node::Node;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    deserialize_from_slice, flush_stream, read_from_stream_no_zero, serialize_to_string,
    start_listener, use_node_meta_data, write_to_stream,
};
use crate::utils::node_ip::NodeIp;
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
        let cluster =
            use_node_meta_data(|handler| handler.get_full_nodes_list(NODES_METADATA_PATH))?;
        let serialized = serialize_to_string(&cluster)?;
        flush_stream(stream)?;
        write_to_stream(stream, serialized.as_bytes())
    }

    fn get_new_node(stream: &mut TcpStream) -> Result<Node, Errors> {
        let buf = read_from_stream_no_zero(stream)?;
        deserialize_from_slice(buf.as_slice())
    }

    fn set_new_node(new_node: Node) -> Result<(), Errors> {
        use_node_meta_data(|node_metadata| {
            let cluster = node_metadata.get_cluster(NODES_METADATA_PATH)?;
            for node in cluster.get_other_nodes().iter() {
                if node.get_ip() == new_node.get_ip() {
                    node_metadata.set_booting(NODES_METADATA_PATH, new_node.get_ip())?;
                    return Ok(());
                }
            }
            node_metadata.append_new_node(NODES_METADATA_PATH, new_node)
        })
    }
}
