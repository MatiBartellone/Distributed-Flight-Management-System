use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::meta_data::nodes::node::Node;
use crate::utils::constants::{NODES_METADATA, SEED_LISTENER_MOD};
use crate::utils::errors::Errors;

pub struct SeedListener;

impl SeedListener {
    pub fn start_listening(ip: String, port: String) -> Result<(), Errors> {
        let seed_listener_port = port
            .parse::<i32>()
            .map_err(|_| Errors::ServerError(String::from("Failed to parse port")))?
            + SEED_LISTENER_MOD;
        let listener = TcpListener::bind(format!("{}:{}", ip, seed_listener_port))
            .map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => Self::handle_connection(&mut stream)?,
                Err(_) => {
                    return Err(Errors::ServerError(String::from(
                        "Failed to connect to data access handler",
                    )))
                }
            }
        }
        Ok(())
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut buffer = [0; 1024];
        let new_node;
        match stream.read(&mut buffer) {
            Ok(n) => {
                new_node = serde_json::from_slice(&buffer[..n]).expect("Failed to deserialize json");
            },
            _ => return Err(Errors::ServerError(String::from("Error reading from stream"))),
        }
        Self::send_nodes_list(stream, new_node)?;
        Ok(())
    }

    fn send_nodes_list(stream: &mut TcpStream, new_node: Node) -> Result<(), Errors> {
        let mut meta_data_stream = MetaDataHandler::establish_connection()?;
        let node_metadata = MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
        let cluster = node_metadata.get_full_nodes_list(NODES_METADATA)?;
        let serialized = serde_json::to_string(&cluster)
            .map_err(|_| Errors::ServerError("Failed to serialize data access".to_string()))?;
        stream
            .flush()
            .map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| Errors::ServerError("Error writing to stream".to_string()))?;
        node_metadata.append_new_node(NODES_METADATA, new_node)?;
        Ok(())
    }
}