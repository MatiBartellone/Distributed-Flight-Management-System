use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::{deserialize_from_slice, flush_stream, get_own_ip, serialize_to_string, start_listener, write_to_stream};
use crate::utils::node_ip::NodeIp;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::net::TcpStream;

#[derive(Serialize, Deserialize)]
pub struct MetaDataHandler;

impl MetaDataHandler {
    // pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
    //     let listener = TcpListener::bind(ip.get_meta_data_access_socket())
    //         .map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
    //     for stream in listener.incoming() {
    //         match stream {
    //             Ok(mut stream) => Self::handle_connection(&mut stream)?,
    //             _ => continue,
    //         }
    //     }
    //     Ok(())
    // }
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_meta_data_access_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let meta_data_handler = MetaDataHandler {};
        let serialized = serialize_to_string(&meta_data_handler)?;
        flush_stream(stream)?;
        write_to_stream(stream, &serialized.as_bytes())?;
        flush_stream(stream)?;
        match stream.read(&mut [0; 1024]) {
            Ok(0) => Ok(()),
            _ => Err(Errors::ServerError(String::from(""))),
        }
    }

    pub fn establish_connection() -> Result<TcpStream, Errors> {
        match TcpStream::connect(get_own_ip()?.get_meta_data_access_socket()) {
            Ok(stream) => Ok(stream),
            Err(e) => Err(Errors::ServerError(e.to_string())),
        }
    }

    pub fn get_instance(stream: &mut TcpStream) -> Result<MetaDataHandler, Errors> {
        let mut buf = [0; 1024];
        flush_stream(stream)?;
        match stream.read(&mut buf) {
            Ok(n) => Ok(deserialize_from_slice(&buf[..n])?),
            Err(_) => Err(Errors::ServerError(String::from(
                "Unable to read from node",
            ))),
        }
    }

    pub fn get_client_meta_data_access(&self) -> ClientMetaDataAcces {
        ClientMetaDataAcces {}
    }
    pub fn get_keyspace_meta_data_access(&self) -> KeyspaceMetaDataAccess {
        KeyspaceMetaDataAccess {}
    }
    pub fn get_nodes_metadata_access(&self) -> NodesMetaDataAccess {
        NodesMetaDataAccess {}
    }
}
