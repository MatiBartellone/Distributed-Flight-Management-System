use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    connect_to_socket, deserialize_from_slice, flush_stream, get_own_ip, read_exact_from_stream,
    read_from_stream_no_zero, serialize_to_string, start_listener, write_to_stream,
};
use crate::utils::types::node_ip::NodeIp;
use serde::{Deserialize, Serialize};
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
        write_to_stream(stream, serialized.as_bytes())?;
        flush_stream(stream)?;
        read_exact_from_stream(stream)?;
        Ok(())
    }

    pub fn establish_connection() -> Result<TcpStream, Errors> {
        connect_to_socket(get_own_ip()?.get_meta_data_access_socket())
    }

    pub fn get_instance(stream: &mut TcpStream) -> Result<MetaDataHandler, Errors> {
        flush_stream(stream)?;
        deserialize_from_slice(read_from_stream_no_zero(stream)?.as_slice())
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
