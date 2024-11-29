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

    fn establish_connection() -> Result<TcpStream, Errors> {
        connect_to_socket(get_own_ip()?.get_meta_data_access_socket())
    }

    fn get_instance(stream: &mut TcpStream) -> Result<MetaDataHandler, Errors> {
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
pub fn use_client_meta_data<F, T>(action: F) -> Result<T, Errors>
where
    F: FnOnce(&ClientMetaDataAcces) -> Result<T, Errors>,
{
    let mut meta_data_stream = MetaDataHandler::establish_connection()?;
    let client_metadata =
        MetaDataHandler::get_instance(&mut meta_data_stream)?.get_client_meta_data_access();
    action(&client_metadata)
}

pub fn use_node_meta_data<F, T>(action: F) -> Result<T, Errors>
where
    F: FnOnce(&NodesMetaDataAccess) -> Result<T, Errors>,
{
    let mut meta_data_stream = MetaDataHandler::establish_connection()?;
    let node_metadata =
        MetaDataHandler::get_instance(&mut meta_data_stream)?.get_nodes_metadata_access();
    action(&node_metadata)
}

pub fn use_keyspace_meta_data<F, T>(action: F) -> Result<T, Errors>
where
    F: FnOnce(&KeyspaceMetaDataAccess) -> Result<T, Errors>,
{
    let mut meta_data_stream = MetaDataHandler::establish_connection()?;
    let keyspace_metadata =
        MetaDataHandler::get_instance(&mut meta_data_stream)?.get_keyspace_meta_data_access();
    action(&keyspace_metadata)
}