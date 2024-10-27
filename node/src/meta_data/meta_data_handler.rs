use crate::meta_data::clients::meta_data_client::ClientMetaDataAcces;
use crate::meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::get_meta_data_handler_ip;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use crate::utils::constants::META_DATA_ACCESS_MOD;

#[derive(Serialize, Deserialize)]
pub struct MetaDataHandler;

impl MetaDataHandler {
    pub fn start_listening(ip: String, port: String) -> Result<(), Errors> {
        let meta_data_port = port.parse::<i32>().map_err(|_| Errors::ServerError(String::from("Failed to parse port")))? + META_DATA_ACCESS_MOD;
        let listener = TcpListener::bind(format!(
            "{}:{}",
            ip,
            meta_data_port
        )).map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => Self::handle_connection(&mut stream)?,
                _ => {continue}
            }
        }
        Ok(())
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let meta_data_handler = MetaDataHandler {};
        let serialized = serde_json::to_string(&meta_data_handler).map_err(|_| {
            Errors::ServerError("Failed to serialize meta data handler".to_string())
        })?;
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| Errors::ServerError("Error writing to stream".to_string()))?;
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        match stream.read(&mut [0; 1024]) {
            Ok(0) => Ok(()),
            Err(e) => Err(Errors::ServerError(format!(
                    "Error reading from stream: {}",
                    e
            ))),
            _ => Err(Errors::ServerError(String::from("")))
        }
    }

    pub fn establish_connection() -> Result<TcpStream, Errors> {
        match TcpStream::connect(get_meta_data_handler_ip()?) {
            Ok(stream) => Ok(stream),
            Err(e) => Err(Errors::ServerError(e.to_string())),
        }
    }

    pub fn get_instance(stream: &mut TcpStream) -> Result<MetaDataHandler, Errors> {
        let mut buf = [0; 1024];
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        match stream.read(&mut buf) {
            Ok(n) => Ok(serde_json::from_slice(&buf[..n]).map_err(|_| {
                Errors::ServerError(String::from("Failed to deserialize meta data handler"))
            })?),
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
