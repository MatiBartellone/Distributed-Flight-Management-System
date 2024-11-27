use rustls::{ServerConnection, StreamOwned};

use crate::query_delegation::query_serializer::QuerySerializer;
use crate::utils::errors::Errors;
use crate::utils::functions::bind_listener;
use crate::utils::tls_stream::{
    create_server_config, flush_stream, get_stream_owned, read_exact_from_stream, write_to_stream
};
use crate::utils::types::node_ip::NodeIp;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        let listener = bind_listener(ip.get_query_delegation_socket())?;
        let config = create_server_config()?;
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    let mut stream = get_stream_owned(stream, Arc::new(config.clone()))?; 
                    thread::spawn(move || -> Result<(), Errors> {
                        if let Ok(response) = handle_query(&mut stream) {
                            respond_to_request(&mut stream, response)?;
                        } else {
                            respond_to_request(&mut stream, "error".as_bytes().to_vec())?;
                        };
                        Ok(())
                    });
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error in connection"))),
            }
        }
        Ok(())
    }
}
fn handle_query(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<Vec<u8>, Errors> {
    let res = read_exact_from_stream(stream)?;
    stream.flush().expect("sds");
    let query = QuerySerializer::deserialize(res.as_slice())?;
    match query.run() {
        Ok(result) => Ok(result),
        Err(e) => Ok(e.to_string().as_bytes().to_vec()),
    }
}

fn respond_to_request(stream: &mut StreamOwned<ServerConnection, TcpStream>, response: Vec<u8>) -> Result<(), Errors> {
    flush_stream(stream)?;
    write_to_stream(stream, response.as_slice())?;
    flush_stream(stream)
}
