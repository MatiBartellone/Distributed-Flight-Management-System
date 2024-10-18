use crate::node_communication::query_serializer::QuerySerializer;
use crate::utils::errors::Errors;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use crate::meta_data::nodes::node_meta_data_acces::NodesMetaDataAccess;
use crate::utils::constants::NODES_METADATA;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening() -> Result<(), Errors> {

        let listener = TcpListener::bind(format!("{}:{}", NodesMetaDataAccess::get_own_ip(NODES_METADATA)?, 9090)).unwrap();
        println!(
            "Start listening on {}",
            format!("{}:{}", NodesMetaDataAccess::get_own_ip(NODES_METADATA)?, 9090)
        );
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    println!("Incoming connection from {}", stream.peer_addr().unwrap());
                    thread::spawn(move || {
                        if let Ok(response) = handle_query(&stream) {
                            respond_to_request(stream, response);
                        } else {
                            respond_to_request(stream, "error".as_bytes().to_vec());
                        };
                    });
                }
                Err(_) => {}
            }
        }
        Ok(())
    }
}
fn handle_query(mut stream: &TcpStream) -> Result<Vec<u8>, Errors> {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(n) => {
            stream.flush().expect("sds");
            let query = QuerySerializer::deserialize(&buffer[..n])?;
            let response = query.run()?;
            println!("response: {:?}", response);
            Ok(response)
        }
        Err(_) => Err(Errors::ServerError(String::from(""))),
    }
}

fn respond_to_request(mut stream: TcpStream, response: Vec<u8>) {
    stream.flush().expect("sds");
    stream.write_all(response.as_slice()).unwrap();
    stream.flush().expect("sds");
}
