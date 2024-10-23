use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::node_communication::query_serializer::QuerySerializer;
use crate::utils::constants::{nodes_meta_data_path, QUERY_DELEGATION_PORT};
use crate::utils::errors::Errors;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening() -> Result<(), Errors> {
        let mut stream = MetaDataHandler::establish_connection()?;
        let nodes_meta_data =
            MetaDataHandler::get_instance(&mut stream)?.get_nodes_metadata_access();
        let listener = TcpListener::bind(format!(
            "{}:{}",
            nodes_meta_data.get_own_ip(nodes_meta_data_path().as_ref())?,
            QUERY_DELEGATION_PORT
        ))
        .unwrap();
        let listening_ip = format!(
            "{}:{}",
            nodes_meta_data.get_own_ip(nodes_meta_data_path().as_ref())?,
            QUERY_DELEGATION_PORT
        );
        println!("Start listening on {}", listening_ip);
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
                Err(_) => return Err(Errors::ServerError(String::from("Error in connection"))),
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
