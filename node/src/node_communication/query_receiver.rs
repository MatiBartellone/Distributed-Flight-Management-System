use crate::node_communication::query_serializer::QuerySerializer;
use crate::utils::constants::QUERY_DELEGATION_PORT;
use crate::utils::errors::Errors;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use crate::utils::functions::get_own_ip;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening() -> Result<(), Errors> {
        let ip = get_own_ip()?;
        let listener = TcpListener::bind(format!(
            "{}:{}",
            ip,
            QUERY_DELEGATION_PORT
        ))
        .map_err(|_| Errors::ServerError(String::from("Can't bind the port")))?;
        let listening_ip = format!(
            "{}:{}",
            ip,
            QUERY_DELEGATION_PORT
        );
        println!("QUERY RECEIVER Start listening on {}", listening_ip);
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
            match query.run() {
                Ok(result) => Ok(result),
                Err(e) => Ok(e.to_string().as_bytes().to_vec()),
            }
        }
        Err(_) => Err(Errors::ServerError(String::from(""))),
    }
}

fn respond_to_request(mut stream: TcpStream, response: Vec<u8>) {
    stream.flush().expect("sds");
    stream.write_all(response.as_slice()).unwrap();
    stream.flush().expect("sds");
}
