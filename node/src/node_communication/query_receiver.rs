use crate::executables::query_executable::Node;
use crate::node_communication::query_serializer::QuerySerializer;
use crate::queries::query::Query;
use crate::utils::errors::Errors;
use std::fmt::format;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening() {
        let listener = TcpListener::bind(format!("{}:{}", get_own_ip(), 9090)).unwrap();
        println!(
            "Start listening on {}",
            format!("{}:{}", get_own_ip(), 9090)
        );
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    println!("Incoming connection from {}", stream.peer_addr().unwrap());
                    thread::spawn(move || {
                        if let Ok(response) = handle_query(&stream) {
                            respond_to_request(stream, response);
                        } else {
                            respond_to_request(stream, String::from("error"));
                        };
                    });
                }
                Err(_) => {}
            }
        }
    }
}
fn handle_query(mut stream: &TcpStream) -> Result<String, Errors> {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(_) => {
            stream.flush().expect("sds");
            let query = QuerySerializer::deserialize(&buffer)?;
            let response = query.run()?;
            println!("response: {:?}", response);
            Ok(response)
        }
        Err(_) => Err(Errors::ServerError(String::from(""))),
    }
}

fn respond_to_request(mut stream: TcpStream, response: String) {
    stream.flush().expect("sds");
    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().expect("sds");
}
