use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use crate::frame::Frame;
use crate::node_communication::query_serializer::QuerySerializer;
use crate::response_builders::error_builder::ErrorBuilder;
use crate::utils::errors::Errors;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening(&self) {
        let listener = TcpListener::bind(""/*get_own_node_comms_ip():NODES_PORT*/).unwrap();

        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    thread::spawn(move || {
                        if let Ok(response) = self.handle_query(&stream) {
                            self.respond_to_request(stream, response);
                        } else {
                            self.respond_to_request(stream, String::from("error"));
                        };
                    });
                },
                Err(e) => {}
            }
        }
    }

    fn handle_query(&self, mut stream: &TcpStream) -> Result<String, Errors> {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let query = QuerySerializer::deserialize(&buffer);
                let response = query.run()?;
                Ok(response)
            }
            Err(e) => {
                Err(Errors::ServerError(String::from("")))
            }
        }
    }

    fn respond_to_request(&self, mut stream: TcpStream, response: String) {
        stream.write(response.as_bytes()).unwrap();
    }
}