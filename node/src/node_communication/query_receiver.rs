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
        let listener = TcpListener::bind(""/*get_own_node_comms_ip()*/).unwrap();

        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    thread::spawn(move || {
                        self.handle_query(stream);
                    });
                },
                Err(e) => {}
            }
        }
    }

    fn handle_query(&self, mut stream: TcpStream) {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let query = QuerySerializer::deserialize(&buffer);
                query.run()
            }
            Err(e) => {
                Err(Errors::ServerError(String::from("")))
            }
        }
    }
}