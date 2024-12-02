use crate::query_delegation::query_serializer::QuerySerializer;
use crate::utils::errors::Errors;
use crate::utils::functions::{bind_listener, read_exact_from_stream, write_to_stream};
use crate::utils::types::node_ip::NodeIp;
use std::net::TcpStream;
use std::thread;

pub struct QueryReceiver {}

impl QueryReceiver {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        let listener = bind_listener(ip.get_query_delegation_socket())?;
        for incoming in listener.incoming() {
            match incoming {
                Ok(mut stream) => {
                    thread::spawn(move || -> Result<(), Errors> {
                        match handle_query(&mut stream) {
                            Ok(response) => write_to_stream(&mut stream, response.as_slice())?,
                            Err(e) => write_to_stream(&mut stream, e.serialize().as_slice())?,
                        }
                        Ok(())
                    });
                }
                Err(_) => return Err(Errors::ServerError(String::from("Error in connection"))),
            }
        }
        Ok(())
    }
}
fn handle_query(stream: &mut TcpStream) -> Result<Vec<u8>, Errors> {
    let res = read_exact_from_stream(stream)?;
    let query = QuerySerializer::deserialize(res.as_slice())?;
    query.run()
}
