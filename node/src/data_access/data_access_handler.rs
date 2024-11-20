use crate::data_access::data_access::DataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::{deserialize_from_slice, flush_stream, get_own_ip, serialize_to_string, start_listener, write_to_stream};
use crate::utils::node_ip::NodeIp;
use std::io::Read;
use std::net::TcpStream;

pub struct DataAccessHandler {}

impl DataAccessHandler {
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_data_access_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let data_access = DataAccess {};
        let serialized = serialize_to_string(&data_access)?;
        flush_stream(stream)?;
        write_to_stream(stream, &serialized.as_bytes())?;
        flush_stream(stream)?;
        match stream.read(&mut [0; 1024]) {
            Ok(0) => Ok(()),
            _ => Err(Errors::ServerError(String::from(""))),
        }
    }

    pub fn establish_connection() -> Result<TcpStream, Errors> {
        match TcpStream::connect(get_own_ip()?.get_data_access_socket()) {
            Ok(stream) => Ok(stream),
            Err(e) => Err(Errors::ServerError(e.to_string())),
        }
    }

    pub fn get_instance(stream: &mut TcpStream) -> Result<DataAccess, Errors> {
        let mut buf = [0; 1024];
        flush_stream(stream)?;
        match stream.read(&mut buf) {
            Ok(n) => Ok(deserialize_from_slice(&buf[..n])?),
            Err(_) => Err(Errors::ServerError(String::from(
                "Unable to read from node",
            ))),
        }
    }
}
