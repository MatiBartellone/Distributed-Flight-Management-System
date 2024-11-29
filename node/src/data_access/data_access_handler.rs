use crate::data_access::data_access::DataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    connect_to_socket, deserialize_from_slice, flush_stream, get_own_ip, read_exact_from_stream,
    read_from_stream_no_zero, serialize_to_string, start_listener, write_to_stream,
};
use crate::utils::types::node_ip::NodeIp;
use std::net::TcpStream;

pub struct DataAccessHandler {}

impl DataAccessHandler {
    /// binds the data access TcpListener
    pub fn start_listening(ip: NodeIp) -> Result<(), Errors> {
        start_listener(ip.get_data_access_socket(), Self::handle_connection)
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let data_access = DataAccess {};
        let serialized = serialize_to_string(&data_access)?;
        flush_stream(stream)?;
        write_to_stream(stream, serialized.as_bytes())?;
        flush_stream(stream)?;
        read_exact_from_stream(stream)?;
        Ok(())
    }

    /// connects to the data access socket
    pub fn establish_connection() -> Result<TcpStream, Errors> {
        connect_to_socket(get_own_ip()?.get_data_access_socket())
    }

    /// given a data access TcpStream, returns a DataAccess instance to use its functionalities
    pub fn get_instance(stream: &mut TcpStream) -> Result<DataAccess, Errors> {
        flush_stream(stream)?;
        deserialize_from_slice(read_from_stream_no_zero(stream)?.as_slice())
    }
}
