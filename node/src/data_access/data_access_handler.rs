use crate::data_access::data_access::DataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::{
    connect_to_socket, deserialize_from_slice, get_own_ip, read_exact_from_stream,
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
        write_to_stream(stream, serialized.as_bytes())?;
        read_exact_from_stream(stream)?;
        Ok(())
    }

    /// connects to the data access socket
    fn establish_connection() -> Result<TcpStream, Errors> {
        connect_to_socket(get_own_ip()?.get_data_access_socket())
    }

    /// given a data access TcpStream, returns a DataAccess instance to use its functionalities
    fn get_instance(stream: &mut TcpStream) -> Result<DataAccess, Errors> {
        deserialize_from_slice(read_from_stream_no_zero(stream)?.as_slice())
    }
}

/// use_data_access is the way to access data_access functions.
///
/// It represents a lock, in which the DataAccess instance ends its lifetime when the function finishes.
/// action is a function that uses the data access
///
/// ```ignore
/// use node::data_access::data_access_handler::use_data_access;
/// use_data_access(|data_access| {
///     data_access.drop_table(String::from("kp1.table1"))
/// })
/// ```
pub fn use_data_access<F, T>(action: F) -> Result<T, Errors>
where
    F: FnOnce(&DataAccess) -> Result<T, Errors>,
{
    let mut meta_data_stream = DataAccessHandler::establish_connection()?;
    let data_access = DataAccessHandler::get_instance(&mut meta_data_stream)?;
    action(&data_access)
}
