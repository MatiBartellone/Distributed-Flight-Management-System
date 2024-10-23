use crate::data_access::data_access::DataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::get_data_access_ip;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

pub struct DataAccessHandler {}

impl DataAccessHandler {
    pub fn start_listening() -> Result<(), Errors> {
        let listener = TcpListener::bind(get_data_access_ip()?)
            .map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => Self::handle_connection(&mut stream)?,
                Err(_) => {
                    return Err(Errors::ServerError(String::from(
                        "Failed to connect to data access handler",
                    )))
                }
            }
        }
        Ok(())
    }

    fn handle_connection(stream: &mut TcpStream) -> Result<(), Errors> {
        let data_access = DataAccess {};
        let serialized = serde_json::to_string(&data_access)
            .map_err(|_| Errors::ServerError("Failed to serialize data access".to_string()))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| Errors::ServerError("Error writing to stream".to_string()))?;
        match stream.read_exact(&mut [0; 1024]) {
            Ok(_) => Ok(()),
            Err(e) => Err(Errors::ServerError(format!(
                "Error reading from stream: {}",
                e
            ))),
        }
    }

    pub fn establish_connection() -> Result<TcpStream, Errors> {
        match TcpStream::connect(get_data_access_ip()?) {
            Ok(stream) => Ok(stream),
            Err(e) => Err(Errors::ServerError(e.to_string())),
        }
    }

    pub fn get_instance(stream: &mut TcpStream) -> Result<DataAccess, Errors> {
        let mut buf = [0; 1024];
        match stream.read(&mut buf) {
            Ok(n) => Ok(serde_json::from_slice(&buf[..n]).map_err(|_| {
                Errors::ServerError(String::from("Failed to deserialize data access"))
            })?),
            Err(_) => Err(Errors::ServerError(String::from(
                "Unable to read from node",
            ))),
        }
    }
}
