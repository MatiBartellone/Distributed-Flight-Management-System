use crate::data_access::data_access::DataAccess;
use crate::utils::errors::Errors;
use crate::utils::functions::get_data_access_ip;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use crate::utils::constants::DATA_ACCESS_PORT_MOD;

pub struct DataAccessHandler {}

impl DataAccessHandler {
    pub fn start_listening(ip: String, port: String) -> Result<(), Errors> {
        let data_access_port = port.parse::<i32>().map_err(|_| Errors::ServerError(String::from("Failed to parse port")))? + DATA_ACCESS_PORT_MOD;
        let listener = TcpListener::bind(format!(
            "{}:{}",
            ip,
            data_access_port
        )).map_err(|_| Errors::ServerError(String::from("Failed to set listener")))?;
        let listening_ip = format!(
            "{}:{}",
            ip,
            data_access_port
        );
        println!("Listening on {}", listening_ip);
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
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        stream
            .write_all(serialized.as_bytes())
            .map_err(|_| Errors::ServerError("Error writing to stream".to_string()))?;
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
        match stream.read(&mut [0; 1024]) {
            Ok(0) => Ok(()),
            Err(e) => Err(Errors::ServerError(format!(
                "Error reading from stream: {}",
                e
            ))),
            _ => Err(Errors::ServerError(String::from("")))
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
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;
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
