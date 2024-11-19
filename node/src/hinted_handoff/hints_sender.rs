use crate::utils::constants::HINTED_HANDOF_DATA;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::node_ip::NodeIp;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use crate::hinted_handoff::stored_query::StoredQuery;

pub struct HintsSender;

impl HintsSender {
    pub fn send_hints(ip: NodeIp) -> Result<(), Errors> {
        let hints_path = format!("{}/{}.txt", HINTED_HANDOF_DATA, ip.get_string_ip());
        if Path::new(&hints_path).exists() {
            let mut stream = TcpStream::connect(ip.get_hints_receiver_socket())
                .map_err(|_| ServerError(String::from("Error connecting to handoff.")))?;
            let file = File::open(Path::new(&hints_path))
                .map_err(|_| ServerError(String::from("Could not open file")))?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line =
                    line.map_err(|_| ServerError(String::from("Error reading from handoff.")))?;
                stream
                    .write_all(line.trim().as_bytes())
                    .map_err(|_| ServerError(String::from("Error writing to handoff.")))?;
                stream.flush().map_err(|_| ServerError(String::from("Failed to flush stream")))?;
                Self::expect_acknowledge(&mut stream)?;
                stream.flush().map_err(|_| ServerError(String::from("Failed to flush stream")))?;
            }
            stream
                .write_all(b"FINISHED")
                .map_err(|_| ServerError(String::from("Failed to write to stream")))?;
            fs::remove_file(Path::new(&hints_path))
                .map_err(|_| ServerError(String::from("Failed to remove file")))?;
        };
        Ok(())
    }

    fn expect_acknowledge(stream: &mut TcpStream) -> Result<(), Errors> {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer) {
            Ok(_n) => Ok(()),
            Err(_) => Err(ServerError(String::from("Failed to read from stream"))),
        }
    }
}
