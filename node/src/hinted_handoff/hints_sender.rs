use rustls::{ServerConnection, StreamOwned};

use crate::utils::constants::HINTED_HANDOFF_DATA;
use crate::utils::errors::Errors;
use crate::utils::errors::Errors::ServerError;
use crate::utils::tls_stream::{
    connect_to_socket, flush_stream, read_from_stream_no_zero, write_to_stream,
};
use crate::utils::node_ip::NodeIp;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::Path;

pub struct HintsSender;

impl HintsSender {
    pub fn send_hints(ip: NodeIp) -> Result<(), Errors> {
        let hints_path = format!("{}/{}.txt", HINTED_HANDOFF_DATA, ip.get_string_ip());
        if Path::new(&hints_path).exists() {
            let mut stream = connect_to_socket(ip.get_hints_receiver_socket())?;
            let file = File::open(Path::new(&hints_path))
                .map_err(|_| ServerError(String::from("Could not open file")))?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line =
                    line.map_err(|_| ServerError(String::from("Error reading from handoff.")))?;
                write_to_stream(&mut stream, line.trim().as_bytes())?;
                flush_stream(&mut stream)?;
                Self::expect_acknowledge(&mut stream)?;
                flush_stream(&mut stream)?;
            }
            write_to_stream(&mut stream, b"FINISHED")?;
            fs::remove_file(Path::new(&hints_path))
                .map_err(|_| ServerError(String::from("Failed to remove file")))?;
        };
        Ok(())
    }

    fn expect_acknowledge(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<(), Errors> {
        read_from_stream_no_zero(stream)?;
        Ok(())
    }
}
