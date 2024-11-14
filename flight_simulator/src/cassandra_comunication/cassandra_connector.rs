use std::io::{Read, Write};
use std::net::TcpStream;

use crate::utils::frame::Frame;

pub struct CassandraConnection {
    stream: TcpStream,
}

impl CassandraConnection {
    pub fn new(node: &str) -> Result<Self, String> {
        match TcpStream::connect(node) {
            Ok(stream) => Ok(Self { stream }),
            Err(e) => Err(format!("Failed to connect to node {}: {}", node, e)),
        }
    }

    pub fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.stream.write_all(frame).map_err(|_| "Error al escribir".to_string())?;
        self.stream.flush().map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    pub fn read_frame_response(&mut self) -> Result<Frame, String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => Frame::parse_frame(&buf[..n]),
            _ => Err("Fail reading the response".to_string()),
        }
    }
}