use std::io::{Read, Write};
use std::net::TcpStream;

use crate::utils::frame::Frame;
pub struct CassandraConnection {
    stream: TcpStream
}

impl CassandraConnection {
    /// Create a new connection to the server with the given node
    pub fn new(node: &str) -> Result<Self, String> {
        match TcpStream::connect(node) {
            Ok(stream) => Ok(Self { stream }),
            Err(e) => Err(format!("Failed to connect to node {}: {}", node, e)),
        }
    }

    fn write_stream(&mut self, frame: &Frame) -> Result<(), String> {
        let frame = frame.to_bytes()
            .map_err(|_| "Error al convertir a bytes".to_string())?;
        // Encrypt the frame

        self.stream.write_all(&frame)
            .map_err(|_| "Error al escribir".to_string())?;
        self.stream.flush()
            .map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    fn read_stream(&mut self) -> Result<Frame, String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                // Desencrypt the frame
                let desencrypted_bytes = &buf[..n];
                Frame::parse_frame(&desencrypted_bytes)
            }
            _ => Err("Fail reading the response".to_string()),
        }
    }

    /// Send a frame to the server and wait for the response
    pub fn send_and_receive(&mut self, frame: &Frame) -> Result<Frame, String> {
        self.write_stream(&frame)?;
        self.read_stream()
    }
}