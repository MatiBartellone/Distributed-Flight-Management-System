use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::utils::frame::Frame;

pub struct CassandraConnection {
    stream: TcpStream,
    response_map: Arc<Mutex<HashMap<i16, Sender<Frame>>>> // Map to store the response of the server
}

impl CassandraConnection {
    // Create a new connection to the server
    pub fn new(node: &str) -> Result<Self, String> {
        match TcpStream::connect(node) {
            Ok(stream) => Ok(Self {
                stream,
                response_map: Arc::new(Mutex::new(HashMap::new())),
            }),
            Err(e) => Err(format!("Failed to connect to node {}: {}", node, e)),
        }
    }

    // Send a frame to the server and return a receiver to get the response
    pub fn send_frame(&mut self, frame: &mut Frame, id: usize) -> Result<Receiver<Frame>, String> {
        frame.stream = id as i16;
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;

        let (tx, rx) = channel();
        self.response_map.lock().unwrap().insert(id as i16, tx);
        
        self.stream.write_all(&frame).map_err(|_| "Error al escribir".to_string())?;
        self.stream.flush().map_err(|_| "Error al hacer flush".to_string())?;
        
        Ok(rx)
    }

    // Read the response from the server
    pub fn read_frame_response(&mut self) -> Result<(), String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n])?;
                let id = frame.stream;
                if let Some(tx) = self.response_map.lock().unwrap().remove(&id) {
                    let _ = tx.send(frame);
                }
                Ok(())
            },
            _ => Err("Fail reading the response".to_string()),
        }
    }
}