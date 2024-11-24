use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
//use std::thread;

use crate::utils::frame::Frame;

#[derive(Clone)]
pub struct CassandraConnection {
    stream: Arc<Mutex<TcpStream>>,
    response_map: Arc<Mutex<HashMap<i16, Sender<Frame>>>> // Map to store the response of the server
}

impl CassandraConnection {
    // Create a new connection to the server
    pub fn new(node: &str) -> Result<Self, String> {
        match TcpStream::connect(node) {
            Ok(stream) => Ok(Self {
                stream: Arc::new(Mutex::new(stream)),
                response_map: Arc::new(Mutex::new(HashMap::new())),
            }),
            Err(e) => Err(format!("Failed to connect to node {}: {}", node, e)),
        }
    }

    fn write_stream(&self, frame: &Frame) -> Result<(), String> {
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;
        // Encrypt the frame


        let mut stream = match self.stream.lock() {
            Ok(lock) => lock,
            Err(_) => return Err("Error al obtener el stream".to_string()),
        };
        stream.write_all(&frame).map_err(|_| "Error al escribir".to_string())?;
        stream.flush().map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    fn save_response(&self, frame_id: i16) -> Result<Receiver<Frame>, String> {
        let (tx, rx) = channel();
        let mut response_map = match self.response_map.lock() {
            Ok(lock) => lock,
            Err(_) => return Err("Error al obtener el lock".to_string()),
        };
        (*response_map).insert(frame_id, tx);
        Ok(rx)
    }

    // Send a frame to the server and return a receiver to get the response
    pub fn send_frame(&self, frame: &Frame) -> Result<Receiver<Frame>, String> {
        self.write_stream(&frame)?;
        self.save_response(frame.stream)
    }

    fn read_stream(&self) -> Result<Frame, String> {
        let mut stream = match self.stream.lock() {
            Ok(lock) => lock,
            Err(_) => return Err("Error al obtener el stream".to_string()),
        };

        let mut buf = [0; 1024];
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                // Desencrypt the frame
                let desencrypted_bytes = &buf[..n];
                Frame::parse_frame(&desencrypted_bytes)
            }
            _ => Err("Fail reading the response".to_string()),
        }
    }

    fn send_response(&self, frame: Frame) -> Result<(), String> {
        let mut response_map = match self.response_map.lock() {
            Ok(lock) => lock,
            Err(_) => return Err("Error al obtener el lock".to_string()),
        };

        let id: i16 = frame.stream;
        if let Some(tx) = response_map.remove(&id) {
            let _ = tx.send(frame);
        }
        Ok(())
    }

    // Read the response from the server and send it to the receiver
    pub fn read_frame_response(&self) -> Result<(), String> {
        let frame = self.read_stream()?;
        self.send_response(frame)
    }

    // Send a frame to the server and wait for the response
    pub fn send_and_receive(&self, frame: &Frame) -> Result<Frame, String> {
        self.write_stream(&frame)?;
        self.read_stream()
    }
}