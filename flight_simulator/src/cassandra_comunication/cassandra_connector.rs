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

    // Send a frame to the server and return a receiver to get the response
    pub fn send_frame(&self, frame: &mut Frame) -> Result<Receiver<Frame>, String> {
        let frame_id = frame.stream;
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;
        // Encrypt the frame

        // Send the frame to the server
        {
            let mut stream = self.stream.lock().unwrap();
            stream.write_all(&frame).map_err(|_| "Error al escribir".to_string())?;
            stream.flush().map_err(|_| "Error al hacer flush".to_string())?;
        }

        let (tx, rx) = channel();
        let mut response_map = self.response_map.lock().unwrap();
        response_map.insert(frame_id, tx);
    
        Ok(rx)
    }

    // Read the response from the server and send it to the receiver
    pub fn read_frame_response(&self) -> Result<(), String> {
        let mut buf = [0; 1024];
        let mut stream = self.stream.lock().unwrap();
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                drop(stream);
                // Desencrypt the frame

                let frame = Frame::parse_frame(&buf[..n])?;
                let id: i16 = frame.stream;

                let mut response_map = self.response_map.lock().unwrap();
                if let Some(tx) = response_map.remove(&id) {
                    let _ = tx.send(frame);
                }
                Ok(())
            },
            _ => Err("Fail reading the response".to_string()),
        }
    }

    // Send a frame to the server and wait for the response
    pub fn send_and_receive(&self, frame: &mut Frame) -> Result<Frame, String> {
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;

        let mut stream = self.stream.lock().unwrap();
        stream.write_all(&frame).map_err(|_| "Error al escribir".to_string())?;
        stream.flush().map_err(|_| "Error al hacer flush".to_string())?;


        let mut buf = [0; 1024];
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let frame = Frame::parse_frame(&buf[..n])?;
                Ok(frame)
            },
            _ => Err("Fail reading the response".to_string()),
        }
    }
}