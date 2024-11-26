use webpki_roots::TLS_SERVER_ROOTS;
use std::io::Read;
use std::{io::Write, sync::Arc};
use std::net::TcpStream;
use crate::utils::frame::Frame;

use rustls::{ClientConfig, ClientConnection, RootCertStore, Stream};

pub struct CassandraConnection {
    stream: Stream<'static, ClientConnection, TcpStream>
}

impl CassandraConnection {
    /// Create a new connection to the server with the given node (ip:port)
    pub fn new(node: &str) -> Result<Self, String> {
        let ip = node.split_once(":")
            .map(|(ip, _)| ip.to_string())
            .ok_or_else(|| "Formato incorrecto de la cadena ip:port".to_string())?;
        
        // Get the ClientConfig
        let config = CassandraConnection::get_config()?;
        let rc_config = Arc::new(config);

        // Create a new connection to the server
        let ip = ip.try_into()
            .map_err(|_| "Invalid IP".to_string())?;
        let mut conn = ClientConnection::new(rc_config, ip)
            .map_err(|e| format!("Error creating the connection: {}", e))?;

        // Connect to the server with a socket
        let mut socket = TcpStream::connect(node)
            .map_err(|e| format!("Error connecting to {}: {}", node, e))?;

        // Create a new Stream with the connection and the socket
        let tls_conn= Stream::new(&mut conn, &mut socket);
        
        Ok(Self { stream: tls_conn })
    }

    fn write_stream(&mut self, frame: &Frame) -> Result<(), String> {
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;
        
        self.stream.write_all(&frame)
            .map_err(|_| "Error al escribir".to_string())?;

        self.stream.flush()
            .map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    fn read_stream(&mut self) -> Result<Frame, String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => Frame::parse_frame(&buf[..n]),
            _ => Err("Fail reading the response".to_string()),
        }
    }
    
    /// Send a frame to the server and wait for the response
    pub fn send_and_receive(& mut self, frame: &Frame) -> Result<Frame, String> {
        self.write_stream(&frame)?;
        self.read_stream()
    }

    fn get_config() -> Result<ClientConfig, String> {
        let root_store = RootCertStore::from_iter(
            TLS_SERVER_ROOTS
                .iter()
                .cloned(),
        );
        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    }
}