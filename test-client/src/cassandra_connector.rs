use rustls_pki_types::{pem::PemObject, CertificateDer, ServerName};
use webpki_roots::TLS_SERVER_ROOTS;
use std::{io::{Read, Write}, path::Path, sync::Arc};

use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::net::TcpStream;
use crate::frame::Frame;

pub struct CassandraConnection {
    stream: StreamOwned<ClientConnection, TcpStream>,
}

impl CassandraConnection {
    /// Create a new connection to the server with the given node (ip:port)
    pub fn new(node: &str) -> Result<Self, String> {
        let (ip, _port) = node.split_once(":")

            .ok_or_else(|| "Formato incorrecto de la cadena ip:port".to_string())?;
        // Get the ClientConfig
        let config = CassandraConnection::get_config()?;
        let rc_config = Arc::new(config);

        // Create a new connection to the server
        let domain = ServerName::try_from(ip)
            .map_err(|_| "Error converting IP Inmutables to ServerName".to_string())?
            .to_owned();
        let connector = ClientConnection::new(rc_config, domain)
            .map_err(|e| format!("Error creating TLS connection: {}", e))?;

        // Connect to the server with a socket
        let socket = TcpStream::connect(node)
            .map_err(|e| format!("Error connecting to {}: {}", node, e))?;

        // Create a new Stream with the connection and the socket
        let stream = StreamOwned::new(connector, socket);

        Ok(Self { stream })
    }

    pub fn write_stream(&mut self, frame: &Frame) -> Result<(), String> {
        let frame_bytes = frame.to_bytes();

        let mut message = (frame_bytes.len() as i32).to_be_bytes().to_vec();
        message.extend(frame_bytes);

        self.stream.write_all(&message)
            .map_err(|_| "Error al escribir".to_string())?;

        self.stream.flush()
            .map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    pub fn read_stream(&mut self) -> Result<Frame, String> {
        let mut size_buffer = [0; 4];
        self.stream
            .read_exact(&mut size_buffer)
            .map_err(|_| String::from("Failed to read stream size"))?;
        let size = i32::from_be_bytes(size_buffer) as usize;

        if size == 0 {
            Err("Fail reading the response".to_string())
        } else {
            let mut buffer = vec![0; size];
            self.stream
                .read_exact(&mut buffer)
                .map_err(|_| String::from("Failed to read full message"))?;
            Frame::parse_frame(&buffer[0..]).map_err(|_| String::from("Failed to parse frame"))
        }
    }
    
    /// Send a frame to the server and wait for the response
    pub fn send_and_receive(& mut self, frame: &Frame) -> Result<Frame, String> {
        self.write_stream(frame)?;
        self.read_stream()
    }

    fn get_config() -> Result<ClientConfig, String> {
        let mut root_store = RootCertStore::from_iter(
            TLS_SERVER_ROOTS
                .iter()
                .cloned(),
        );
        let certs = Self::load_certs("certificates/certificate.pem")?;
        for cert in certs {
            root_store.add(cert).map_err(|_| "Failed to add cert to root store".to_string())?;
        }
        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    }

    fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>, String> {
        let certs = CertificateDer::pem_file_iter(path)
            .map_err(|_| "Cannot read certificate file".to_string())?
            .map(|cert| cert.unwrap())
            .collect();
        Ok(certs)
    }
}