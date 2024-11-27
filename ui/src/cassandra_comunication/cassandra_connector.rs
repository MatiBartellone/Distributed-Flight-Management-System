use rustls_pki_types::ServerName;
use tokio_rustls::client::TlsStream;
use webpki_roots::TLS_SERVER_ROOTS;
use std::sync::Arc;
use crate::utils::frame::Frame;

use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub struct CassandraConnection {
    stream: TlsStream<TcpStream>,
}

impl CassandraConnection {
    /// Create a new connection to the server with the given node (ip:port)
    pub async  fn new(node: &str) -> Result<Self, String> {
        let (ip, _port) = node.split_once(":")
            .ok_or_else(|| "Formato incorrecto de la cadena ip:port".to_string())?;
        
        // Get the ClientConfig
        let config = CassandraConnection::get_config()?;
        let rc_config = Arc::new(config);

        // Create a new connection to the server
        let connector = TlsConnector::from(rc_config);

        // Connect to the server with a socket
        let socket = TcpStream::connect(node)
            .await
            .map_err(|e| format!("Error connecting to {}: {}", node, e))?;

        // Create a new Stream with the connector and the socket
        let domain = ServerName::try_from(ip)
            .map_err(|_| format!("Error converting IP {} to ServerName", ip))?
            .to_owned();
        let stream = connector.connect(domain, socket)
            .await
            .map_err(|e| format!("Error connecting to {}: {}", node, e))?;
        
        Ok(Self { stream })
    }

    async fn write_stream(&mut self, frame: &Frame) -> Result<(), String> {
        let frame = frame.to_bytes().map_err(|_| "Error al convertir a bytes".to_string())?;
        
        self.stream.write_all(&frame)
            .await
            .map_err(|_| "Error al escribir".to_string())?;

        self.stream.flush()
            .await
            .map_err(|_| "Error al hacer flush".to_string())?;
        Ok(())
    }

    async fn read_stream(&mut self) -> Result<Frame, String> {
        let mut buf = [0; 1024];
        match self.stream.read(&mut buf).await {
            Ok(n) if n > 0 => Frame::parse_frame(&buf[..n]),
            _ => Err("Fail reading the response".to_string()),
        }
    }
    
    /// Send a frame to the server and wait for the response
    pub async fn send_and_receive(& mut self, frame: &Frame) -> Result<Frame, String> {
        self.write_stream(&frame).await?;
        self.read_stream().await
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