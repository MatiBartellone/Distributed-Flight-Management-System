use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;

pub fn flush_stream(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<(), String> {
    stream
        .flush()
        .map_err(|_| String::from("Failed to flush stream"))
}

pub fn write_to_stream(stream: &mut StreamOwned<ServerConnection, TcpStream>, content: &[u8]) -> Result<(), String> {
    stream
        .write_all(content)
        .map_err(|_| String::from("Failed to write to stream"))
}

pub fn read_exact_from_stream(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<Vec<u8>, String> {
    let mut buffer = [0; 1024];
    let size = stream
        .read(&mut buffer)
        .map_err(|_| String::from("Failed to read stream"))?;
    if size == 0 {
        return Ok(Vec::new());
    }
    Ok(buffer[0..size].to_vec())
}

pub fn read_from_stream_no_zero(stream: &mut StreamOwned<ServerConnection, TcpStream>) -> Result<Vec<u8>, String> {
    let buf = read_exact_from_stream(stream)?;
    if buf.is_empty() {
        return Err(String::from("Empty stream"));
    }
    Ok(buf)
}

fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|_| "Cannot read certificate file".to_string())?
        .map(|cert| cert.unwrap())
        .collect();
    Ok(certs)
}

fn load_private_key(path: impl AsRef<Path>) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|_| "Cannot read private key file".to_string())
}

pub fn create_server_config() -> Result<ServerConfig, String> {
    let certs = load_certs(&"src/certificates/certificate.pem")?;
    let private_key = load_private_key(&"src/certificates/private_key.pem")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|e| format!("Invalid server configuration: {}", e))?;

    Ok(config)
}

pub fn get_stream_owned(stream: TcpStream, config: Arc<ServerConfig>) -> Result<StreamOwned<ServerConnection, TcpStream>, String> {
    let conn = ServerConnection::new(config)
        .map_err(|e| format!("Error creating TLS connection: {}", e))?;
    let stream_owned = StreamOwned::new(conn, stream);
    Ok(stream_owned)
}

pub fn start_listener<F>(socket: SocketAddr, handle_connection: F) -> Result<(), String>
where
    F: Fn(&mut StreamOwned<ServerConnection, TcpStream>) -> Result<(), String>,
{
    let listener = TcpListener::bind(socket)
        .map_err(|_| String::from("Failed to set listener"))?;
    let config = create_server_config()?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut stream = get_stream_owned(stream, Arc::new(config.clone()))?;
                handle_connection(&mut stream)?
            }
            Err(_) => return Err(String::from("Failed to connect to listener")),
        }
    }
    Ok(())
}

pub fn connect_to_socket(socket_addr: SocketAddr) -> Result<StreamOwned<ServerConnection, TcpStream>, String> {
    let config = create_server_config()?;
    let stream = TcpStream::connect(socket_addr)
        .map_err(|_| String::from("Error connecting to socket."))?;
    get_stream_owned(stream, Arc::new(config))
}