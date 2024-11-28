use std::sync::Arc;
use std::net::TcpStream;
use rustls::{ StreamOwned, ServerConnection};
use std::io::{Write, Read};
use simple_server::tls_stream::{create_server_config, flush_stream, get_stream_owned, read_exact_from_stream, read_from_stream_no_zero, write_to_stream};
use std::net::TcpListener;
use std::io;

/// Get user data from the terminal
pub fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}

fn handle_client(mut stream: StreamOwned<ServerConnection, TcpStream>) {
    loop {
        match read_from_stream_no_zero(&mut stream) {
            Ok(frame) => println!("Received: {:?}", String::from_utf8_lossy(&frame)),
            Err(e) => {
                eprintln!("{}", e);
                return;
            }
        }
    
        match write_to_stream(&mut stream, b"Hello from server!"){
            Ok(_) => println!("Sent: Hello from server!"),
            Err(e) => eprintln!("Error writing to stream: {}", e),
        }
        match flush_stream(&mut stream) {
            Ok(_) => println!("Flushed"),
            Err(e) => eprintln!("Error flushing stream: {}", e),
        }
    }
}

fn main() -> Result<(), String> {
    let node = get_user_data("FULL IP (ip:port): ");
    let listener = TcpListener::bind(node).expect("Error binding socket");
    let config = create_server_config()?;
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());
                let stream = get_stream_owned(stream, Arc::new(config.clone()))?;
                handle_client(stream);
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}