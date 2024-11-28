use std::sync::Arc;
use std::net::TcpStream;
use rustls:: ServerConfig;
use std::io::{Write, Read};
use simple_server::tls_stream::{flush_stream_tls, get_stream_owned, read_from_stream_no_zero, write_to_stream};
use std::net::TcpListener;
use std::io;
use openssl::symm::{Cipher, encrypt, decrypt};
use openssl::rand::rand_bytes;


pub const AES_KEY: [u8; 32] = [
    107, 133, 195, 73, 171, 146, 174, 177, 245, 55, 2, 116, 4, 202, 100, 1,
    75, 15, 151, 34, 194, 240, 98, 3, 111, 115, 214, 153, 82, 205, 149, 103
];

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

fn _handle_client_tls(stream: TcpStream, config: Arc<ServerConfig>) -> Result<(), String> {
    let mut stream = get_stream_owned(stream, config)?;
    loop {
        match read_from_stream_no_zero(&mut stream) {
            Ok(frame) => println!("Received: {:?}", String::from_utf8_lossy(&frame)),
            Err(e) => {
                eprintln!("{}", e);
                return Ok(());
            }
        }
        match write_to_stream(&mut stream, b"Hello from server!"){
            Ok(_) => println!("Sent: Hello from server!"),
            Err(e) => eprintln!("Error writing to stream: {}", e),
        }
        match flush_stream_tls(&mut stream) {
            Ok(_) => println!("Flushed"),
            Err(e) => eprintln!("Error flushing stream: {}", e),
        }
    }
}

fn write_stream(socket: &mut TcpStream, frame: String) -> Result<(), String> {
    // Usar la función auxiliar para encriptar el mensaje
    let (encrypted_data, iv) = encrypt_message(&frame, &AES_KEY)
        .map_err(|e| format!("Error al encriptar el frame: {}", e))?;

    // Empaquetar el IV y los datos cifrados juntos
    let mut message = iv.clone();
    message.extend(encrypted_data); // Añadir los datos cifrados al final del IV

    socket.write_all(&message).map_err(|_| "Error al enviar los datos cifrados".to_string())?;
    Ok(())
}

fn read_stream(socket: &mut TcpStream) -> Result<String, String> {
    let mut buf = vec![0; 1024];
    let n = socket.read(&mut buf).map_err(|_| "Error al leer datos cifrados".to_string())?;

    // Dividir los datos leídos en IV y mensaje cifrado
    if n < 16 {
        return Err("Datos recibidos demasiado cortos para ser válidos".to_string());
    }

    let iv = &buf[..16]; // Los primeros 16 bytes son el IV
    let encrypted_data = &buf[16..n]; // El resto son los datos cifrados

    // Usar la función auxiliar para desencriptar los datos
    decrypt_message(encrypted_data, iv, &AES_KEY)
        .map_err(|_| "Error al desencriptar los datos".to_string())
}

fn flush_stream(socket: &mut TcpStream) -> Result<(), String> {
    socket.flush()
        .map_err(|_| "Error al hacer flush".to_string())?;
    Ok(())
}

fn handle_client_encriptacion(mut stream: TcpStream) -> Result<(), String> {
    loop {
        println!("Esperando mensaje cifrado...");
        let message = read_stream(&mut stream)?;
        println!("Respuesta desencriptada: {}", message);
        let frame = get_user_data("Frame:");
        println!("Pregunta desencriptada: {}", &frame);
        write_stream(&mut stream, frame)?;
        println!("Enviado mensaje cifrado");
        flush_stream(&mut stream)?;
    }
}

fn encrypt_message(message: &str, aes_key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let cipher = Cipher::aes_256_cbc();
    let iv = generate_iv()?;
    let encrypted = encrypt(cipher, aes_key, Some(&iv), message.as_bytes())
        .map_err(|e| format!("Error cifrando mensaje: {}", e))?;
    Ok((encrypted, iv))
}

fn decrypt_message(encrypted_message: &[u8], iv: &[u8], aes_key: &[u8]) -> Result<String, String> {
    let cipher = Cipher::aes_256_cbc();
    let decrypted_data = decrypt(cipher, aes_key, Some(iv), encrypted_message)
        .map_err(|e| format!("Error descifrando mensaje: {}", e))?;
    let decrypted_string = String::from_utf8(decrypted_data)
        .map_err(|e| format!("{}", e))?;
    Ok(decrypted_string) 
}

fn generate_iv() -> Result<Vec<u8>, String> {
    let mut iv = vec![0; 16]; // 128-bit IV for AES CBC mode
    rand_bytes(&mut iv)
        .map_err(|e| format!("Error generando IV: {}", e))?; // Generar un IV aleatorio
    Ok(iv)
}

fn main() -> Result<(), String> {
    let node = get_user_data("FULL IP (ip:port): ");
    let listener = TcpListener::bind(node).expect("Error binding socket");

    //let config = create_server_config()?;
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());
                //handle_client_tls(stream, Arc::new(config.clone()));
                if let Err(e) = handle_client_encriptacion(stream) {
                    eprintln!("Error handling client: {}", e);
                }
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}