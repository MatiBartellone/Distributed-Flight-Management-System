use std::fs::File;
use std::io::{self, BufRead};
use std::io::{Read, Write};
use std::net::TcpStream;

use inicializator::bytes_cursor::BytesCursor;
use inicializator::errors::Errors;
use inicializator::frame::Frame;

fn main() -> Result<(), Errors> {
    print!("FullIp: ");
    io::stdout().flush().map_err(|_| Errors::ReadTimeout("Error flushing stdout".to_string()))?;
    
    let mut node = String::new();
    io::stdin()
        .read_line(&mut node)
        .map_err(|_| Errors::ReadTimeout("Error reading node".to_string()))?;
    
    let node = node.trim();
    let mut stream = TcpStream::connect(node).map_err(|_| Errors::ServerError("Error connecting to server".to_string()))?;

    let file = File::open("simulator/src/querys.txt").map_err(|_| Errors::ReadTimeout("Error al leer las querys".to_string()))?;
    let reader = io::BufReader::new(file);
    let consistency = 1;

    for line in reader.lines() {
        let query = line.map_err(|_| Errors::ReadTimeout("Error reading line from file".to_string()))?;
        if query.is_empty() || query.starts_with("//") {
            continue; 
        }
        let mut body = Vec::new();
        body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
        body.extend_from_slice(query.as_bytes());
        body.extend_from_slice((consistency as i16).to_be_bytes().as_slice());
        
        let mut query_bytes = vec![0x03, 0x00, 0x00, 0x01, 0x07];
        query_bytes.extend_from_slice((body.len() as i32).to_be_bytes().as_slice());
        query_bytes.extend_from_slice(body.as_slice());

        stream.write_all(query_bytes.as_slice()).map_err(|_| Errors::ServerError("Error writing to socket".to_string()))?;
        stream.flush().map_err(|_| Errors::ServerError("Error flushing stream".to_string()))?;

        let mut buf = [0; 1024];
        match stream.read(&mut buf) {
            Ok(n) => {
                if n > 0 {
                    let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                    match frame.opcode {
                        0x00 => {
                            let mut cursor = BytesCursor::new(frame.body.as_slice());
                            println!("ERROR");
                            if let Ok(string) = cursor.read_long_string() {
                                println!("{}", string);
                            } else {
                                println!("{:?}", &String::from_utf8_lossy(frame.body.as_slice()));
                            }
                        }
                        0x08 => {
                            let mut cursor = BytesCursor::new(frame.body.as_slice());
                            println!("RESULT");
                            if let Ok(string) = cursor.read_long_string() {
                                println!("{}", string);
                            } else {
                                println!("{:?}", &String::from_utf8_lossy(frame.body.as_slice()));
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                println!("Error leyendo del socket: {}", e);
            }
        }
    }
    Ok(())
}