use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;
use test_client::bytes_cursor::BytesCursor;
use test_client::frame::Frame;

pub const ERROR: u8 = 0;
pub const STARTUP: u8 = 1;
const AUTHENTICATE: u8 = 3;
const OPTIONS: u8 = 5;
const SUPPORTED: u8 = 6;
const QUERY: u8 = 7;
const RESULT: u8 = 8;
const PREPARE: u8 = 9;
const EXECUTE: u8 = 10;
const AUTH_CHALLENGE: u8 = 14;
const AUTH_RESPONSE: u8 = 15;
const AUTH_SUCCESS: u8 = 16;
const VERSION: u8 = 3;
const FLAG: u8 = 0;

fn main() {
    let node = get_user_data("FULL IP (ip:port): ");

    if let Ok(mut stream) = TcpStream::connect(node) {
        let mut input = String::new();
        while io::stdin().read_line(&mut input).is_ok() {
            match input.trim() {
                "startup" => send_startup(&mut stream),
                "admin" => send_auth_admin(&mut stream),
                "auth_response" => send_auth_response(&mut stream),
                "options" => send_options(&mut stream),
                "query" => send_query(&mut stream),
                "prepare" => send_prepare(&mut stream),
                "execute" => send_execute(&mut stream),
                _ => {
                    input.clear();
                    continue;
                }
            }
            println!();
            stream.flush().expect("could not flush stream");
            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(n) => {
                    if n > 0 {
                        let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                        match frame.opcode {
                            ERROR => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("ERROR");
                                if let Ok(string) = cursor.read_long_string() {
                                    println!("{}", string);
                                } else {
                                    println!(
                                        "{:?}",
                                        &String::from_utf8_lossy(frame.body.as_slice())
                                    );
                                }
                            }
                            AUTHENTICATE => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("AUTHENTICATE");
                                println!("{}", cursor.read_string().unwrap());
                            }
                            AUTH_SUCCESS => {
                                println!("AUTH_SUCCESS");
                            }
                            AUTH_CHALLENGE => {
                                println!("AUTH_CHALLENGE");
                            }
                            SUPPORTED => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("SUPPORTED");
                                for (key, value) in cursor.read_string_map().unwrap() {
                                    println!("{}: {}", key, value);
                                }
                            }
                            RESULT => {
                                let mut cursor = BytesCursor::new(frame.body.as_slice());
                                println!("RESULT");


                                println!(
                                    "{:?}",
                                    &String::from_utf8_lossy(frame.body.as_slice())
                                );

                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    println!("Error leyendo del socket: {}", e);
                }
            }
            stream.flush().expect("sds");
            input.clear();
            println!()
        }
    }
}

fn get_user_data(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().expect("Failed to flush stdout");
    let mut data = String::new();
    io::stdin()
        .read_line(&mut data)
        .expect("Error reading data");
    data.trim().to_string()
}

fn write_to_stream(stream: &mut TcpStream, content: &[u8]) {
    stream
        .write_all(content)
        .expect("Error writing to socket");
}

fn parse_string_to_i16_be_bytes(string: String) -> [u8; 2] {
    string.parse::<i16>().unwrap().to_be_bytes()
}

fn send_startup(stream: &mut TcpStream) {
    let startup_bytes = vec![
        VERSION, FLAG, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x16, 0x00, 0x01, // n = 1
        0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
        b'N', // "CQL_VERSION"
        0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
    ];
    write_to_stream(stream, &startup_bytes.as_slice());
}

fn send_auth_admin(stream: &mut TcpStream) {
    let auth_response_bytes = vec![
        VERSION, FLAG, 0x00, 0x01, 0x0F, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x0E, b'a', b'd',
        b'm', b'i', b'n', b':', b'p', b'a', b's', b's', b'w', b'o', b'r', b'd',
    ];
    write_to_stream(stream, &auth_response_bytes.as_slice());
}

fn send_auth_response(stream: &mut TcpStream) {
    let credentiasl = get_user_data("Enter credentials user:password");
    write_to_stream(stream, &build_frame(credentiasl.as_bytes().to_vec(), AUTH_RESPONSE).as_slice());
}

fn send_options(stream: &mut TcpStream) {
    write_to_stream(stream, &build_frame(Vec::new(), OPTIONS).as_slice());
}

fn send_query(stream: &mut TcpStream) {
    let query = get_user_data("Query: ");
    let consistency = get_user_data("Consistency: ");
    let mut body = Vec::new();
    body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
    body.extend_from_slice(query.as_bytes());
    body.extend_from_slice(&parse_string_to_i16_be_bytes(consistency));
    write_to_stream(stream, &build_frame(body, QUERY).as_slice())
}

fn send_prepare(stream: &mut TcpStream) {
    let query = get_user_data("Query: ");
    let mut body = Vec::new();
    body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
    body.extend_from_slice(query.as_bytes());
    write_to_stream(stream, &build_frame(body, PREPARE).as_slice())
}

fn send_execute(stream: &mut TcpStream) {
    let id = get_user_data("Query id: ");
    let consistency = get_user_data("Consistency: ");
    let mut body = Vec::new();
    body.extend_from_slice(&parse_string_to_i16_be_bytes(id));
    body.extend_from_slice(&parse_string_to_i16_be_bytes(consistency));
    write_to_stream(stream, &build_frame(body, EXECUTE).as_slice())
}

fn build_frame(body: Vec<u8>, opcode: u8) -> Vec<u8> {
    let mut frame = vec![VERSION, FLAG, 0x00, 0x01, opcode];
    frame.extend_from_slice((body.len() as i32).to_be_bytes().as_slice());
    frame.extend_from_slice(body.as_slice());
    frame
}