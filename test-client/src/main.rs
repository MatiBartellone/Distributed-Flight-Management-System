use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Write};
use test_client::bytes_cursor::BytesCursor;
use test_client::cassandra_connector::CassandraConnection;
use test_client::errors::Errors;
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
    let mut connector = CassandraConnection::new(&node).expect("Failed to connect to Cassandra");

    let mut input = String::new();
    while io::stdin().read_line(&mut input).is_ok() {
        match input.trim() {
            "startup" => send_startup(&mut connector),
            "admin" => send_auth_admin(&mut connector),
            "auth_response" => send_auth_response(&mut connector),
            "options" => send_options(&mut connector),
            "query" => send_query(&mut connector),
            "prepare" => send_prepare(&mut connector),
            "execute" => send_execute(&mut connector),
            "queries" => {
                send_queries(&mut connector);
                continue;
            },
            _ => {
                input.clear();
                continue;
            }
        }
        println!();
        match connector.read_stream() {
            Ok(frame) => {
                match frame.opcode {
                    ERROR => {
                        let mut cursor = BytesCursor::new(frame.body.as_slice());
                        let error_type = vec![cursor.read_u8().unwrap(), cursor.read_u8().unwrap()];
                        let msg = cursor.read_string().unwrap();
                        let error = Errors::new(error_type.as_slice(), msg);
                        println!("{}", error);
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
                        show_response(frame.body).unwrap();
                    }
                    _ => {}
                }

            }
            Err(e) => {
                println!("Error leyendo del socket: {}", e);
            }
        }
        input.clear();
        println!()
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

fn parse_string_to_i16_be_bytes(string: String) -> [u8; 2] {
    string.parse::<i16>().unwrap().to_be_bytes()
}

fn send_startup(connector: &mut CassandraConnection) {
    let startup_bytes = vec![
        VERSION, FLAG, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x16, 0x00, 0x01, // n = 1
        0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O',
        b'N', // "CQL_VERSION"
        0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
    ];
    connector.write_stream(&Frame::parse_frame(startup_bytes.as_slice()).unwrap()).unwrap()
}

fn send_auth_admin(connector: &mut CassandraConnection) {
    let auth_response_bytes = vec![
        VERSION, FLAG, 0x00, 0x01, 0x0F, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x0E, b'a', b'd',
        b'm', b'i', b'n', b':', b'p', b'a', b's', b's', b'w', b'o', b'r', b'd',
    ];
    connector.write_stream(&Frame::parse_frame(auth_response_bytes.as_slice()).unwrap()).unwrap()
}

fn send_auth_response(connector: &mut CassandraConnection) {
    let credentiasl = get_user_data("Enter credentials (user:password) :");
    let mut body = Vec::new();
    body.extend_from_slice((credentiasl.len() as i32).to_be_bytes().as_slice());
    body.extend_from_slice(credentiasl.as_bytes());
    let frame_bytes = build_frame(body, AUTH_RESPONSE);
    connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap()
}

fn send_options(connector: &mut CassandraConnection) {
    let frame_bytes = build_frame(Vec::new(), OPTIONS);
    connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap()
}

fn send_query(connector: &mut CassandraConnection) {
    let query = get_user_data("Query: ");
    let consistency = get_user_data("Consistency: ");
    let mut body = Vec::new();
    body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
    body.extend_from_slice(query.as_bytes());
    body.extend_from_slice(&parse_string_to_i16_be_bytes(consistency));
    let frame_bytes = build_frame(body, QUERY);
    connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap()
}
fn send_queries(connector: &mut CassandraConnection) {
    let path = get_user_data("Queries path : ");
    let consistency = get_user_data("General consistency: ");

    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.unwrap();
        let query = line.trim();
        if query.starts_with("/") {
            continue;
        }
        let mut body = Vec::new();
        body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
        body.extend_from_slice(query.as_bytes());
        body.extend_from_slice(&parse_string_to_i16_be_bytes(consistency.to_string()));
        let frame_bytes = build_frame(body, QUERY);
        connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap();

        match connector.read_stream() {
            Ok(frame) => {
                match frame.opcode {
                    ERROR => {
                        let mut cursor = BytesCursor::new(frame.body.as_slice());
                        let error_type = vec![cursor.read_u8().unwrap(), cursor.read_u8().unwrap()];
                        let msg = cursor.read_string().unwrap();
                        let error = Errors::new(error_type.as_slice(), msg);
                        println!("{}", error);
                    }
                    RESULT => {
                        show_response(frame.body).unwrap();
                    }
                    _ => {}
                }
            }
            Err(e) => {
                println!("Error reading from socket: {}", e);
            }
        }
    }
}

fn send_prepare(connector: &mut CassandraConnection) {
    let query = get_user_data("Query: ");
    let mut body = Vec::new();
    body.extend_from_slice((query.len() as i32).to_be_bytes().as_slice());
    body.extend_from_slice(query.as_bytes());
    let frame_bytes = build_frame(body, PREPARE);
    connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap()
}

fn send_execute(connector: &mut CassandraConnection) {
    let id = get_user_data("Query id: ");
    let consistency = get_user_data("Consistency: ");
    let mut body = Vec::new();
    body.extend_from_slice(&parse_string_to_i16_be_bytes(id));
    body.extend_from_slice(&parse_string_to_i16_be_bytes(consistency));
    let frame_bytes = build_frame(body, EXECUTE);
    connector.write_stream(&Frame::parse_frame(frame_bytes.as_slice()).unwrap()).unwrap()
}

fn build_frame(body: Vec<u8>, opcode: u8) -> Vec<u8> {
    let mut frame = vec![VERSION, FLAG, 0x00, 0x01, opcode];
    frame.extend_from_slice((body.len() as i32).to_be_bytes().as_slice());
    frame.extend_from_slice(body.as_slice());
    frame
}

fn show_response(body: Vec<u8>) -> Result<(), Errors> {
    let mut cursor = BytesCursor::new(body.as_slice());

    match cursor.read_int()?{
        1 => {
            println!("Operation was succesful");
            Ok(())
        },
        3 => {
            println!("Use keyspace was succesful");
            Ok(())
        },
        5 => {
            let change = cursor.read_string()?;
            let target = cursor.read_string()?;
            let option = cursor.read_string()?;
            println!("Operation was succesful, change: {}, target: {}, option: {}", change, target, option);
            Ok(())
        },
        2 => {
            let _ = cursor.read_int()?;
            let col_count = cursor.read_int()?;
            let keyspace = cursor.read_string()?;
            let table = cursor.read_string()?;
            println!("Rows from keyspace: {} and table {}:", keyspace, table);
            let mut header = String::new();
            for i in 0..col_count {
                if i != 0 {
                    header += ", ";
                }
                let col_name = cursor.read_string()?;
                let _ = cursor.read_i16()?;
                header += &col_name;
            }
            println!("{}", header);
            let row_count = cursor.read_int()?;
            for _ in 0..row_count {
                let mut row = String::new();
                for i in 0..col_count {
                    if i != 0 {
                        row += ", ";
                    }
                    let value = cursor.read_string()?;
                    row += &value;
                }
                println!("{}", row);
            }
            Ok(())
        }
        _ => {Ok(())}
    }
}