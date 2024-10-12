use std::io;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;
use test_client::bytes_cursor::BytesCursor;
use test_client::frame::Frame;

fn main() {



    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let startup_bytes = vec![
        0x03,
        0x00,
        0x00,0x01,
        0x01,
        0x00, 0x00, 0x00, 0x16,
        0x00, 0x01, // n = 1
        0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
        0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
    ];
    let mut auth_response_bytes = vec![
        0x03,
        0x00,
        0x00,0x01,
        0x0F,
        0x00, 0x00, 0x00, 0x0D,
    ];
    auth_response_bytes.extend_from_slice("user:password".as_bytes());

    let options_bytes = vec![
        0x03,
        0x00,
        0x00,0x01,
        0x05,
    ];


    //let mut addrs_iter = ip.to_socket_addrs().expect("Invalid socket address");
    if let Ok(mut stream) = TcpStream::connect(socket) {

        let mut input = String::new();
        while let Ok(_) = io::stdin().read_line(&mut input) {
            match input.trim() {
                "exit" => { break; },
                "startup" => {stream.write_all(startup_bytes.as_slice()).expect("Error writing to socket");}
                "auth_response" => {stream.write_all(auth_response_bytes.as_slice()).expect("Error writing to socket");}
                "options" => {stream.write_all(options_bytes.as_slice()).expect("Error writing to socket");}
                _ => { continue; }
            }
            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(n) => {
                    if n > 0 {
                        let frame = Frame::parse_frame(&buf[..n]).expect("Error parsing frame");
                        dbg!(&frame);
                    }
                }
                Err(e) => {
                    println!("Error leyendo del socket: {}", e);
                }
            }


            //let mut cursor = BytesCursor::new(frame.body.as_slice());
            //dbg!(cursor.read_string_map());
        }
    }
}
