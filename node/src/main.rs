use node::frame::Frame;
use node::parsers::parser_factory::ParserFactory;
use node::utils::errors::Errors;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use node::response_builders::error_builder::ErrorBuilder;
use node::response_builders::frame_builder::FrameBuilder;

fn main() {
    print!("node's ip: ");
    io::stdout().flush().unwrap();
    let mut ip = String::new();
    io::stdin().read_line(&mut ip)
        .expect("Error reading ip");
    let ip = ip.trim();

    let listener = TcpListener::bind(ip).expect("Error binding socket");
    println!("Servidor escuchando en {}", ip);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                println!("Cliente conectado: {:?}", stream.peer_addr());

                // Mover la conexión a un hilo
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("Error aceptando la conexión: {}", e);
            }
        }
    }

}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Cliente desconectado");
                break;
            }
            Ok(_) => {
                match execute_request(buffer.to_vec()) {
                    Ok(response) => {
                        stream.write_all(response.as_slice()).expect("Error writing response");

                    }
                    Err(e) => {
                        let frame = ErrorBuilder::build_error_frame(Frame::parse_frame(buffer.as_slice()).unwrap(), e).unwrap();
                        stream.write_all(frame.to_bytes().as_slice()).expect("Error writing response");
                    }
                }
            }
            Err(e) => {
                println!("Error leyendo del socket: {}", e);
                break; // Sal del bucle si hay un error en la lectura
            }
        }
    }
}

fn execute_request(bytes: Vec<u8>) -> Result<Vec<u8>, Errors> {
    let frame = Frame::parse_frame(bytes.as_slice())?;
    dbg!(&frame);
    frame.validate_request_frame()?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let executable = parser.parse(frame.body.as_slice())?;
    let frame = executable.execute(frame)?;

    Ok(frame.to_bytes())
}