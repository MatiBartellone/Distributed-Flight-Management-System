use node::frame::Frame;
use node::parsers::parser_factory::ParserFactory;
use node::utils::errors::Errors;
use std::io::{self, Read, Write};
use std::net::TcpListener;

fn main() {
    print!("node's ip: ");
    io::stdout().flush().unwrap();
    let mut ip = String::new();
    io::stdin().read_line(&mut ip)
        .expect("Error reading ip");
    let ip = ip.trim();

    //let ip = "127.0.0.1:8080";
    let listener = TcpListener::bind(ip).expect("Error binding socket");

    match listener.accept() {
        Ok((mut stream, _)) => {
            // Mover la conexión a un thread si es necesario
            loop {
                let mut buffer = [0; 1024];
                println!("holaaa");
                match stream.read(&mut buffer) {
                    Ok(0) => {
                        // El cliente ha cerrado la conexión
                        println!("Cliente desconectado");
                        break;
                    }
                    Ok(_) => {
                        // Ejecutar la lógica de la solicitud
                        match execute_request(buffer.to_vec()) {
                            Ok(response) => {
                                stream.write_all(response.as_slice()).expect("Error writing response");
                                stream.flush().expect("Error flushing stream");
                            }
                            Err(e) => { println!("Error ejecutando solicitud: {}", e) }
                        }
                    }
                    Err(e) => {
                        println!("Error leyendo del socket: {}", e);
                        break; // Sal del bucle si hay un error en la lectura
                    }
                }
            }
        }
        Err(e) => {
            println!("Error aceptando la conexión: {}", e);
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