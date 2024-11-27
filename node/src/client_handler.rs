use rustls::{ServerConnection, StreamOwned};

use crate::parsers::parser_factory::ParserFactory;
use crate::response_builders::error_builder::ErrorBuilder;
use crate::utils::constants::CLIENT_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;
use crate::utils::parser_constants::{AUTH_RESPONSE, AUTH_SUCCESS, STARTUP};
use crate::utils::tls_stream::use_client_meta_data;
use std::net::TcpStream;
use crate::utils::tls_stream::{
    flush_stream, read_exact_from_stream, write_to_stream,
};

pub struct ClientHandler {}

impl ClientHandler {
    pub fn handle_client(mut stream: StreamOwned<ServerConnection, TcpStream>) -> Result<(), Errors> {
        add_new_client()?;
        loop {
            flush_stream(&mut stream)?;
            match read_exact_from_stream(&mut stream)? {
                vec if vec.is_empty() => {
                    println!("Client disconnected");
                    delete_client()?;
                    break;
                }
                vec => match execute_request(vec.clone()) {
                    Ok(response) => {
                        flush_stream(&mut stream)?;
                        write_to_stream(&mut stream, response.as_slice())?
                    }
                    Err(e) => {
                        let frame = ErrorBuilder::build_error_frame(
                            Frame::parse_frame(vec.as_slice())?,
                            e,
                        )?;
                        flush_stream(&mut stream)?;
                        write_to_stream(&mut stream, frame.to_bytes().as_slice())?
                    }
                },
            }
        }
        Ok(())
    }
}

fn execute_request(bytes: Vec<u8>) -> Result<Vec<u8>, Errors> {
    let frame = Frame::parse_frame(bytes.as_slice())?;
    frame.validate_request_frame()?;
    let initial_opcode = frame.opcode;
    check_startup(frame.opcode)?;
    check_auth(frame.opcode)?;
    let parser = ParserFactory::get_parser(frame.opcode)?;
    let mut executable = parser.parse(frame.body.as_slice())?;
    let frame = executable.execute(frame)?;
    set_startup(initial_opcode)?;
    set_auth(initial_opcode, frame.opcode)?;
    Ok(frame.to_bytes())
}

fn add_new_client() -> Result<(), Errors> {
    use_client_meta_data(|handler| handler.add_new_client(CLIENT_METADATA_PATH.to_string()))
}

fn has_started() -> Result<bool, Errors> {
    use_client_meta_data(|handler| handler.has_started(CLIENT_METADATA_PATH.to_string()))
}

fn is_authorized() -> Result<bool, Errors> {
    use_client_meta_data(|handler| handler.is_authorized(CLIENT_METADATA_PATH.to_string()))
}

fn check_startup(opcode: u8) -> Result<(), Errors> {
    let has_started = has_started()?;
    if opcode == STARTUP && has_started {
        return Err(Errors::Invalid(String::from(
            "Client has already done startup",
        )));
    } else if opcode != STARTUP && !has_started {
        return Err(Errors::Unprepared(String::from(
            "Client must startup first",
        )));
    }
    Ok(())
}

fn check_auth(opcode: u8) -> Result<(), Errors> {
    let is_authorized = is_authorized()?;
    if opcode == AUTH_RESPONSE && is_authorized {
        return Err(Errors::Invalid(String::from(
            "Client is already authorized",
        )));
    } else if opcode != STARTUP && opcode != AUTH_RESPONSE && !is_authorized {
        return Err(Errors::Unprepared(String::from(
            "Client must authorize first",
        )));
    }
    Ok(())
}

fn set_startup(initial_opcode: u8) -> Result<(), Errors> {
    if initial_opcode == STARTUP {
        use_client_meta_data(|handler| handler.startup_client(CLIENT_METADATA_PATH.to_string()))?
    }
    Ok(())
}

fn set_auth(initial_opcode: u8, response_opcode: u8) -> Result<(), Errors> {
    if initial_opcode == AUTH_RESPONSE && response_opcode == AUTH_SUCCESS {
        use_client_meta_data(|handler| handler.authorize_client(CLIENT_METADATA_PATH.to_string()))?
    }
    Ok(())
}

fn delete_client() -> Result<(), Errors> {
    use_client_meta_data(|handler| handler.delete_client(CLIENT_METADATA_PATH.to_string()))
}
