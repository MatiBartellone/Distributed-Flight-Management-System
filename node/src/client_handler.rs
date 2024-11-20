use crate::meta_data::meta_data_handler::MetaDataHandler;
use crate::parsers::parser_factory::ParserFactory;
use crate::response_builders::error_builder::ErrorBuilder;
use crate::utils::constants::CLIENT_METADATA_PATH;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;
use crate::utils::parser_constants::{AUTH_RESPONSE, AUTH_SUCCESS, STARTUP};
use std::io::{Read, Write};
use std::net::TcpStream;

pub struct ClientHandler {}

impl ClientHandler {
    pub fn handle_client(mut stream: TcpStream) -> Result<(), Errors> {
        add_new_client()?;
        let mut buffer = [0; 1024];
        loop {
            stream.flush().expect("Failed to flush client");
            match stream.read(&mut buffer) {
                Ok(0) => {
                    println!("Client disconnected");
                    delete_client()?;
                    break;
                }
                Ok(n) => match execute_request(buffer[0..n].to_vec()) {
                    Ok(response) => {
                        stream.flush().expect("Failed to flush client");
                        stream
                            .write_all(response.as_slice())
                            .expect("Error writing response");
                    }
                    Err(e) => {
                        let frame = ErrorBuilder::build_error_frame(
                            Frame::parse_frame(buffer.as_slice()).expect("Failed to parse frame"),
                            e,
                        )
                        .expect("Failed to build error frame");
                        stream.flush().expect("Failed to flush client");
                        stream
                            .write_all(frame.to_bytes().as_slice())
                            .expect("Error writing response");
                    }
                },
                Err(e) => {
                    println!("Error leyendo del socket: {}", e);
                    delete_client()?;
                    break;
                }
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
    let mut stream = MetaDataHandler::establish_connection()?;
    let client_metadata = MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
    client_metadata.add_new_client(CLIENT_METADATA_PATH.to_string())
}

fn has_started() -> Result<bool, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let client_metadata = MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
    client_metadata.has_started(CLIENT_METADATA_PATH.to_string())
}

fn is_authorized() -> Result<bool, Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let client_metadata = MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
    client_metadata.is_authorized(CLIENT_METADATA_PATH.to_string())
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
        let mut stream = MetaDataHandler::establish_connection()?;
        let client_metadata =
            MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
        client_metadata.startup_client(CLIENT_METADATA_PATH.to_string())?
    }
    Ok(())
}

fn set_auth(initial_opcode: u8, response_opcode: u8) -> Result<(), Errors> {
    if initial_opcode == AUTH_RESPONSE && response_opcode == AUTH_SUCCESS {
        let mut stream = MetaDataHandler::establish_connection()?;
        let client_metadata =
            MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
        client_metadata.authorize_client(CLIENT_METADATA_PATH.to_string())?
    }
    Ok(())
}

fn delete_client() -> Result<(), Errors> {
    let mut stream = MetaDataHandler::establish_connection()?;
    let client_metadata = MetaDataHandler::get_instance(&mut stream)?.get_client_meta_data_access();
    client_metadata.delete_client(CLIENT_METADATA_PATH.to_string())
}
