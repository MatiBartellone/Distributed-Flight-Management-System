use rustls::{ServerConnection, StreamOwned};

use crate::logger::Logger;
use crate::meta_data::meta_data_handler::{use_client_meta_data, use_node_meta_data};
use crate::meta_data::nodes::node::State;
use crate::parsers::parser_factory::ParserFactory;
use crate::response_builders::error_builder::ErrorBuilder;
use crate::utils::constants::{CLIENT_METADATA_PATH, LOGGER_PATH, NODES_METADATA_PATH};
use crate::utils::errors::Errors;
use crate::utils::parser_constants::{AUTH_RESPONSE, AUTH_SUCCESS, STARTUP};
use crate::utils::types::frame::Frame;
use crate::utils::types::tls_stream::{read_exact_from_tls_stream, write_to_tls_stream};
use std::net::TcpStream;
use crate::utils::frame_reader::FrameReader;

pub struct ClientHandler {}

impl ClientHandler {
    pub fn handle_client(
        mut stream: StreamOwned<ServerConnection, TcpStream>,
    ) -> Result<(), Errors> {
        let logger = Logger::new(LOGGER_PATH);
        
        add_new_client()?;
        loop {
            match read_exact_from_tls_stream(&mut stream) {
                Err(_) => {
                    logger.log_message("Client disconnected.");
                    delete_client()?;
                    break;
                }
                Ok(vec) => match execute_request(vec.clone()) {
                    Ok(response) => {
                        let string_response = FrameReader::read_frame(Frame::parse_frame(response.as_slice())?)?;
                        logger.log_response(&string_response);    
                        write_to_tls_stream(&mut stream, response.as_slice())?
                    }
                    Err(e) => {
                        logger.log_error(&format!("{}", &e));
                        let frame = ErrorBuilder::build_error_frame(
                            Frame::parse_frame(vec.as_slice())?,
                            e,
                        )?;
                        write_to_tls_stream(&mut stream, frame.to_bytes().as_slice())?
                    }
                },
            }
        }
        Ok(())
    }
}

fn execute_request(bytes: Vec<u8>) -> Result<Vec<u8>, Errors> {
    use_node_meta_data(|handler| {
        if handler
            .get_cluster(NODES_METADATA_PATH)?
            .get_own_node()
            .state
            == State::StandBy
        {
            return Err(Errors::Invalid(String::from("Node is in standby mode")));
        }
        Ok(())
    })?;
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
        return Err(Errors::Unauthorized(String::from(
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
