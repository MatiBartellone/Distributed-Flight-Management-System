use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

const SERVER_ERROR: &[u8] = &[0x00, 0x00];
const PROTOCOL_ERROR: &[u8] = &[0x00, 0x0A];
const BAD_CREDENTIALS: &[u8] = &[0x01, 0x00];
const UNAVAILABLE_EXCEPTION: &[u8] = &[0x10, 0x00];
const OVERLOADED: &[u8] = &[0x10, 0x01];
const IS_BOOTSTRAPPING: &[u8] = &[0x10, 0x02];
const TRUNCATE_ERROR: &[u8] = &[0x10, 0x03];
const WRITE_TIMEOUT: &[u8] = &[0x11, 0x00];
const READ_TIMEOUT: &[u8] = &[0x12, 0x00];
const SYNTAX_ERROR: &[u8] = &[0x20, 0x00];
const UNAUTHORIZED: &[u8] = &[0x21, 0x00];
const INVALID: &[u8] = &[0x22, 0x00];
const CONFIG_ERROR: &[u8] = &[0x23, 0x00];
const ALREADY_EXISTS: &[u8] = &[0x24, 0x00];
const UNPREPARED: &[u8] = &[0x25, 0x00];

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Errors {
    ServerError(String),
    ProtocolError(String),
    BadCredentials(String),
    UnavailableException(String),
    Overloaded(String),
    IsBootstrapping(String),
    TruncateError(String),
    WriteTimeout(String),
    ReadTimeout(String),
    SyntaxError(String),
    Unauthorized(String),
    Invalid(String),
    ConfigError(String),
    AlreadyExists(String),
    Unprepared(String),
}

impl Errors {
    pub fn get_bytes_body(&self) -> Vec<u8> {
        match self {
            Errors::ServerError(msg) => join_bytes(SERVER_ERROR, msg),
            Errors::ProtocolError(msg) => join_bytes(PROTOCOL_ERROR, msg),
            Errors::BadCredentials(msg) => join_bytes(BAD_CREDENTIALS, msg),
            Errors::UnavailableException(msg) => join_bytes(UNAVAILABLE_EXCEPTION, msg),
            Errors::Overloaded(msg) => join_bytes(OVERLOADED, msg),
            Errors::IsBootstrapping(msg) => join_bytes(IS_BOOTSTRAPPING, msg),
            Errors::TruncateError(msg) => join_bytes(TRUNCATE_ERROR, msg),
            Errors::WriteTimeout(msg) => join_bytes(WRITE_TIMEOUT, msg),
            Errors::ReadTimeout(msg) => join_bytes(READ_TIMEOUT, msg),
            Errors::SyntaxError(msg) => join_bytes(SYNTAX_ERROR, msg),
            Errors::Unauthorized(msg) => join_bytes(UNAUTHORIZED, msg),
            Errors::Invalid(msg) => join_bytes(INVALID, msg),
            Errors::ConfigError(msg) => join_bytes(CONFIG_ERROR, msg),
            Errors::AlreadyExists(msg) => join_bytes(ALREADY_EXISTS, msg),
            Errors::Unprepared(msg) => join_bytes(UNPREPARED, msg),
        }
    }

    pub fn new(error_type: &[u8], msg: String) -> Errors {
        match error_type {
            SERVER_ERROR => Errors::ServerError(msg),
            PROTOCOL_ERROR => Errors::ProtocolError(msg),
            BAD_CREDENTIALS => Errors::BadCredentials(msg),
            UNAVAILABLE_EXCEPTION => Errors::UnavailableException(msg),
            OVERLOADED => Errors::Overloaded(msg),
            IS_BOOTSTRAPPING => Errors::IsBootstrapping(msg),
            TRUNCATE_ERROR => Errors::TruncateError(msg),
            WRITE_TIMEOUT => Errors::WriteTimeout(msg),
            READ_TIMEOUT => Errors::ReadTimeout(msg),
            SYNTAX_ERROR => Errors::SyntaxError(msg),
            UNAUTHORIZED => Errors::Unauthorized(msg),
            INVALID => Errors::Invalid(msg),
            CONFIG_ERROR => Errors::ConfigError(msg),
            ALREADY_EXISTS => Errors::AlreadyExists(msg),
            UNPREPARED => Errors::Unprepared(msg),
            _ => Errors::ServerError(msg),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        match self {
            Errors::ServerError(msg) => {
                data.push(6);
                data.extend(msg.as_bytes());
            }
            Errors::ProtocolError(msg) => {
                data.push(7);
                data.extend(msg.as_bytes());
            }
            Errors::BadCredentials(msg) => {
                data.push(8);
                data.extend(msg.as_bytes());
            }
            Errors::UnavailableException(msg) => {
                data.push(9);
                data.extend(msg.as_bytes());
            }
            Errors::Overloaded(msg) => {
                data.push(10);
                data.extend(msg.as_bytes());
            }
            Errors::IsBootstrapping(msg) => {
                data.push(11);
                data.extend(msg.as_bytes());
            }
            Errors::TruncateError(msg) => {
                data.push(12);
                data.extend(msg.as_bytes());
            }
            Errors::WriteTimeout(msg) => {
                data.push(13);
                data.extend(msg.as_bytes());
            }
            Errors::ReadTimeout(msg) => {
                data.push(14);
                data.extend(msg.as_bytes());
            }
            Errors::SyntaxError(msg) => {
                data.push(15);
                data.extend(msg.as_bytes());
            }
            Errors::Unauthorized(msg) => {
                data.push(16);
                data.extend(msg.as_bytes());
            }
            Errors::Invalid(msg) => {
                data.push(17);
                data.extend(msg.as_bytes());
            }
            Errors::ConfigError(msg) => {
                data.push(18);
                data.extend(msg.as_bytes());
            }
            Errors::AlreadyExists(msg) => {
                data.push(19);
                data.extend(msg.as_bytes());
            }
            Errors::Unprepared(msg) => {
                data.push(20);
                data.extend(msg.as_bytes());
            }
        }
        data
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let (discriminador, mensaje) = (data[0], &data[1..]);
        let msg = String::from_utf8_lossy(mensaje).into_owned();

        match discriminador {
            6 => Some(Errors::ServerError(msg)),
            7 => Some(Errors::ProtocolError(msg)),
            8 => Some(Errors::BadCredentials(msg)),
            9 => Some(Errors::UnavailableException(msg)),
            10 => Some(Errors::Overloaded(msg)),
            11 => Some(Errors::IsBootstrapping(msg)),
            12 => Some(Errors::TruncateError(msg)),
            13 => Some(Errors::WriteTimeout(msg)),
            14 => Some(Errors::ReadTimeout(msg)),
            15 => Some(Errors::SyntaxError(msg)),
            16 => Some(Errors::Unauthorized(msg)),
            17 => Some(Errors::Invalid(msg)),
            18 => Some(Errors::ConfigError(msg)),
            19 => Some(Errors::AlreadyExists(msg)),
            20 => Some(Errors::Unprepared(msg)),
            _ => None,
        }
    }
}

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Errors::ServerError(msg) => write!(f, "ServerError: {}", msg),
            Errors::ProtocolError(msg) => write!(f, "ProtocolError: {}", msg),
            Errors::BadCredentials(msg) => write!(f, "BadCredentials: {}", msg),
            Errors::UnavailableException(msg) => write!(f, "UnavailableException: {}", msg),
            Errors::Overloaded(msg) => write!(f, "Overloaded: {}", msg),
            Errors::IsBootstrapping(msg) => write!(f, "IsBootstrapping: {}", msg),
            Errors::TruncateError(msg) => write!(f, "TruncateError: {}", msg),
            Errors::WriteTimeout(msg) => write!(f, "WriteTimeout: {}", msg),
            Errors::ReadTimeout(msg) => write!(f, "ReadTimeout: {}", msg),
            Errors::SyntaxError(msg) => write!(f, "SyntaxError: {}", msg),
            Errors::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            Errors::Invalid(msg) => write!(f, "Invalid: {}", msg),
            Errors::ConfigError(msg) => write!(f, "ConfigError: {}", msg),
            Errors::AlreadyExists(msg) => write!(f, "AlreadyExists: {}", msg),
            Errors::Unprepared(msg) => write!(f, "Unprepared: {}", msg),
        }
    }
}

fn join_bytes(bytes: &[u8], msg: &str) -> Vec<u8> {
    let mut new_bytes = bytes.to_vec();
    new_bytes.extend((msg.len() as i16).to_be_bytes().to_vec());
    new_bytes.extend(msg.as_bytes());
    new_bytes
}
