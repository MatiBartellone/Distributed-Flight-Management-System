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

#[derive(Debug)]
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
}

fn join_bytes(bytes: &[u8], msg: &str) -> Vec<u8> {
    let mut new_bytes = bytes.to_vec();
    new_bytes.extend(msg.as_bytes());
    new_bytes
}
