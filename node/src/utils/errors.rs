#[derive(Debug)]
pub enum Errors {
    ServerError(String),
    ProtocolError(String),
    BadCredentials(String),
    UnavailableException(String),
    Overloaded(String),
    IsBootstraping(String),
    TruncateError(String),
    WriteTimeout(String),
    ReadTimeout(String),
    SintaxError(String),
    Unauthorized(String),
    Invalid(String),
    ConfigError(String),
    AlreadyExists(String),
    Unprepared(String),
}