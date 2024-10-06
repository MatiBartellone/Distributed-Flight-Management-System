use crate::executables::auth_response_executable::AuthResponseExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct AuthResponseParser;

impl Parser for AuthResponseParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(AuthResponseExecutable))
    }
}

