use crate::executables::auth_response_executable::AuthResponseExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;

pub struct AuthResponseParser;

impl Parser for AuthResponseParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(AuthResponseExecutable)
    }
}
