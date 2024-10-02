use crate::executables::auth_response_executable::AuthResponseExecutable;
use crate::parsers::parser::Parser;

struct AuthResponseParser{}

impl Parser for AuthResponseParser {
    fn parse(&self, bytes: &[u8]) -> AuthResponseExecutable {
        AuthResponseExecutable{}
    }
}