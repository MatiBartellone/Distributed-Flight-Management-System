use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct AuthResponseParser{}

impl Parser for AuthResponseParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}