use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct ExecuteParser{}

impl Parser for ExecuteParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}