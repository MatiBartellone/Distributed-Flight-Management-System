use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct QueryParser{}

impl Parser for QueryParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}