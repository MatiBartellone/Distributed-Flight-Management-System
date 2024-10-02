use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct BatchParser {}

impl Parser for BatchParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}