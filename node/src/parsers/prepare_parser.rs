use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct PrepareParser{}

impl Parser for PrepareParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}