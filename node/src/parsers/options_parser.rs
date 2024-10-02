use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct OptionsParser{}

impl Parser for OptionsParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}