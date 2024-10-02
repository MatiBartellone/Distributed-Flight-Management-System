use node::executables::executable::Executable;
use crate::parsers::parser::Parser;

struct StartupParser{}

impl Parser for StartupParser {
    fn parse(&self, bytes: &[u8]) -> impl Executable {
        todo!()
    }
}