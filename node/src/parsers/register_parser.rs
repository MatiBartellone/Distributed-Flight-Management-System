use crate::executables::executable::Executable;
use crate::executables::register_executable::RegisterExecutable;
use crate::parsers::parser::Parser;

pub struct RegisterParser;

impl Parser for RegisterParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(RegisterExecutable)
    }
}
