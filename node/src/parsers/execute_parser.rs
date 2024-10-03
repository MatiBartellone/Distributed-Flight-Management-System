use crate::executables::executable::Executable;
use crate::executables::execute_executable::ExecuteExecutable;
use crate::parsers::parser::Parser;

pub struct ExecuteParser;

impl Parser for ExecuteParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(ExecuteExecutable)
    }
}