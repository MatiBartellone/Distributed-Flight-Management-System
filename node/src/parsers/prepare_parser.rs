use crate::executables::executable::Executable;
use crate::executables::prepare_executable::PrepareExecutable;
use crate::parsers::parser::Parser;

pub struct PrepareParser;

impl Parser for PrepareParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(PrepareExecutable)
    }
}
