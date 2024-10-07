use crate::executables::batch_executable::BatchExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;

pub struct BatchParser;

impl Parser for BatchParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(BatchExecutable)
    }
}
