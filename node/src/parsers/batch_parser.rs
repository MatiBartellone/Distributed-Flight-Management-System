use crate::executables::batch_executable::BatchExecutable;
use crate::parsers::parser::Parser;

struct BatchParser {}

impl Parser for BatchParser {
    fn parse(&self, bytes: &[u8]) -> BatchExecutable {
        BatchExecutable {}
    }
}