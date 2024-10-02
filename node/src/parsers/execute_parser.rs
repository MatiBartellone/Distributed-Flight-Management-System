use crate::executables::execute_executable::ExecuteExecutable;
use crate::parsers::parser::Parser;

struct ExecuteParser{}

impl Parser for ExecuteParser {
    fn parse(&self, bytes: &[u8]) -> ExecuteExecutable{
        ExecuteExecutable{}
    }
}