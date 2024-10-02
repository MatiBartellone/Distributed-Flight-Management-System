use crate::executables::prepare_executable::PrepareExecutable;
use crate::parsers::parser::Parser;

struct PrepareParser{}

impl Parser for PrepareParser {
    fn parse(&self, bytes: &[u8]) ->  PrepareExecutable {
        PrepareExecutable{}
    }
}