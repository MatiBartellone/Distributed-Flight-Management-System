use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;

struct OptionsParser{}

impl Parser for OptionsParser {
    fn parse(&self, bytes: &[u8]) -> OptionsExecutable{
        OptionsExecutable{}
    }
}