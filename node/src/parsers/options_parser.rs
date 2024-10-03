use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;

pub struct OptionsParser;

impl Parser for OptionsParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(OptionsExecutable)
    }
}