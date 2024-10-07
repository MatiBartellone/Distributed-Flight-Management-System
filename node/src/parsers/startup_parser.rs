use crate::executables::executable::Executable;
use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;

pub struct StartupParser;

impl Parser for StartupParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(StartupExecutable)
    }
}
