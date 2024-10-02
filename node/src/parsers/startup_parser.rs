use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;

struct StartupParser{}

impl Parser for StartupParser {
    fn parse(&self, bytes: &[u8]) -> StartupExecutable {
        StartupExecutable{}
    }
}