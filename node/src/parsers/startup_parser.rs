use crate::executables::executable::Executable;
use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct StartupParser;

impl Parser for StartupParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(StartupExecutable))
    }
}

