use crate::executables::executable::Executable;
use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;
use crate::utils::conversion::bytes_to_string_map;
use crate::utils::errors::Errors;

pub struct StartupParser;

impl Parser for StartupParser {
    fn parse(&self, bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let config = bytes_to_string_map(bytes)?;
        let executable = StartupExecutable::new(config);
        Ok(Box::new(executable))
    }
}
