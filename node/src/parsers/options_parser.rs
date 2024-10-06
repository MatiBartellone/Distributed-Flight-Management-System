use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct OptionsParser;

impl Parser for OptionsParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(OptionsExecutable))
    }
}