use crate::executables::executable::Executable;
use crate::executables::register_executable::RegisterExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct RegisterParser;

impl Parser for RegisterParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(RegisterExecutable))
    }
}