use crate::executables::executable::Executable;
use crate::executables::execute_executable::ExecuteExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct ExecuteParser;

impl Parser for ExecuteParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(ExecuteExecutable))
    }
}

