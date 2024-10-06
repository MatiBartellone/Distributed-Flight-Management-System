use crate::executables::executable::Executable;
use crate::executables::prepare_executable::PrepareExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct PrepareParser;

impl Parser for PrepareParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(PrepareExecutable))
    }
}

