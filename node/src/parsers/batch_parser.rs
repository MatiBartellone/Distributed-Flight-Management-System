use crate::executables::batch_executable::BatchExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct BatchParser;

impl Parser for BatchParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(BatchExecutable))
    }
}