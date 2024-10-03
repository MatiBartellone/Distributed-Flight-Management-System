use crate::executables::executable::Executable;
use crate::executables::event_executable::EventExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct EventParser;

impl Parser for EventParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(EventExecutable))
    }
}