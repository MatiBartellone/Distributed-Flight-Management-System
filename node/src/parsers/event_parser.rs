use crate::executables::event_executable::EventExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct EventParser;

impl Parser for EventParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(EventExecutable))
    }
}
