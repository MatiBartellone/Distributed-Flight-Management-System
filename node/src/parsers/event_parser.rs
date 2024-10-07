use crate::executables::event_executable::EventExecutable;
use crate::executables::executable::Executable;
use crate::parsers::parser::Parser;

pub struct EventParser;

impl Parser for EventParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(EventExecutable)
    }
}
