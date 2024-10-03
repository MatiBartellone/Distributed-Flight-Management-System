use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        Box::new(QueryExecutable)
    }
}