use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;

pub struct QueryParser{}

impl Parser for QueryParser {
    fn parse(&self, bytes: &[u8]) -> QueryExecutable {
        QueryExecutable{}
    }
}