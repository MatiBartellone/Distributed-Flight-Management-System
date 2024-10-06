use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        Ok(Box::new(QueryExecutable))
    }
}

