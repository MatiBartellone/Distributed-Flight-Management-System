use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;

use super::tokens::lexer::normalizar;
use super::tokens::token::tokenize;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, _bytes: &[u8]) -> Box<dyn Executable> {
        let string = "aa"; //castear a string los bytes
        let message = normalizar(string);
        let _tokens = tokenize(message);
        Box::new(QueryExecutable)
    }
}
