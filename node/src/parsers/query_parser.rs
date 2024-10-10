use crate::executables::executable::Executable;
use crate::executables::query_executable::QueryExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

use super::tokens::lexer::standardize;
use super::tokens::token::tokenize;

pub struct QueryParser;

impl Parser for QueryParser {
    fn parse(&self, _body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let string = "aa"; //castear a string los bytes
        let message = standardize(string);
        let _tokens = tokenize(message);
        Ok(Box::new(QueryExecutable))
    }
}
