use crate::executables::executable::Executable;
use crate::executables::prepare_executable::PrepareExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;
use crate::utils::types::bytes_cursor::BytesCursor;

pub struct PrepareParser;

impl Parser for PrepareParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let string = cursor.read_long_string()?;
        let tokens = crate::parsers::query_parser::query_lexer(string)?;
        let query = crate::parsers::query_parser::query_parser(tokens)?;
        let executable = PrepareExecutable::new(query);
        Ok(Box::new(executable))
    }
}
