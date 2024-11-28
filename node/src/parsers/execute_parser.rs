use crate::executables::executable::Executable;
use crate::executables::execute_executable::ExecuteExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;
use crate::utils::types::bytes_cursor::BytesCursor;

pub struct ExecuteParser;

impl Parser for ExecuteParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let mut cursor = BytesCursor::new(body);
        let id = cursor.read_short()?;
        let consistency = cursor.read_short()?;
        let executable = ExecuteExecutable::new(id, consistency);
        Ok(Box::new(executable))
    }
}
