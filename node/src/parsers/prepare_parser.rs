use crate::executables::executable::Executable;
use crate::executables::prepare_executable::PrepareExecutable;
use crate::parsers::parser::Parser;
use crate::utils::bytes_cursor::BytesCursor;
use crate::utils::errors::Errors;

pub struct PrepareParser;

impl PrepareParser {
    fn get_id_and_query(&self, body: &[u8]) -> Result<(uuid::Uuid, String), Errors> {
        let mut cursor = BytesCursor::new(body);
        let query = cursor.read_long_string()?;
        let id = uuid::Uuid::new_v4();
        Ok((id, query))
    }
}

impl Parser for PrepareParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let (id, query) = self.get_id_and_query(body)?;
        let executable = PrepareExecutable::new(id, query);
        Ok(Box::new(executable))
    }
}
