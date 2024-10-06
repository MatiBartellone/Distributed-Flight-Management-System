use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct OptionsParser;

impl OptionsParser {
    fn validate_empty_body(&self, bytes: &[u8]) -> Result<(), Errors> {
        if !bytes.is_empty() {
            return Err(Errors::ProtocolError(String::from(
                "Options body should be empty",
            )));
        }
        Ok(())
    }
}

impl Parser for OptionsParser {
    fn parse(&self, body: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        self.validate_empty_body(body)?;

        let executable = OptionsExecutable::new();
        Ok(Box::new(executable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (OptionsParser, Vec<u8>) {
        let parser = OptionsParser;
        let bytes = vec![];
        (parser, bytes)
    }
    #[test]
    fn test_01_options_parser_validate_empty_body() {
        let (parser, bytes) = setup();
        let result = parser.parse(&bytes);
        assert!(result.is_ok());
    }
    #[test]
    fn test_02_options_with_body_is_not_valid() {
        let (parser, mut bytes) = setup();
        bytes.extend(b"hola");
        let result = parser.parse(&bytes);
        assert!(result.is_err());
    }
}
