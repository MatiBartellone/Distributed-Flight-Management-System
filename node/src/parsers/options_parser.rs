use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

const ARGUMENTS_LENGTH: usize = 5;
const CQL_VERSION: &str = "3.0.0";
const COMPRESSION: &str = "lz4";

pub struct OptionsParser;

impl OptionsParser {
    fn validate_empty_body(&self, bytes: &[u8]) -> Result<(), Errors> {
        if bytes.len() != ARGUMENTS_LENGTH {
            return Err(Errors::ProtocolError(String::from(
                "Options body should be empty",
            )));
        }
        Ok(())
    }

    fn get_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::<String, String>::new();
        options.insert("CQL_VERSION".to_string(), CQL_VERSION.to_string());
        options.insert("COMPRESSION".to_string(), COMPRESSION.to_string());

        options
    }
}

impl Parser for OptionsParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        self.validate_empty_body(_bytes)?;
        let options = self.get_options();

        let executable = OptionsExecutable::new(options);
        Ok(Box::new(executable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (OptionsParser, Vec<u8>) {
        let parser = OptionsParser;
        let bytes = vec![0x03, 0x00, 0x00, 0x01, 0x05];
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
