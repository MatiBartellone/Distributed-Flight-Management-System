use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

pub struct OptionsParser;

impl OptionsParser {}

fn get_options() -> HashMap<String, String> {
    let mut options = HashMap::<String, String>::new();
    options.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
    options.insert("COMPRESSION".to_string(), "lz4".to_string());

    options
}

impl Parser for OptionsParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let options = get_options();
        let executable = OptionsExecutable::new(options);
        Ok(Box::new(executable))
    }
}
