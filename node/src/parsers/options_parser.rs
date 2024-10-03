use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::executables::options_executable::OptionsExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

const CQL_VERSION: &str = "3.0.0";
const COMPRESSION: &str = "lz4";

pub struct OptionsParser;

impl OptionsParser {}

fn get_options() -> Result<HashMap<String, String>, Errors> {
    let mut options = HashMap::<String, String>::new();
    options.insert("CQL_VERSION".to_string(), CQL_VERSION.to_string());
    options.insert("COMPRESSION".to_string(), COMPRESSION.to_string());
    Ok(options)
}

impl Parser for OptionsParser {
    fn parse(&self, _bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let options = get_options()?;
        let executable = OptionsExecutable::new(options);
        Ok(Box::new(executable))
    }
}

