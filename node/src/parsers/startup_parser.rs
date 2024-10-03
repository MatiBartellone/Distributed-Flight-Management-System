use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;
use crate::utils::conversion::bytes_to_string_map;
use crate::utils::errors::Errors;

const CQL_VERSION: &str = "CQL_VERSION";
const COMPRESSION: &str = "COMPRESSION";
const VERSION_VALUE: &str = "3.0.0";

pub struct StartupParser;

fn validate_config(config: &HashMap<String, String>) -> Result<(), Errors> {
    if config.len() > 2 { return Err(Errors::ConfigError("Invalid len of arguments".to_string())); }
    for key in config.keys() {
        if key != CQL_VERSION && key != COMPRESSION {
            return Err(Errors::ConfigError(format!("Invalid config key: {}", key)));
        }
    }
    match config.get(CQL_VERSION) {
        Some(version_value) if version_value == VERSION_VALUE => Ok(()),
        Some(_) => Err(Errors::ConfigError(format!("Unsupported CQL version: {}", config[CQL_VERSION]))),
        None => Err(Errors::ConfigError("Missing CQL_VERSION".to_string())),
    }
    // validate compression?
}

impl Parser for StartupParser {
    fn parse(&self, bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let config = bytes_to_string_map(bytes)?;
        validate_config(&config)?;
        let executable = StartupExecutable::new(config);
        Ok(Box::new(executable))
    }
}
