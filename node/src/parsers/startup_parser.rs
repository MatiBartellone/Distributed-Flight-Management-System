use std::collections::HashMap;
use crate::utils::bytes_cursor::BytesCursor;
use crate::executables::executable::Executable;
use crate::executables::startup_executable::StartupExecutable;
use crate::parsers::parser::Parser;
use crate::utils::errors::Errors;

const CQL_VERSION: &str = "CQL_VERSION";
const COMPRESSION: &str = "COMPRESSION";
const VERSION_VALUE: &str = "3.0.0";
const COMPRESSION_VALUE: &str = "iz4";

pub struct StartupParser;

fn validate_config(config: &HashMap<String, String>) -> Result<(), Errors> {
    if config.len() > 2 { return Err(Errors::ConfigError("Invalid len of arguments".to_string())); }
    for key in config.keys() {
        if key != CQL_VERSION && key != COMPRESSION {
            return Err(Errors::ConfigError(format!("Invalid config key: {}", key)));
        }
    }
    match config.get(CQL_VERSION) {
        Some(version_value) if version_value == VERSION_VALUE => {},
        Some(_) => return Err(Errors::ConfigError(format!("CQL version must be {}, the version provided (CQL {}) is unsupported", VERSION_VALUE, config[CQL_VERSION]))),
        None => return Err(Errors::ConfigError("Missing CQL_VERSION".to_string())),
    }
    if let Some(compression_value) = config.get(COMPRESSION) {
        if compression_value != COMPRESSION_VALUE {
            return Err(Errors::ConfigError(format!("COMPRESSION must be {}, the value provided (COMPRESSION {}) is unsupported", COMPRESSION_VALUE, config[COMPRESSION])));
        }
    }
    Ok(())
}

impl Parser for StartupParser {
    fn parse(&self, bytes: &[u8]) -> Result<Box<dyn Executable>, Errors> {
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map()?;
        validate_config(&config)?;
        let executable = StartupExecutable::new(config);
        Ok(Box::new(executable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config_valid_bytes_cql_version_only() {
        let bytes = vec![
            0x00, 0x01, // n = 1
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
        ];

        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_valid_bytes_with_compression() {
        let bytes = vec![
            0x00, 0x02, // n = 2
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
            0x00, 0x0A, b'C', b'O', b'M', b'P', b'R', b'E', b'S', b'S', b'I', b'O', // "COMPRESSION"
            0x00, 0x03, b'i', b'z', b'4', // "iz4"
        ];

        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_config_invalid_key() {
        let bytes = vec![
            0x00, 0x01, // n = 1
            0x00, 0x07, b'I', b'N', b'V', b'A', b'L', b'I', b'D', // "INVALID"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
        ];
    
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
        assert!(result.is_err());

        if let Err(error) = result {
            match error {
                Errors::ConfigError(message) => {
                    assert_eq!(message, "Invalid config key: INVALID".to_string());
                }
                _ => panic!("Expected a ConfigError"),
            }
        }
    }
    
    #[test]
    fn test_validate_config_missing_cql_version() {
        let bytes = vec![
            0x00, 0x01, // n = 1
            0x00, 0x0A, b'C', b'O', b'M', b'P', b'R', b'E', b'S', b'S', b'I', b'O', // "COMPRESSION"
            0x00, 0x03, b'i', b'z', b'4', // "iz4"
        ];
    
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
        assert!(result.is_err());
    
        if let Err(error) = result {
            match error {
                Errors::ConfigError(message) => {
                    assert_eq!(message, "Missing CQL_VERSION".to_string());
                }
                _ => panic!("Expected a ConfigError"),
            }
        }
    }

    #[test]
    fn test_validate_config_too_many_entries() {
        let bytes = vec![
            0x00, 0x03, // n = 3
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
            0x00, 0x0A, b'C', b'O', b'M', b'P', b'R', b'E', b'S', b'S', b'I', b'O', // "COMPRESSION"
            0x00, 0x03, b'i', b'z', b'4', // "iz4"
            0x00, 0x03, b'k', b'e', b'y', // extra key
            0x00, 0x05, b'v', b'a', b'l', b'u', b'e', // extra value
        ];
    
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
        assert!(result.is_err());
    
        if let Err(error) = result {
            match error {
                Errors::ConfigError(message) => {
                    assert_eq!(message, "Invalid len of arguments".to_string());
                }
                _ => panic!("Expected a ConfigError"),
            }
        }
    }

    #[test]
    fn test_validate_config_invalid_bytes_cql_version() {
        let bytes = vec![
            0x00, 0x01, // n = 1
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
            0x00, 0x05, b'2', b'.', b'0', b'.', b'0', // "2.0.0"
        ];
    
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
    
        assert!(result.is_err());
        if let Err(Errors::ConfigError(msg)) = result {
            assert_eq!(msg, format!("CQL version must be {}, the version provided (CQL 2.0.0) is unsupported", VERSION_VALUE));
        } else {
            panic!("Expected ConfigError due to invalid CQL_VERSION");
        }
    }
    
    #[test]
    fn test_validate_config_invalid_bytes_compression() {
        let bytes = vec![
            0x00, 0x02, // n = 2 
            0x00, 0x0B, b'C', b'Q', b'L', b'_', b'V', b'E', b'R', b'S', b'I', b'O', b'N', // "CQL_VERSION"
            0x00, 0x05, b'3', b'.', b'0', b'.', b'0', // "3.0.0"
            0x00, 0x0A, b'C', b'O', b'M', b'P', b'R', b'E', b'S', b'S', b'I', b'O', b'N', // "COMPRESSION"
            0x00, 0x07, b'I', b'N', b'V', b'A', b'L', b'I', b'D', // "INVALID"
        ];
    
        let mut cursor = BytesCursor::new(&bytes);
        let config = cursor.read_string_map(&bytes).unwrap();
        let result = validate_config(&config);
    
        assert!(result.is_err());
        if let Err(Errors::ConfigError(msg)) = result {
            assert_eq!(msg,format!("COMPRESSION must be {}, the value provided (COMPRESSION unsupported_compression) is unsupported", COMPRESSION_VALUE));
        } else {
            panic!("Expected ConfigError due to invalid COMPRESSION");
        }
    }
}
