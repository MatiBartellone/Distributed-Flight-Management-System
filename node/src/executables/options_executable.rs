use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;

const ARGUMENTS_QUANTITY: u8 = 0x02;

pub struct OptionsExecutable {
    options: HashMap<String, String>,
}

impl OptionsExecutable {
    pub fn new(options: HashMap<String, String>) -> OptionsExecutable {
        OptionsExecutable { options }
    }

    fn get_string_multimap(&self) -> Vec<u8> {
        let mut string_multimap = Vec::new();

        string_multimap.push(ARGUMENTS_QUANTITY);

        for (key, value) in self.options.iter() {
            string_multimap.extend_from_slice(key.as_bytes());
            string_multimap.extend_from_slice(value.as_bytes());
        }
        string_multimap
    }
}

impl Executable for OptionsExecutable {
    fn execute(&self) -> Frame {
        let version = 0x03;
        let flags = 0x00;
        let stream = 0x01;
        let opcode = 0x06;
        let body = self.get_string_multimap();
        let length = body.len() as u32;
        let frame = Frame {
            version,
            flags,
            stream,
            opcode,
            length,
            body,
        };

        frame
    }
}

#[cfg(test)]
mod tests {
    const EXPECTED_VERSION: u8 = 0x03;
    const EXPECTED_FLAGS: u8 = 0x00;
    const EXPECTED_STREAM: i16 = 0x01;
    const EXPECTED_OPCODE: u8 = 0x06;
    const EXPECTED_LENGTH: u32 = 31;
    use super::*;
    fn setup() -> OptionsExecutable {
        let mut options = HashMap::<String, String>::new();
        options.insert("CQL_VERSION".to_string(), "3.0.0".to_string());
        options.insert("COMPRESSION".to_string(), "lz4".to_string());
        let executable = OptionsExecutable::new(options);
        executable
    }

    #[test]
    fn test_01_options_executable() {
        let executable = setup();
        let frame = executable.execute();
        assert_eq!(frame.version, EXPECTED_VERSION);
        assert_eq!(frame.flags, EXPECTED_FLAGS);
        assert_eq!(frame.stream, EXPECTED_STREAM);
        assert_eq!(frame.opcode, EXPECTED_OPCODE);
        assert_eq!(frame.length, EXPECTED_LENGTH);
    }
}
