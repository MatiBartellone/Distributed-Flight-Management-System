use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;

const ARGUMENTS_QUANTITY: u8 = 0x02;
const CQL_VERSION: &str = "3.0.0";
const COMPRESSION: &str = "lz4";

pub struct OptionsExecutable {}

impl Default for OptionsExecutable {
    fn default() -> Self {
        Self::new()
    }
}

impl OptionsExecutable {
    pub fn new() -> OptionsExecutable {
        OptionsExecutable {}
    }

    fn get_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::<String, String>::new();
        options.insert("CQL_VERSION".to_string(), CQL_VERSION.to_string());
        options.insert("COMPRESSION".to_string(), COMPRESSION.to_string());

        options
    }

    fn extend_multimap_with_string(&self, string_multimap: &mut Vec<u8>, element: &String) {
        string_multimap.push(element.len() as u8);
        string_multimap.extend_from_slice(element.as_bytes());
    }

    fn get_string_multimap(&self) -> Vec<u8> {
        let mut string_multimap = Vec::new();
        let options = self.get_options();
        string_multimap.push(ARGUMENTS_QUANTITY);

        for (key, value) in options.iter() {
            self.extend_multimap_with_string(&mut string_multimap, key);
            self.extend_multimap_with_string(&mut string_multimap, value);
        }

        string_multimap
    }
}

impl Executable for OptionsExecutable {
    fn execute(&self) -> Frame {
        let new_body = self.get_string_multimap();
        let frame = Frame {
            // TODO: Sacarlo una vez todos los parsers reciban el frame, asi
            // se le pasa al FrameBuilder el frame viejo.
            version: 0x03,
            flags: 0x00,
            stream: 0x01,
            opcode: 0x05,
            length: 0,
            body: vec![],
        };
        // TODO: Sacar el unwrap y manejar el error

        FrameBuilder::build_response_frame(frame, 0x06, new_body).unwrap()
    }
}

#[cfg(test)]
mod tests {
    const EXPECTED_VERSION: u8 = 131;
    const EXPECTED_FLAGS: u8 = 0x00;
    const EXPECTED_STREAM: i16 = 0x01;
    const EXPECTED_OPCODE: u8 = 0x06;
    const EXPECTED_LENGTH: u32 = 35;
    use super::*;

    fn setup() -> OptionsExecutable {
        let executable = OptionsExecutable::new();
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
