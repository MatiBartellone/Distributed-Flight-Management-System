use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::config_constants::{COMPRESSION, CQL_VERSION};
use crate::utils::errors::Errors;
use crate::utils::parser_constants::SUPPORTED;
use crate::utils::types::frame::Frame;

const ARGUMENTS_QUANTITY: u8 = 0x02;
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
        string_multimap.extend_from_slice((element.len() as u16).to_be_bytes().as_ref());
        string_multimap.extend_from_slice(element.as_bytes());
    }

    fn get_string_multimap(&self) -> Vec<u8> {
        let mut string_multimap = Vec::new();
        let options = self.get_options();
        string_multimap.extend_from_slice((ARGUMENTS_QUANTITY as u16).to_be_bytes().as_ref());

        for (key, value) in options.iter() {
            self.extend_multimap_with_string(&mut string_multimap, key);
            self.extend_multimap_with_string(&mut string_multimap, value);
        }

        string_multimap
    }
}

impl Executable for OptionsExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        let new_body = self.get_string_multimap();
        let response = FrameBuilder::build_response_frame(request, SUPPORTED, new_body)?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    const EXPECTED_VERSION: u8 = 131;
    const EXPECTED_FLAGS: u8 = 0x00;
    const EXPECTED_STREAM: i16 = 0x01;
    const EXPECTED_OPCODE: u8 = 0x06;
    const EXPECTED_LENGTH: u32 = 40;
    use super::*;

    fn setup() -> (OptionsExecutable, Frame) {
        let executable = OptionsExecutable::new();
        let request = Frame {
            version: EXPECTED_VERSION,
            flags: EXPECTED_FLAGS,
            stream: EXPECTED_STREAM,
            opcode: 0x05,
            length: 0,
            body: Vec::new(),
        };
        (executable, request)
    }

    #[test]
    fn test_01_options_executable() {
        let (mut executable, request) = setup();
        let frame = executable.execute(request).unwrap();
        assert_eq!(frame.version, EXPECTED_VERSION);
        assert_eq!(frame.flags, EXPECTED_FLAGS);
        assert_eq!(frame.stream, EXPECTED_STREAM);
        assert_eq!(frame.opcode, EXPECTED_OPCODE);
        assert_eq!(frame.length, EXPECTED_LENGTH);
    }
}
