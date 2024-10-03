use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct OptionsExecutable {
    options: HashMap<String, String>,
}

impl OptionsExecutable {
    pub fn new(options: HashMap<String, String>) -> OptionsExecutable {
        OptionsExecutable { options }
    }

    fn get_string_multimap(&self) -> Vec<u8> {
        let mut string_multimap = Vec::new();
        for (key, value) in self.options.iter() {
            string_multimap.extend_from_slice(key.as_bytes());
            string_multimap.extend_from_slice(value.as_bytes());
        }

        string_multimap
    }
}

impl Executable for OptionsExecutable {
    fn execute(&self) -> Frame {
        let version = 0x03; // debe ser 0x83?
        let flags = 0x00;
        let stream = 0x01;
        let opcode = 0x06;
        let length = 0x00;
        let body = self.get_string_multimap();

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
