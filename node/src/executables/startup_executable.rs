use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
use crate::utils::types::frame::Frame;
use crate::utils::parser_constants::AUTHENTICATE;

#[derive(Debug)]
pub struct StartupExecutable {
    _config: HashMap<String, String>,
}

impl StartupExecutable {
    pub fn new(_config: HashMap<String, String>) -> StartupExecutable {
        StartupExecutable { _config }
    }
}

impl Executable for StartupExecutable {
    fn execute(&mut self, request: Frame) -> Result<Frame, Errors> {
        // Aplicar configs
        // let auth_class = vec![]; // HAY QUE ELEGIR AUTH CLASS Y PASARLA EN BYTES
        let body: Vec<u8> = vec![
            0x00, 0x15, b'P', b'a', b's', b's', b'w', b'o', b'r', b'd', b'A', b'u', b't', b'h',
            b'e', b'n', b't', b'i', b'c', b'a', b't', b'o', b'r',
        ];
        FrameBuilder::build_response_frame(request, AUTHENTICATE, body)
    }
}
