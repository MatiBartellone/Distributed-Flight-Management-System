use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::response_builders::frame_builder::FrameBuilder;
use crate::utils::errors::Errors;
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
    /// Configurate the startup configurations and create a AUTHENTICATE response
    fn execute(&self, request: Frame) -> Result<Frame, Errors> {
        // Aplicar configs
        // let auth_class = vec![]; // HAY QUE ELEGIR AUTH CLASS Y PASARLA EN BYTES
        let body: Vec<u8> = Vec::new();
        FrameBuilder::build_response_frame(request, AUTHENTICATE, body)
    }
}
