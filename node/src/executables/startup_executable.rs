use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;

#[derive(Debug)]
pub struct StartupExecutable {
    _config: HashMap<String, String>
}

impl StartupExecutable {
    pub fn new(_config: HashMap<String, String>) -> StartupExecutable{
        StartupExecutable{_config}
    }
}

impl Executable for StartupExecutable {
    /// Configurate the startup configurations and create a AUTHENTICATE response
    fn execute(&self) -> Frame {
        todo!()
        // Aplicar configs
        // let auth_class = vec![]; // HAY QUE ELEGIR AUTH CLASS Y PASARLA EN BYTES
        // FrameBuilder::build_response_frame(request_frame, AUTHENTICATE, auth_class)
    }
}

