use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct StartupExecutable {
    _config: HashMap<String, String>
}

impl StartupExecutable {
    pub fn new(_config: HashMap<String, String>) -> StartupExecutable{
        StartupExecutable{_config}
    }
}


impl Executable for StartupExecutable {
    fn execute(&self) -> Frame {
        todo!()
    }
}