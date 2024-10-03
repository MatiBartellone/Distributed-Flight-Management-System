use std::collections::HashMap;

use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct OptionsExecutable {
    _options: HashMap<String, String>,
}

impl OptionsExecutable {
    pub fn new(_options: HashMap<String, String>) -> OptionsExecutable {
        OptionsExecutable { _options }
    }
}

impl Executable for OptionsExecutable {
    fn execute(&self) -> Frame {
        todo!()
    }
}
