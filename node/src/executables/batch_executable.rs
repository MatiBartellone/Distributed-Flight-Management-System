use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct BatchExecutable;

impl Executable for BatchExecutable {
    fn execute(&self, _request: Frame) -> Frame {
        todo!()
    }
}

