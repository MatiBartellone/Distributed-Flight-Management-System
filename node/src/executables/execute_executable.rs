use crate::executables::executable::Executable;
use crate::frame::Frame;

pub struct ExecuteExecutable;

impl Executable for ExecuteExecutable {
    fn execute(&self, _request: Frame) -> Frame {
        todo!()
    }
}

