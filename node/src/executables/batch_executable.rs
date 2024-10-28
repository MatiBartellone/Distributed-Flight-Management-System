use crate::executables::executable::Executable;
use crate::utils::errors::Errors;
use crate::utils::frame::Frame;

pub struct BatchExecutable;

impl Executable for BatchExecutable {
    fn execute(&mut self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
