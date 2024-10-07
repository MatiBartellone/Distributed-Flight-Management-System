use crate::executables::executable::Executable;
use crate::frame::Frame;
use crate::utils::errors::Errors;

pub struct BatchExecutable;

impl Executable for BatchExecutable {
    fn execute(&self, _request: Frame) -> Result<Frame, Errors> {
        todo!()
    }
}
